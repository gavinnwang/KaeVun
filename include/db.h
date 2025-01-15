#pragma once

#include "error.h"
#include "fd.h"
#include "log.h"
#include "mmap.h"
#include "os.h"
#include "page.h"
#include "slice.h"
#include "tx.h"
#include <cassert>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>

namespace kv {

class DB {
  static constexpr uint64_t INIT_MMAP_SIZE = 1 << 30;

public:
  explicit DB() noexcept {}

  static std::expected<std::unique_ptr<DB, std::function<void(DB *)>>, Error>
  Open(const std::filesystem::path &path) noexcept {
    auto db = std::unique_ptr<DB, std::function<void(DB *)>>(
        new DB{}, [](DB *db_ptr) {
          if (db_ptr) {
            db_ptr->Close();
            delete db_ptr;
          }
        });

    int flags = (O_RDWR | O_CREAT);
    mode_t mode = 0666;
    LOG_TRACE("Opening db file: {}", path.string());
    // acquire a file descriptor
    auto fd = ::open(path.c_str(), flags, mode);
    db->fd_ = Fd{fd};
    if (db->fd_.GetFd() == -1) {
      LOG_ERROR("Failed to open db file");
      return std::unexpected{Error{"IO error"}};
    }
    db->path_ = path;
    // acquire file descriptor lock
    if (::flock(db->fd_.GetFd(), LOCK_EX) == -1) {
      LOG_ERROR("Failed to lock db file");
      db->Close();
      return std::unexpected{Error{"Failed to lock db file"}};
    }

    db->fs_.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!db->fs_.is_open()) {
      LOG_ERROR("Failed to open db file after creation: {}", path.string());
      db->Close();
      return std::unexpected{Error{"IO error"}};
    }
    auto file_sz_or_err = OS::GetFileSize(db->path_);
    if (!file_sz_or_err) {
      return std::unexpected{file_sz_or_err.error()};
    }
    auto file_sz = file_sz_or_err.value();
    if (file_sz == 0) {
      // if file size is 0, init, set up meta
      db->Init();
    } else {
      // check the file to detect corruption
      if (!db->Validate()) {
        LOG_ERROR("Validation failed");
        db->Close();
        return std::unexpected{Error{"IO error"}};
      }
    }

    // set up mmap
    db->Mmap(INIT_MMAP_SIZE);

    // set up page pool
    // set up freelist
    // recover

    db->opened_ = true;
    return db;
  }

  std::optional<Error> Put(const Slice &key, const Slice &value) noexcept;
  std::optional<Error> Delete(const Slice &key) noexcept;
  std::optional<Error> Get(const Slice &key, std::string *output) noexcept;

  // Close the DB and release all resources
  void Close() noexcept {
    LOG_INFO("Closing db, releasing resources");
    if (!opened_) {
      LOG_INFO("DB is not opened or is already closed, no need to close");
      return;
    }
    // release the mmap region to trigger the deconstructor that will unmap the
    // region
    mmap_handle_.Reset();
    if (fs_.is_open()) {
      fs_.close();
    }
    fd_.Reset();
    opened_ = false;
  }

  std::expected<Tx, Error> Begin(bool writable) noexcept {
    if (writable) {
      return BeginRWTx();
    }
    return BeginRTx();
  }

  std::expected<Tx, Error> BeginRWTx() noexcept {
    std::lock_guard writerlock(writerlock_);
    std::lock_guard metalock(metalock_);
    Tx tx(*this);
    if (!opened_)
      return std::unexpected{Error{"DB not opened"}};
    return {tx};
  }

  std::expected<Tx, Error> BeginRTx() noexcept {
    std::lock_guard metalock(metalock_);
    Tx tx(*this);
    if (!opened_)
      return std::unexpected{Error{"DB not opened"}};
    return {tx};
  }

private:
  void Init() noexcept {
    // init the meta page
    // first page is meta, second page is freelist
    // third page is leaf
    page_size_ = OS::GetOSDefaultPageSize();
    std::vector<std::byte> buf(page_size_ * 3);

    {
      auto *p = GetPageFromBuffer<class Page>(buf.data(), 0);
      p->SetId(0);
      p->SetFlags(PageFlag::MetaPage);
      Meta *m = p->Meta();
      m->SetMagic(MAGIC);
      m->SetVersion(VERSION_NUMBER);
      m->SetPageSize(page_size_);
      m->SetFreelist(1);
      // m->setRootBucket(common::NewInBucket(3, 0));
      // first leaf page is on page 2
      m->SetPgid(2);
      m->SetTxid(0);
      m->SetChecksum(m->Sum64());
    }
    {
      auto *p = GetPageFromBuffer<class Page>(buf.data(), 1);
      p->SetId(1);
      p->SetFlags(PageFlag::FreelistPage);
    }
    {
      auto *p = GetPageFromBuffer<class Page>(buf.data(), 2);
      p->SetId(2);
      p->SetFlags(PageFlag::LeafPage);
    }

    fs_.write(reinterpret_cast<const char *>(buf.data()), buf.size());
  }

  bool Validate() noexcept {
    std::vector<std::byte> buf(page_size_);
    fs_.seekg(0, std::ios::beg);
    fs_.read(reinterpret_cast<char *>(buf.data()), buf.size());
    if (!fs_) {
      LOG_ERROR("Failed to read the meta page");
      return false;
    }
    auto *p = GetPageFromBuffer<class Page>(buf.data(), 0);
    return p->Meta()->Validate();
  }

  std::optional<Error> Mmap(uint64_t min_sz) noexcept {
    auto file_sz_or_err = OS::GetFileSize(path_);
    if (!file_sz_or_err) {
      return file_sz_or_err.error();
    }
    auto file_sz = file_sz_or_err.value();
    auto mmap_sz = MmapSize(fmax(min_sz, file_sz));
    LOG_INFO("Mmaping size {}", mmap_sz);

    void *b = mmap(nullptr, mmap_sz, PROT_READ | PROT_WRITE, MAP_SHARED,
                   fd_.GetFd(), 0);
    if (b == MAP_FAILED) {
      return Error("Mmap failed");
    }

    auto mmap_ptr_handle = std::unique_ptr<void, std::function<void(void *)>>(
        std::move(b), [mmap_sz](void *ptr) {
          if (ptr && ptr != MAP_FAILED) {
            LOG_INFO("Unmapping mmap region of size {}", mmap_sz);
            munmap(ptr, mmap_sz);
          }
        });

    int result = madvise(mmap_ptr_handle.get(), mmap_sz, MADV_RANDOM);

    if (result == -1) {
      return Error("Mmap advise failed");
    }

    mmap_sz_ = mmap_sz;
    LOG_INFO("Successfully created mmap memory of size {}", mmap_sz_);

    return std::nullopt;
  }

  [[nodiscard]] uint64_t MmapSize(uint64_t request_sz) noexcept {
    uint64_t step = 1 << 30; // 1GB
    if (request_sz <= step) {
      for (uint32_t i = 15; i <= 30; ++i) {
        if (request_sz <= 1 << i) {
          return 1 << i;
        }
      }
      return step;
    } else {
      uint64_t sz = request_sz;
      uint64_t remainder = request_sz % step;
      if (step > 0)
        sz += step - remainder;

      // ensure sz is a multiple of page size
      if (sz % page_size_ != 0) {
        sz = ((sz / page_size_) + 1) * page_size_;
      }
      return sz;
    }
  }
  // cast a buffer as a page
  template <typename T>
  [[nodiscard]] T *GetPageFromBuffer(std::byte *b, Pgid pgid) const noexcept {
    return reinterpret_cast<T *>(b + pgid * page_size_);
  }

  [[nodiscard]] Page *Page(Pgid id) noexcept {
    uint64_t pos = id * page_size_;
    assert(pos + sizeof(class Page) <= mmap_sz_);
    return reinterpret_cast<class Page *>(mmap_handle_.Data()[pos]);
  }

private:
  // mutex to protect the meta pages
  std::mutex metalock_;
  // only allow one writer to the database at a time
  std::mutex writerlock_;
  // mutex to protect mmap access
  std::shared_mutex mmaplock_;
  // whether the db is opened or not. Close() will only work if opened_ is true
  bool opened_{false};
  // path of the database file
  std::filesystem::path path_{""};
  // fstream of the database file
  std::fstream fs_;
  // file descriptor handle
  Fd fd_;
  // mmap handle that will unmap when released
  MmapDataHandle mmap_handle_;
  // size of the mmap memory
  uint64_t mmap_sz_;
  // page size of the db
  uint32_t page_size_{OS::DEFAULT_PAGE_SIZE};
};
} // namespace kv
