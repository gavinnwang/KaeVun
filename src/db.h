#pragma once

#include "error.h"
#include "fd.h"
#include "log.h"
#include "mmap.h"
#include "os.h"
#include "page.h"
// #include "slice.h"
#include "scope.h"
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

  friend class Tx;

  static constexpr uint64_t INIT_MMAP_SIZE = 1 << 30;

public:
  explicit DB() noexcept = default;

  static std::expected<std::unique_ptr<DB, std::function<void(DB *)>>, Error>
  Open(const std::filesystem::path &path) noexcept {
    auto db = std::unique_ptr<DB, std::function<void(DB *)>>(
        new DB{}, [](DB *db_ptr) {
          if (db_ptr) {
            db_ptr->Close();
            delete db_ptr;
          }
        });

    constexpr auto flags = (O_RDWR | O_CREAT);
    constexpr auto mode = 0666;
    LOG_TRACE("Opening db file: {}", path.string());

    // acquire a file descriptor
    auto fd = ::open(path.c_str(), flags, mode);
    if (fd == -1) {
      LOG_ERROR("Failed to open db file");
      return std::unexpected{Error{"IO error"}};
    }
    db->fd_ = Fd{fd};
    db->path_ = path;

    // acquire file descriptor lock
    if (::flock(db->fd_.GetFd(), LOCK_EX) == -1) {
      LOG_ERROR("Failed to lock db file");
      db->Close();
      return std::unexpected{Error{"Failed to lock db file"}};
    }

    // open fstream for io
    db->fs_.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!db->fs_.is_open()) {
      LOG_ERROR("Failed to open db file after creation: {}", path.string());
      db->Close();
      return std::unexpected{Error{"IO error"}};
    }
    if (auto file_sz = OS::FileSize(db->path_); file_sz) {
      if (file_sz == 0) {
        // if file size is 0, init, set up meta
        auto err_opt = db->Init();
        if (err_opt.has_value()) {
          LOG_ERROR("Init failed {}", err_opt->message());
          db->Close();
          return std::unexpected{*err_opt};
        }
      } else {
        // check the file to detect corruption
        auto err_opt = db->Validate();
        if (err_opt.has_value()) {
          LOG_ERROR("Validation failed {}", err_opt->message());
          db->Close();
          return std::unexpected{*err_opt};
        }
      }
    } else {
      return std::unexpected{file_sz.error()};
    }
    // set up mmap for io
    if (auto errOpt = db->Mmap(INIT_MMAP_SIZE)) {
      return std::unexpected{*errOpt};
    }

    // set up page pool
    // set up freelist
    // recover

    db->opened_ = true;
    return db;
  }

  // Close the DB and release all resources
  void Close() noexcept {
    LOG_INFO("Closing db, releasing resources");
    if (!opened_) {
      LOG_INFO("DB is not opened or is already closed, no need to close");
      return;
    }
    // release the mmap region to trigger the deconstructor that will unmap the
    // region
    if (fs_.is_open()) {
      fs_.close();
    }
    mmap_handle_.Reset();
    fd_.Reset();
    opened_ = false;
  }

  std::expected<Tx, Error> Begin(bool writable) noexcept {
    LOG_INFO("Begin new tx");
    if (writable) {
      return BeginRTx();
    }
    return BeginRWTx();
  }

  std::expected<Tx, Error> BeginRWTx() noexcept {
    std::lock_guard writerlock(writerlock_);
    std::lock_guard metalock(metalock_);
    if (!opened_)
      return std::unexpected{Error{"DB not opened"}};
    assert(mmap_handle_.Valid());
    Tx tx{this, true};
    txs.push_back(&tx);
    rwtx_ = &tx;

    // release pending freelist pages
    return tx;
  }

  std::expected<Tx, Error> BeginRTx() noexcept {
    std::lock_guard metalock(metalock_);
    if (!opened_)
      return std::unexpected{Error{"DB not opened"}};
    assert(mmap_handle_.Valid());
    Tx tx{this, false};
    txs.push_back(&tx);
    // add read only txid to freelist

    std::lock_guard statslock(statslock_);
    stats_.tx_cnt_++;
    stats_.open_tx_cnt_ = txs.size();
    return tx;
  }

  // std::optional<Error> Put(const Slice &key, const Slice &value) noexcept;
  // std::optional<Error> Delete(const Slice &key) noexcept;
  // std::optional<Error> Get(const Slice &key, std::string *output) noexcept;

  std::optional<Error>
  Update(const std::function<std::optional<Error>(Tx *)> &fn) noexcept {
    auto tx_or_err = Begin(true);
    if (!tx_or_err)
      return tx_or_err.error();
    auto tx = tx_or_err.value();

    auto rollback_guard = Defer([&]() {
      // make sure tx is rollback when it panics
      tx.Rollback();
    });

    auto err_opt = fn(&tx);
    if (err_opt) {
      tx.Rollback();
    }

    return tx.Commit();
  }

private:
  std::optional<Error> Init() noexcept {
    // init the meta pages
    // first page is meta
    // second page is freelist
    // third page is buckets page
    // third page is leaf
    page_size_ = OS::OSPageSize();
    std::vector<std::byte> buf(page_size_ * 4);

    {
      auto &p = GetPageFromBuffer(buf.data(), 0);
      p.SetId(0);
      p.SetFlags(PageFlag::MetaPage);
      auto &m = p.Meta();
      m.SetMagic(MAGIC);
      m.SetVersion(VERSION_NUMBER);
      m.SetPageSize(page_size_);
      m.SetFreelist(1);
      m.SetPgid(2);
      m.SetTxid(0);
      m.SetChecksum(m.Sum64());
    }
    {
      auto &p = GetPageFromBuffer(buf.data(), 1);
      p.SetId(1);
      p.SetFlags(PageFlag::FreelistPage);
    }
    {
      auto &p = GetPageFromBuffer(buf.data(), 2);
      p.SetId(2);
      p.SetFlags(PageFlag::BucketPage);
    }
    {
      auto &p = GetPageFromBuffer(buf.data(), 3);
      p.SetId(3);
      p.SetFlags(PageFlag::LeafPage);
    }

    fs_.write(reinterpret_cast<const char *>(buf.data()), buf.size());
    if (fs_.fail()) {
      return Error{"IO Error"};
    }
    return fd_.Sync();
  }

  [[nodiscard]] std::optional<Error> Validate() noexcept {
    std::vector<std::byte> buf(page_size_);
    fs_.seekg(0, std::ios::beg);
    fs_.read(reinterpret_cast<char *>(buf.data()), buf.size());
    if (fs_.fail()) {
      return Error{"Unable to read meta page"};
    }
    auto &p = GetPageFromBuffer(buf.data(), 0);
    return p.Meta().Validate();
  }

  [[nodiscard]] std::optional<Error> Mmap(uint64_t min_sz) noexcept {
    auto file_sz_or_err = OS::FileSize(path_);
    if (!file_sz_or_err) {
      return file_sz_or_err.error();
    }
    auto file_sz = file_sz_or_err.value();
    auto mmap_sz = MmapSize(fmax(min_sz, file_sz));
    LOG_INFO("Mmaping size {}", mmap_sz);

    void *b = mmap(nullptr, mmap_sz, PROT_READ | PROT_WRITE, MAP_SHARED,
                   fd_.GetFd(), 0);
    if (b == MAP_FAILED) {
      return Error("Failed to mmap");
    }

    mmap_handle_ = MmapDataHandle{b, mmap_sz};

    int result = madvise(mmap_handle_.MmapPtr(), mmap_sz, MADV_RANDOM);

    if (result == -1) {
      return Error("Mmap advise failed");
    }

    LOG_INFO("Successfully created mmap memory of size {}",
             mmap_handle_.Size());

    // validate the mmap
    auto &p = GetPage(0);
    auto err_opt = p.Meta().Validate();
    if (err_opt.has_value()) {
      return *err_opt;
    }

    assert(mmap_handle_.Valid());
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
  [[nodiscard]] Page &GetPageFromBuffer(std::byte *b,
                                        Pgid pgid) const noexcept {
    return *reinterpret_cast<Page *>(b + pgid * page_size_);
  }

  // gets a page from mmap
  [[nodiscard]] Page &GetPage(Pgid id) noexcept {
    uint64_t pos = id * page_size_;
    assert(mmap_handle_.Valid());
    assert(pos + sizeof(Page) <= mmap_handle_.Size());
    LOG_INFO("Accessing mmap memory address: {}",
             static_cast<void *>(
                 static_cast<std::byte *>(mmap_handle_.MmapPtr()) + pos));
    return *reinterpret_cast<Page *>(
        static_cast<std::byte *>(mmap_handle_.MmapPtr()) + pos);
    return *reinterpret_cast<Page *>(
        static_cast<std::byte *>(mmap_handle_.MmapPtr()) + pos);
  }

private:
  struct Stats {
    // total number of started read tx
    uint64_t tx_cnt_;
    // number of currently open read transactions
    uint64_t open_tx_cnt_;
  };
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
  // page size of the db
  uint32_t page_size_{OS::DEFAULT_PAGE_SIZE};
  // tracks all the txs
  std::vector<Tx *> txs;
  // tracking stats
  Stats stats_;
  // mutex to protect stats
  std::mutex statslock_;
  // write tx
  Tx *rwtx_;
};
} // namespace kv
