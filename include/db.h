#pragma once

#include "error.h"
#include "log.h"
#include "os.h"
#include "slice.h"
#include "tx.h"
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <sys/file.h>
#include <unistd.h>

namespace kv {

class DB {
public:
  static std::expected<std::unique_ptr<DB>, Error>
  Open(const std::filesystem::path &path) noexcept {
    auto db = std::make_unique<DB>();

    int flags = (O_RDWR | O_CREAT);
    mode_t mode = 0666;
    LOG_TRACE("Opening db file: {}", path.string());
    // acquire a file descriptor
    db->fd_ = ::open(path.c_str(), flags, mode);
    if (db->fd_ == -1) {
      LOG_ERROR("Failed to open db file");
      return std::unexpected{Error{"IO error"}};
    }
    db->path_ = path;
    // acquire file descriptor lock
    if (::flock(db->fd_, LOCK_EX) == -1) {
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

    std::error_code ec;
    auto file_sz = std::filesystem::file_size(db->path_, ec);
    LOG_INFO("Current DB file size {}", file_sz);
    if (ec) {
      return std::unexpected{Error{"Failed to check for file size"}};
    }

    if (file_sz == 0) {
      // if file size is 0, init, set up meta
      db->Init();
    } else {
      if (!db->Validate()) {
        LOG_ERROR("Validation failed");
        db->Close();
        return std::unexpected{Error{"IO error"}};
      }
      // check the file to detect corruption
    }

    // set up page pool
    // mmap the opened .db file into a data region
    // set up freelist
    // recover

    db->opened_ = true;
    return db;
  }

  std::expected<Tx, Error> Begin(bool writable) noexcept {
    if (writable) {
      return BeginRWTx();
    }
    return BeginRTx();
  }

  std::optional<Error> Put(const Slice &key, const Slice &value) noexcept;
  std::optional<Error> Delete(const Slice &key) noexcept;
  std::optional<Error> Get(const Slice &key, std::string *output) noexcept;

  explicit DB() noexcept {}

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
  bool Validate() noexcept {
    std::vector<std::byte> buf(page_size_);
    fs_.seekg(0, std::ios::beg);
    fs_.read(reinterpret_cast<char *>(buf.data()), buf.size());
    if (!fs_) {
      LOG_ERROR("Failed to read the meta page");
      return false;
    }
    Page *p = GetPageFromBuffer(buf.data(), 0);
    return p->Meta()->Validate();
  }

  void Init() noexcept {
    // init the meta page
    // first page is meta, second page is freelist
    // third page is leaf
    page_size_ = GetOSDefaultPageSize();
    std::vector<std::byte> buf(page_size_ * 3);

    {
      Page *p = GetPageFromBuffer(buf.data(), 0);
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
      Page *p = GetPageFromBuffer(buf.data(), 1);
      p->SetId(1);
      p->SetFlags(PageFlag::FreelistPage);
    }
    {
      Page *p = GetPageFromBuffer(buf.data(), 2);
      p->SetId(2);
      p->SetFlags(PageFlag::LeafPage);
    }

    fs_.write(reinterpret_cast<const char *>(buf.data()), buf.size());
  }

  // Close the DB and release all resources
  void Close() noexcept {
    LOG_INFO("Closing db, releasing resources");
    if (fs_.is_open()) {
      fs_.close();
    }
    if (fd_ != -1) {
      // this will also release the flock
      ::close(fd_);
      fd_ = -1;
    }
    opened_ = false;
  }

  // cast a buffer as a page
  [[nodiscard]] Page *GetPageFromBuffer(std::byte *buffer,
                                        Pgid pgid) const noexcept {
    return reinterpret_cast<Page *>(buffer + pgid * page_size_);
  }

private:
  // mutex to protect the meta pages
  std::mutex metalock_;
  // only allow one writer to the database at a time
  std::mutex writerlock_;

  bool opened_{false};
  std::filesystem::path path_{""};
  std::fstream fs_;
  int fd_{-1};

  uint32_t page_size_{DEFAULT_PAGE_SIZE};
};
} // namespace kv
