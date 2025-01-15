#pragma once

#include "error.h"
#include "log.h"
#include "os.h"
#include "slice.h"
#include "tx.h"
#include <cstdint>
#include <expected>
#include <filesystem>
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
    // open the file at path
    db->fd_ = open(path.c_str(), flags, mode);
    if (db->fd_ == -1) {
      LOG_ERROR("Failed to open db file");
      return std::unexpected{Error{"IO error"}};
    }
    db->path_ = path;
    // acquire file descriptor lock
    if (flock(db->fd_, LOCK_EX) == -1) {
      LOG_ERROR("Failed to lock db file");
      db->Close();
      return std::unexpected{Error{"Failed to lock db file"}};
    }
    std::error_code ec;
    auto sz = std::filesystem::file_size(db->path_, ec);
    if (ec) {
      return std::unexpected{Error{"Failed to check for file size"}};
    }

    if (sz == 0) {
      // if file size is 0, init, set up meta
      db->Init();
    } else {
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

  Error Put(const Slice &key, const Slice &value) noexcept;
  Error Delete(const Slice &key) noexcept;
  std::expected<std::string *, Error> Get(const Slice &key) noexcept;

  explicit DB() noexcept {}

private:
  void Init() noexcept {
    // init the meta page
    page_size_ = GetOSDefaultPageSize();
  }

  // Close the DB and release all resources
  void Close() noexcept {
    LOG_INFO("Closing db, releasing resources");
    path_ = "";
    close(fd_);
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
  // mutex to protect the meta pages
  std::mutex metalock_;
  // only allow one writer to the database at a time
  std::mutex writerlock_;

  bool opened_{false};
  std::filesystem::path path_{""};
  int fd_{-1};

  uint32_t page_size_{DEFAULT_PAGE_SIZE};
};
} // namespace kv
