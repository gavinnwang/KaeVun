#pragma once

#include "log.h"
#include "slice.h"
#include "status.h"
#include "tx.h"
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <sys/file.h>

namespace kv {

class DB {
public:
  static std::expected<std::unique_ptr<DB>, Status>
  Open(const std::filesystem::path &path) noexcept {
    // open the file at path
    // acquire file descriptor lock
    // if file size is 0, init, set up meta
    // set up page pool
    // mmap the opened .db file into a data region
    // set up freelist
    // recover

    auto db = std::make_unique<DB>(path);
    int flags = (O_RDWR | O_CREAT);
    mode_t mode = 0666;

    LOG_TRACE("Opening db file: {}", path.string());
    db->fd_ = open(path.c_str(), flags, mode);
    if (db->fd_ == -1) {
      LOG_ERROR("Failed to open db file");
      return std::unexpected{Status::Error("IO error")};
    }
    return db;
  }

  std::expected<Tx, Status> Begin(bool writable) noexcept {
    if (writable) {
      return BeginRWTx();
    }
    return BeginRTx();
  }

  Status Put(const Slice &key, const Slice &value) noexcept;
  Status Delete(const Slice &key) noexcept;
  std::expected<std::string *, Status> Get(const Slice &key) noexcept;

  explicit DB(const std::filesystem::path &path) noexcept : path_(path) {}

private:
  std::expected<Tx, Status> BeginRWTx() noexcept {
    std::lock_guard metalock(metalock_);
    Tx tx(*this);
    if (!opened_)
      return std::unexpected{Status::Error("DB not opened")};

    return {tx};
  }

  std::expected<Tx, Status> BeginRTx() noexcept {
    std::lock_guard writerlock(writerlock_);
    std::lock_guard metalock(metalock_);
    Tx tx(*this);
    if (!opened_)
      return std::unexpected{Status::Error("DB not opened")};

    return {tx};
  }

private:
  // mutex to protect the meta pages
  std::mutex metalock_;
  // only allow one writer to the database at a time
  std::mutex writerlock_;
  bool opened_{false};
  std::filesystem::path path_;
  int fd_{-1};
};
} // namespace kv
