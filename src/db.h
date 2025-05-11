#pragma once

#include "disk.h"
#include "error.h"
#include "log.h"
#include "page.h"
#include "scope.h"
#include "tx.h"
#include <cassert>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>

namespace kv {

class DB {

public:
  DB() noexcept = default;

  using RAII_DB = std::unique_ptr<DB, std::function<void(DB *)>>;

  [[nodiscard]] static std::expected<RAII_DB, Error>
  Open(const std::filesystem::path &path) noexcept {
    auto db = std::unique_ptr<DB, std::function<void(DB *)>>(
        new DB{}, [](DB *db_ptr) {
          if (db_ptr) {
            db_ptr->Close();
            delete db_ptr;
          }
        });

    auto file_sz_or_err = db->disk_handler_.Open(path);
    if (!file_sz_or_err)
      return std::unexpected{file_sz_or_err.error()};
    auto file_sz = file_sz_or_err.value();

    if (file_sz == 0) {
      // if file size is 0, init, set up meta
      auto err_opt = db->InitNewDatabaseFile();
      if (err_opt.has_value()) {
        LOG_ERROR("Init failed {}", err_opt->message());
        db->Close();
        return std::unexpected{*err_opt};
      }
    } else {
      // check the file to detect corruption
      LOG_INFO("Checking file to detect corruption.");
      auto err_opt = db->Validate();
      if (err_opt.has_value()) {
        LOG_ERROR("Validation failed {}", err_opt->message());
        db->Close();
        return std::unexpected{*err_opt};
      }
    }

    // set up meta* reference
    db->Init();
    assert(db->meta_);
    // auto err_opt = db->meta_->Validate();
    // if (err_opt.has_value()) {
    //   LOG_ERROR("Validation failed {}", err_opt->message());
    //   db->Close();
    //   return std::unexpected{*err_opt};
    // }

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
    disk_handler_.Close();
    opened_ = false;
  }

  std::expected<Tx, Error> Begin(bool writable) noexcept {
    if (writable) {
      LOG_INFO("Begin new read write tx");
      return BeginRWTx();
    }
    LOG_INFO("Begin new read tx");
    return BeginRTx();
  }

  std::expected<Tx, Error> BeginRWTx() noexcept {
    std::lock_guard writerlock(writerlock_);
    std::lock_guard metalock(metalock_);
    if (!opened_)
      return std::unexpected{Error{"DB not opened"}};
    // Tx takes in a copy of the db meta
    LOG_DEBUG("---Creating transaction---");
    Tx tx{disk_handler_, true, *meta_};
    txs.push_back(&tx);
    rwtx_ = &tx;

    // release pending freelist pages
    return tx;
  }

  std::expected<Tx, Error> BeginRTx() noexcept {
    std::lock_guard metalock(metalock_);
    if (!opened_)
      return std::unexpected{Error{"DB not opened"}};
    Tx tx{disk_handler_, false, *meta_};
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

  [[nodiscard]] std::optional<Error>
  Update(const std::function<std::optional<Error>(Tx &)> &fn) noexcept {
    auto tx_or_err = Begin(true);
    if (!tx_or_err)
      return tx_or_err.error();
    auto &tx = tx_or_err.value();

    // auto rollback_guard = Defer([&]() noexcept {
    //   // make sure tx is rollback when it panics
    //   LOG_INFO("Rollback guard rolling back transaction.");
    //   tx.Rollback();
    // });

    auto err_opt = fn(tx);
    if (err_opt) {
      LOG_INFO("User function caused error, rolling back transaction.");
      tx.Rollback();
    } else {
      LOG_INFO("User function caused no error.");
    }

    return tx.Commit();
  }

private:
  // Initialize the internal fields of the db
  std::optional<Error> Init() noexcept {
    LOG_DEBUG("Initializing database");
    meta_ = disk_handler_.GetPageFromMmap(0).GetDataAs<Meta>();
    LOG_DEBUG("{}", meta_->ToString());
    return {};
  }
  std::optional<Error> InitNewDatabaseFile() noexcept {
    PageBuffer buf{3, disk_handler_.PageSize()};

    auto &meta_p = buf.GetPage(META_PAGE_ID);
    meta_p.SetId(META_PAGE_ID);
    meta_p.SetFlags(PageFlag::MetaPage);

    auto &m = *meta_p.GetDataAs<Meta>();
    m.SetMagic(MAGIC);
    m.SetVersion(VERSION_NUMBER);
    m.SetPageSize(disk_handler_.PageSize());
    m.SetFreelist(FREELIST_PAGE_ID);
    m.SetBuckets(BUCKET_PAGE_ID);
    m.SetWatermark(3); // highest page id in use
    m.SetTxid(0);
    m.SetChecksum(m.Sum64());

    auto &freelist_p = buf.GetPage(FREELIST_PAGE_ID);
    freelist_p.SetId(FREELIST_PAGE_ID);
    freelist_p.SetFlags(PageFlag::FreelistPage);

    auto &bucket_p = buf.GetPage(BUCKET_PAGE_ID);
    bucket_p.SetId(BUCKET_PAGE_ID);
    bucket_p.SetFlags(PageFlag::BucketPage);

    // auto &leaf_p = buf.GetPage(3);
    // leaf_p.SetId(3);
    // leaf_p.SetFlags(PageFlag::LeafPage);

    auto e = disk_handler_.WritePageBuffer(buf, 0);
    if (e.has_value()) {
      return e;
    }
    return disk_handler_.Sync();
  }

  [[nodiscard]] std::optional<Error> Validate() noexcept {
    // Validate the meta
    auto buf_or_err = disk_handler_.CreatePageBufferFromDisk(0, 1);
    if (!buf_or_err) {
      return buf_or_err.error();
    }
    auto &p = buf_or_err->GetPage(0);

    return p.GetDataAs<Meta>()->Validate();
  }

private:
  struct Stats {
    // total number of started read tx
    std::size_t tx_cnt_;
    // number of currently open read transactions
    std::size_t open_tx_cnt_;
  };
  // mutex to protect the meta pages
  std::mutex metalock_;
  // only allow one writer to the database at a time
  std::mutex writerlock_;
  // whether the db is opened or not. Close() will only work if opened_ is true
  bool opened_{false};
  // disk handler
  DiskHandler disk_handler_;
  // tracks all the txs
  std::vector<Tx *> txs;
  // tracking stats
  Stats stats_;
  // mutex to protect stats
  std::mutex statslock_;
  // write tx
  Tx *rwtx_;
  // Meta
  Meta *meta_;
};
} // namespace kv
