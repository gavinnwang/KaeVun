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
    }

    // set up meta* reference
    db->Init();
    assert(db->even_meta_);
    // check the file to detect corruption
    LOG_INFO("Checking file to detect corruption.");
    auto err_opt = db->Validate();
    if (err_opt.has_value()) {
      LOG_ERROR("Validation failed {}", err_opt->message());
      db->Close();
      return std::unexpected{*err_opt};
    }
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
    Tx tx{disk_handler_, true, GetCurrentMeta()};
    txs.push_back(&tx);
    rwtx_ = &tx;

    // release pending freelist pages
    return tx;
  }

  std::expected<Tx, Error> BeginRTx() noexcept {
    std::lock_guard metalock(metalock_);
    if (!opened_)
      return std::unexpected{Error{"DB not opened"}};
    Tx tx{disk_handler_, false, GetCurrentMeta()};
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
    if (err_opt.has_value()) {
      LOG_INFO("User function caused error, rolling back transaction.");
      tx.Rollback();
      return err_opt.value();
    } else {
      LOG_INFO("User function caused no error.");
    }

    return tx.Commit();
  }

  /// Debug utility to print all pages of a bucket by page id traversal.
  ///
  /// This starts from the bucket’s root page id and traverses recursively,
  /// printing each page (branch or leaf) using its `ToString` or
  /// `ToStringVerbose` method.
  void DebugPrintBucketPages(const std::string &bucket_name) noexcept {
    auto tx_or_err = Begin(false);
    if (!tx_or_err) {
      LOG_ERROR("Failed to start read transaction");
      return;
    }

    auto &tx = tx_or_err.value();
    auto bucket_opt = tx.GetBucket(bucket_name);
    if (!bucket_opt.has_value()) {
      LOG_ERROR("Bucket '{}' not found", bucket_name);
      return;
    }

    auto &bucket = bucket_opt.value();
    Pgid root_pgid = bucket.GetMetaTest().Root();
    TraverseAndPrintPage(root_pgid, 0);
  }

private:
  // Initialize the internal fields of the db
  std::optional<Error> Init() noexcept {
    LOG_DEBUG("Initializing database");
    even_meta_ =
        disk_handler_.GetPageFromMmap(EVEN_META_PAGE_ID).GetDataAs<Meta>();
    odd_meta_ =
        disk_handler_.GetPageFromMmap(ODD_META_PAGE_ID).GetDataAs<Meta>();
    LOG_DEBUG("yo {}", even_meta_->ToString());
    LOG_DEBUG("yo {}", odd_meta_->ToString());
    return {};
  }

  std::optional<Error> InitNewDatabaseFile() noexcept {
    LOG_INFO("InitNewDatabaseFile");
    PageBuffer buf{4, disk_handler_.PageSize()};

    auto &meta_p = buf.GetPage(EVEN_META_PAGE_ID);
    meta_p.SetId(EVEN_META_PAGE_ID);
    meta_p.SetFlags(PageFlag::MetaPage);

    auto &m_even = *meta_p.GetDataAs<Meta>();
    m_even.SetMagic(MAGIC);
    m_even.SetVersion(VERSION_NUMBER);
    m_even.SetPageSize(disk_handler_.PageSize());
    m_even.SetFreelist(FREELIST_PAGE_ID);
    m_even.SetBuckets(BUCKET_PAGE_ID);
    m_even.SetWatermark(3);
    m_even.SetTxid(0);
    m_even.SetChecksum(m_even.Sum64());

    auto &o_meta_p = buf.GetPage(ODD_META_PAGE_ID);
    o_meta_p.SetId(ODD_META_PAGE_ID);
    o_meta_p.SetFlags(PageFlag::MetaPage);

    auto &m_odd = *o_meta_p.GetDataAs<Meta>();
    m_odd.SetMagic(MAGIC);
    m_odd.SetVersion(VERSION_NUMBER);
    m_odd.SetPageSize(disk_handler_.PageSize());
    m_odd.SetFreelist(FREELIST_PAGE_ID);
    m_odd.SetBuckets(BUCKET_PAGE_ID);
    m_odd.SetWatermark(3);
    m_odd.SetTxid(1);
    m_odd.SetChecksum(m_odd.Sum64());

    auto &freelist_p = buf.GetPage(FREELIST_PAGE_ID);
    freelist_p.SetId(FREELIST_PAGE_ID);
    freelist_p.SetFlags(PageFlag::FreelistPage);

    auto &bucket_p = buf.GetPage(BUCKET_PAGE_ID);
    bucket_p.SetId(BUCKET_PAGE_ID);
    bucket_p.SetFlags(PageFlag::BucketPage);

    auto e = disk_handler_.WritePageBuffer(buf, 0);
    if (e.has_value()) {
      return e;
    }
    return disk_handler_.Sync();
  }

  [[nodiscard]] std::optional<Error> Validate() noexcept {
    // Validate the meta
    if (even_meta_->Validate() && odd_meta_->Validate()) {
      return Error{"both meta invalid"};
    }
    return std::nullopt;
  }

  /// Recursively print all pages starting from the given page id.
  void TraverseAndPrintPage(Pgid pgid, int depth) noexcept {
    if (pgid == 0) {
      LOG_INFO("Reached null page id");
      return;
    }

    auto &page = disk_handler_.GetPageFromMmap(pgid);

    std::string indent(depth * 2, ' ');
    if (page.Flags() & static_cast<std::size_t>(PageFlag::LeafPage)) {
      LeafPage &leaf = page.AsPage<LeafPage>();
      LOG_WARN("{}LeafPage {}: {}", indent, pgid, leaf.ToString());
    } else if (page.Flags() & static_cast<std::size_t>(PageFlag::BranchPage)) {
      BranchPage &branch = page.AsPage<BranchPage>();
      LOG_WARN("{}BranchPage {}: {}", indent, pgid, branch.ToString());
      for (std::size_t i = 0; i < branch.Count(); ++i) {
        TraverseAndPrintPage(branch.GetPgid(i), depth + 1);
      }
    } else {
      LOG_INFO("{}Unknown page type for pgid {}", indent, pgid);
    }
  }

  [[nodiscard]] Meta GetCurrentMeta() noexcept {
    auto m0 = *even_meta_;
    auto m1 = *odd_meta_;
    LOG_DEBUG("m1 {}, m0 {}", m1.ToString(), m0.ToString());
    if (m1.GetTxid() < m0.GetTxid()) {
      std::swap(m0, m1);
    }

    // Use higher meta page if valid. Otherwise, fallback to previous, if valid.
    if (!m1.Validate().has_value()) {
      return m1;
    } else {
      LOG_ERROR("m1 meta not valid");
      return m0;
    }
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
  Meta *even_meta_;
  Meta *odd_meta_;
};
} // namespace kv
