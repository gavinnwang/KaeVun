#pragma once

#include "bucket.h"
#include "disk.h"
#include "error.h"
#include "page.h"
#include "tx_cache.h"
#include <expected>
#include <optional>
#include <string>
namespace kv {

class Tx {

public:
  Tx(DiskHandler &disk, bool writable, Meta db_meta) noexcept
      : open_(true), disk_(disk), tx_handler_(disk, writable),
        writable_(writable), meta_(db_meta),
        buckets_(Buckets{disk.GetPageFromMmap(meta_.GetBuckets())}) {
    if (writable_) {
      meta_.IncrementTxid();
    }
  };

  Tx(const Tx &) = delete;
  Tx &operator=(const Tx &) = delete;
  Tx(Tx &&) = default;
  Tx &operator=(Tx &&) noexcept = delete;

  void Rollback() noexcept { LOG_INFO("Rolling back tx"); };

  [[nodiscard]] bool Writable() const noexcept { return writable_; }

  [[nodiscard]] std::optional<Error> Commit() noexcept { return std::nullopt; }

  // GetBucket retrievs the bucket with given name
  [[nodiscard]] std::optional<Bucket>
  GetBucket(const std::string &name) noexcept {
    auto b = buckets_.GetBucket(name);
    if (!b.has_value()) {
      return {};
    }
    return Bucket{tx_handler_, name, b.value().get()};
  }

  [[nodiscard]] std::expected<BucketMeta, Error>
  CreateBucket(const std::string &name) noexcept {
    if (!open_) {
      return std::unexpected{Error{"Tx not open"}};
    }
    if (!writable_) {
      return std::unexpected{Error{"Tx not writable"}};
    }
    if (buckets_.GetBucket(name)) {
      return std::unexpected{Error{"Bucket exists"}};
    }
    if (name.size() == 0) {
      return std::unexpected{Error{"Bucket name required"}};
    }

    auto p_err = tx_handler_.AllocateShadowPage(meta_, 1);
    if (!p_err) {
      return std::unexpected{p_err.error()};
    }
    auto &p = p_err.value().get();
    p.SetFlags(PageFlag::LeafPage);
    auto b = buckets_.AddBucket(name, BucketMeta{p.Id()});
    assert(b);
    return b.value();
  }

private:
  [[nodiscard]] Meta &GetMeta() noexcept { return meta_; }
  // WriteMeta writes the meta to the disk.
  [[nodiscard]] std::optional<Error> WriteMeta() noexcept {
    PageBuffer buf{1, disk_.PageSize()};
    auto &p = buf.GetPage(0);
    meta_.Write(p);
    // Write the meta page to file.
    auto err = disk_.WritePage(p);
    if (err) {
      return err;
    }
    err = disk_.Sync();
    if (err) {
      return err;
    }
    return {};
  }

  bool open_{false};
  DiskHandler &disk_;
  TxCache tx_handler_;
  bool writable_{false};
  Meta meta_;
  Buckets buckets_;
};
} // namespace kv
