#pragma once

#include "bucket.h"
#include "disk.h"
#include "error.h"
#include "node.h"
#include "page.h"
#include "tx_cache.h"
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
namespace kv {

class Tx {

public:
  Tx(DiskHandler &disk, bool writable, Meta &db_meta) noexcept
      : open_(true), disk_(disk), tx_handler_(disk), writable_(writable),
        meta_(db_meta),
        buckets_(Buckets{disk_.GetPageFromMmap(meta_.GetBuckets())}) {
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

    return std::make_optional<Bucket>(tx_handler_, name, b.value().get());
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

    return std::unexpected{Error{"Tx not writable"}};
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

  [[nodiscard]] std::expected<Page *, Error> Allocate(uint32_t count) {
    return nullptr;
  }

  bool open_{false};
  DiskHandler &disk_;
  TxBPlusTreeHandler tx_handler_;
  bool writable_{false};
  Meta &meta_;
  Buckets buckets_;
};
} // namespace kv
