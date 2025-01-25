#pragma once

#include "bucket.h"
#include "disk.h"
#include "error.h"
#include "node.h"
#include "page.h"
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
namespace kv {

class Tx {

public:
  explicit Tx(DiskHandler &disk, bool writable) noexcept
      : open_(true), disk_(disk), writable_(writable),
        buckets_(Buckets{disk_.GetPage(3)}) {};

  Tx(const Tx &) = default;
  Tx &operator=(const Tx &) = delete;

  Tx(Tx &&) = default;
  Tx &operator=(Tx &&) = delete;

  void Rollback() noexcept { LOG_INFO("Rolling back tx"); };

  [[nodiscard]] std::optional<Error> Commit() noexcept { return std::nullopt; }

  [[nodiscard]] std::optional<BucketMeta>
  GetBucket(const std::string &name) noexcept {
    return buckets_.Bucket(name);
  }

  [[nodiscard]] Page &GetPage(Pgid id) noexcept;

  [[nodiscard]] std::expected<BucketMeta, Error>
  CreateBucket(const std::string &name) noexcept {
    if (!open_) {
      return std::unexpected{Error{"Tx not open"}};
    }
    if (!writable_) {
      return std::unexpected{Error{"Tx not writable"}};
    }
    if (buckets_.Bucket(name)) {
      return std::unexpected{Error{"Bucket exists"}};
    }
    if (name.size() == 0) {
      return std::unexpected{Error{"Bucket name required"}};
    }

    return std::unexpected{Error{"Tx not writable"}};
  }

  [[nodiscard]] Meta &Meta() noexcept { return meta_; }

private:
  [[nodiscard]] std::expected<Page *, Error> Allocate(uint32_t count) {
    return nullptr;
  }

  bool open_{false};
  DiskHandler &disk_;
  bool writable_{false};
  std::vector<Node *> pending_;
  std::unordered_map<Pgid, Page &> page_{};
  std::unordered_map<Pgid, Node &> node_{};
  Buckets buckets_;
  class Meta meta_;
};
} // namespace kv
