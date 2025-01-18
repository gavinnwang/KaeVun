#pragma once

#include "error.h"
#include "page.h"
#include "type.h"
#include <cstdint>
#include <expected>
#include <unordered_map>
namespace kv {
class Tx;

class Bucket {
public:
  explicit Bucket() noexcept = default;
  Bucket(const Tx *tx, const std::string name) noexcept;

  std::expected<Bucket, Error> GetBucket(const std::string &name) noexcept {
    return {};
  }
  std::expected<Bucket, Error> CreateBucket(const std::string &name) noexcept {
    return {};
  }

private:
  Tx *tx_;
  uint64_t auto_id_;
  Pgid root_;
};

// In memory representation of the buckets meta page
class Buckets {
public:
  explicit Buckets(Page &p) noexcept : p_(p) {
    // read from id
    Read();
  };

  [[nodiscard]] uint16_t Size() const noexcept { return buckets_.size(); }

  [[nodiscard]] std::optional<std::reference_wrapper<Bucket>>
  GetBucket(const std::string &name) noexcept {
    if (buckets_.find(name) == buckets_.end()) {
      return std::nullopt;
    }
    return std::ref(buckets_.at(name));
  }

private:
  void Read() noexcept {}

  void Write() noexcept { p_.SetCount(buckets_.size()); }

  Page &p_;
  std::unordered_map<std::string, Bucket> buckets_{};
};
} // namespace kv
