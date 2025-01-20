#pragma once

#include "bucket.h"
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

// forward declaration
class DB;

class Tx {

public:
  Tx(const Tx &) = default;
  Tx(Tx &&) = default;
  Tx &operator=(const Tx &) = delete;
  Tx &operator=(Tx &&) = delete;
  explicit Tx(DB *db, bool writable) noexcept;

  void Rollback() noexcept { LOG_INFO("Rolling back tx"); };

  [[nodiscard]] std::optional<Error> Commit() noexcept { return std::nullopt; }

  [[nodiscard]] std::optional<BucketMeta>
  GetBucket(const std::string &name) noexcept {
    return buckets_.Bucket(name);
  }

  [[nodiscard]] Page &GetPage(Pgid id) noexcept;

  [[nodiscard]] std::expected<BucketMeta, Error>
  CreateBucket(const std::string &name) noexcept {
    if (db_ == nullptr) {
      return std::unexpected{Error{"Tx closed"}};
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

private:
  [[nodiscard]] std::expected<Page *, Error> Allocate(uint32_t count) {}

  DB *db_;
  bool writable_{false};
  std::vector<Node *> pending_;
  std::unordered_map<Pgid, Page &> page_{};
  std::unordered_map<Pgid, Node &> node_{};
  Buckets buckets_;
  // Bucket root_;
};
} // namespace kv
