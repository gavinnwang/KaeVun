#pragma once

#include "bucket.h"
#include "error.h"
#include "node.h"
#include "page.h"
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
  Tx &operator=(const Tx &) = default;
  Tx &operator=(Tx &&) = default;
  explicit Tx(DB *db, bool writable) noexcept;

  void Rollback() noexcept { LOG_INFO("Rolling back tx"); };

  [[nodiscard]] std::optional<Error> Commit() noexcept { return std::nullopt; }

  [[nodiscard]] std::optional<Bucket>
  GetBucket(const std::string name) noexcept {}

  [[nodiscard]] Page &GetPage(Pgid id) noexcept;

  // [[nodiscard]] std::expected<Bucket, Error>
  // CreateBucket(const std::string &name) noexcept {
  // return root_.CreateBucket(name);
  // }

private:
  DB *db_;
  bool writable_{false};
  std::vector<Node *> pending_;
  std::unordered_map<Pgid, Page &> page_{};
  std::unordered_map<Pgid, Node &> node_{};
  // Bucket root_;
};
} // namespace kv
