#pragma once

#include "error.h"
#include "page.h"
#include <optional>
#include <unordered_map>
namespace kv {

class DB;
class Tx {

public:
  explicit Tx(DB *db, bool writable) noexcept;
  void Rollback() noexcept { LOG_INFO("Rolling back tx"); };
  std::optional<Error> Commit() noexcept { return std::nullopt; }

private:
  DB *db_;
  std::unordered_map<Pgid, Page *> page_cache_{};
  bool writable_{false};
};
} // namespace kv
