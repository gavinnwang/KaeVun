#pragma once

#include "page.h"
#include <unordered_map>
namespace kv {

class DB;
class Tx {

public:
  explicit Tx(DB &db) noexcept;

private:
  DB &db_;
  std::unordered_map<Pgid, Page *> page_cache_{};
};
} // namespace kv
