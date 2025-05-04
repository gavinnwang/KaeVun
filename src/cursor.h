#pragma once

#include "node.h"
#include "page.h"
#include "tx_cache.h"
#include <utility>
#include <vector>
namespace kv {
class Cursor {
public:
  Cursor(TxBPlusTreeHandler &tx_cache) noexcept : tx_cache_(tx_cache) {};

  [[nodiscard]] std::pair<Slice, Slice> Seek(const Slice &seek) noexcept {
    // assert(tx_.open_);
    stack_.clear();
    return {};
  }

private:
  struct TreeNode {
    Page *p_;
    Node *n_;
  };
  TxBPlusTreeHandler &tx_cache_;
  Pgid root_;
  std::vector<TreeNode> stack_;
};
} // namespace kv
