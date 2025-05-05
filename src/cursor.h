#pragma once

#include "node.h"
#include "page.h"
#include "tx_cache.h"
#include <utility>
#include <vector>
namespace kv {
class Cursor {
public:
  Cursor(TxCache &tx_cache) noexcept : tx_cache_(tx_cache) {};

  [[nodiscard]] std::pair<Slice, Slice> Seek(const Slice &seek) noexcept {
    // assert(tx_.open_);
    stack_.clear();
    return {};
  }

private:
  // Search recursively performs a binary search against a given page/node until
  // it finds a given key
  void Search(const Slice &key, Pgid pgid) {
    auto p_or_n = tx_cache_.GetPageOrNode(pgid);
    auto node = TreeNode{p_or_n};
    stack_.push_back(node);
    if (node.IsLeaf()) {
      node.
    }
  }

  struct TreeNode {
    Page *p_ = nullptr;
    Node *n_ = nullptr;

    explicit TreeNode(std::pair<Page *, Node *> pair) noexcept
        : p_{pair.first}, n_{pair.second} {}

    [[nodiscard]] bool IsLeaf() const noexcept {
      if (n_)
        return n_->IsLeaf();
      return (p_->Flags() & static_cast<uint32_t>(PageFlag::LeafPage)) != 0;
    }
  };

  TxCache &tx_cache_;
  Pgid root_;
  std::vector<TreeNode> stack_;
};
} // namespace kv
