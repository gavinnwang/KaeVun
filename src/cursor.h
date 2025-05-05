#pragma once

#include "node.h"
#include "page.h"
#include "tx_cache.h"
#include <cstdint>
#include <utility>
#include <vector>
namespace kv {
class Cursor {
public:
  Cursor(TxCache &tx_cache) noexcept : tx_cache_(tx_cache) {};

  [[nodiscard]] std::pair<Slice, Slice> Seek(const Slice &seek) noexcept {
    stack_.clear();
    Search(seek, root_);
    return GetKeyValue();
  }

private:
  std::pair<Slice, Slice> GetKeyValue() {
    auto node = stack_.back();
    if (node.n_) {
      auto e = node.n_->GetElements()[node.index_];
      return {e.key_, e.val_};
    } else {
      auto p = node.p_->GetDataAs<LeafPage>();
      auto k = p->GetVal(node.index_);
      auto v = p->GetVal(node.index_);
      return {k, v};
    }
  }
  // Search recursively performs a binary search against a given page/node until
  // it finds a given key
  void Search(const Slice &key, Pgid pgid) {
    auto p_or_n = tx_cache_.GetPageOrNode(pgid);
    auto node = TreeNode{p_or_n};
    stack_.push_back(node);
    if (node.IsLeaf()) {
      if (node.n_) {
        index_ = node.n_->FindLastLessThan(key) + 1;
        node.index_ = index_;
      } else {
        auto &p = node.p_->AsPage<LeafPage>();
        index_ = p.FindLastLessThan(key) + 1;
        node.index_ = index_;
      }
    } else {
      const auto [index, exact] =
          node.n_
              ? node.n_->FindFirstGreaterOrEqualTo(key)
              : node.p_->AsPage<BranchPage>().FindFirstGreaterOrEqualTo(key);

      uint32_t adjusted_index = (!exact && index > 0) ? index - 1 : index;
      stack_.back().index_ = adjusted_index;

      Pgid child_pgid =
          node.n_
              ? node.n_->GetElements()[adjusted_index].pgid_
              : node.p_->AsPage<BranchPage>().GetElement(adjusted_index).pgid_;

      Search(key, child_pgid);
    }
  }

  struct TreeNode {
    Page *p_ = nullptr;
    Node *n_ = nullptr;
    uint32_t index_;

    explicit TreeNode(std::pair<Page *, Node *> pair) noexcept
        : p_{pair.first}, n_{pair.second} {}

    [[nodiscard]] bool IsLeaf() const noexcept {
      if (n_)
        return n_->IsLeaf();
      return (p_->Flags() & static_cast<uint32_t>(PageFlag::LeafPage)) != 0;
    }
  };

  TxCache &tx_cache_;
  uint32_t index_;
  Pgid root_;
  std::vector<TreeNode> stack_;
};
} // namespace kv
