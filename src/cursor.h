#pragma once

#include "bucket_meta.h"
#include "log.h"
#include "node.h"
#include "page.h"
#include "tx_cache.h"
#include <cstdint>
#include <utility>
#include <vector>
namespace kv {
class Cursor {
public:
  Cursor(TxCache &tx_cache, const BucketMeta &b_meta) noexcept
      : tx_cache_(tx_cache), b_meta_(b_meta) {};

  [[nodiscard]] std::pair<Slice, Slice> Seek(const Slice &seek) noexcept {
    stack_.clear();
    Search(seek, b_meta_.Root());
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
    LOG_INFO("searching {}", pgid);
    auto [p, n] = tx_cache_.GetPageOrNode(pgid);
    auto node = TreeNode{p, n};
    stack_.push_back(node);
    if (node.IsLeaf()) {
      LOG_INFO("is leaf pid: {}", node.p_->Id());
      if (node.n_) {
        index_ = node.n_->FindLastLessThan(key) + 1;
        node.index_ = index_;
      } else {
        auto &p = node.p_->AsPage<LeafPage>();
        index_ = p.FindLastLessThan(key) + 1;
        node.index_ = index_;
      }
    } else {
      LOG_INFO("is branch pid: {}", node.p_->Id());
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

    TreeNode(Page *p, Node *n) noexcept : p_{p}, n_{n} {}

    [[nodiscard]] bool IsLeaf() const noexcept {
      if (n_) {
        return n_->IsLeaf();
      }
      LOG_INFO("p {}", static_cast<const void *>(p_));
      return (p_->Flags() & static_cast<uint32_t>(PageFlag::LeafPage)) != 0;
    }
  };

  TxCache &tx_cache_;
  const BucketMeta &b_meta_;
  uint32_t index_;
  std::vector<TreeNode> stack_;
};
} // namespace kv
