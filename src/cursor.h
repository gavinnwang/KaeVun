#pragma once

#include "bucket_meta.h"
#include "log.h"
#include "node.h"
#include "page.h"
#include "tx_cache.h"
#include "type.h"
#include <cstdint>
#include <utility>
#include <vector>
namespace kv {
class Cursor {
public:
  Cursor(ShadowPageHandler &tx_cache, const BucketMeta &b_meta) noexcept
      : tx_cache_(tx_cache), b_meta_(b_meta) {};

  // Places the cursor at the node where we would insert the seek slice
  // After using this method the cursor should always point to a leaf node
  [[nodiscard]] std::optional<std::pair<Slice, Slice>>
  Seek(const Slice &seek) noexcept {
    stack_.clear();
    Search(seek, b_meta_.Root());
    auto node = stack_.back();
    if (node.index_ == -1 || (std::size_t)node.index_ >= node.Size()) {
      PrintStack();
      LOG_INFO("not found index better than count {} > {}", node.index_,
               node.Size());
      return std::nullopt;
    }
    return GetKeyValue();
  }

  // Get the current leaf node
  [[nodiscard]] Node &GetNode() noexcept {
    assert(!stack_.empty());
    auto &node = stack_.back();
    // if the top of the stack is a leaf then just return it
    LOG_DEBUG("Get leaf node");
    if (node.n_) {
      LOG_DEBUG("node exists");
      assert(node.n_->IsLeaf());
      return *node.n_;
    }
    // we only have the page, so we have to construct the node (in memory page)
    // to do so we need the parent node so we have to start from root and
    // construct the node from top down we will cache all the nodes along the
    // way
    auto cur = stack_[0].n_;
    LOG_DEBUG("node doesn't exists, reconstruct from id {}",
              stack_[0].p_->Id());
    if (cur == nullptr) {
      cur = &tx_cache_.GetOrCreateNode(stack_[0].p_->Id(), nullptr);
    }
    for (int i = 0; i < (int)stack_.size() - 1; i++) {
      assert(!stack_[i].IsLeaf());
      cur = &tx_cache_.GetNodeChild(*cur, stack_[i].index_);
    }
    assert(cur->IsLeaf());
    return *cur;
  }

private:
  // Get the key and value that the cursor is pointing at (should be a leaf
  // element)
  [[nodiscard]] std::pair<Slice, Slice> GetKeyValue() const noexcept {
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
    stack_.push_back(TreeNode{p, n});
    auto &node = stack_.back();
    if (node.IsLeaf()) {
      LOG_INFO("is leaf pid: {}", node.p_->Id());
      if (node.n_) {
        // LOG_INFO("node : {}", node.n_->ToString());
        auto [index, _] = node.n_->FindFirstGreaterOrEqualTo(key);
        index_ = index;
        // LOG_INFO("node : {}, index: {}, search key: {}", node.n_->ToString(),
        //          index_, key.ToString());
        node.index_ = index_;
      } else {
        auto &p = node.p_->AsPage<LeafPage>();
        // LOG_INFO("hi: {}", node.p_->Id());
        index_ = p.FindLastLessThan(key) + 1;
        // LOG_INFO("index: {}", index_);
        node.index_ = index_;
      }
    } else {
      LOG_INFO("is branch pid: {}", node.p_->Id());
      const auto [index, exact] =
          node.n_
              ? node.n_->FindFirstGreaterOrEqualTo(key)
              : node.p_->AsPage<BranchPage>().FindFirstGreaterOrEqualTo(key);

      std::size_t adjusted_index = (!exact && index > 0) ? index - 1 : index;
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
    int32_t index_{-1};

    explicit TreeNode(std::pair<Page *, Node *> pair) noexcept

        : p_{pair.first}, n_{pair.second} {}

    TreeNode(Page *p, Node *n) noexcept : p_{p}, n_{n} {}

    [[nodiscard]] std::size_t Size() const noexcept {
      if (n_)
        return n_->GetElements().size();
      return p_->Count();
    }
    [[nodiscard]] bool IsLeaf() const noexcept {
      if (n_) {
        return n_->IsLeaf();
      }
      LOG_INFO("p {}", static_cast<const void *>(p_));
      return (p_->Flags() & static_cast<std::size_t>(PageFlag::LeafPage)) != 0;
    }
  };
  void PrintStack() const noexcept {
    LOG_INFO("=== Cursor Stack Trace ===");
    for (size_t i = 0; i < stack_.size(); ++i) {
      const auto &node = stack_[i];
      if (node.n_) {
        LOG_INFO("[{}] Node: ptr={}, index={}, leaf={}, elements={}", i,
                 static_cast<const void *>(node.n_), node.index_,
                 node.n_->IsLeaf(), node.n_->GetElements().size());
      } else {
        LOG_INFO("[{}] Page: id={}, index={}, leaf={}, count={}", i,
                 node.p_ ? node.p_->Id() : 0, node.index_, node.IsLeaf(),
                 node.p_ ? node.p_->Count() : 0);
      }
    }
    LOG_INFO("=== End Stack Trace ===");
  }

  ShadowPageHandler &tx_cache_;
  const BucketMeta &b_meta_;
  std::size_t index_;
  std::vector<TreeNode> stack_;
};
} // namespace kv
