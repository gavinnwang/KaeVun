#pragma once

#include "fmt/format.h"
#include "page.h"
#include "persist.h"
#include "slice.h"
#include <compare>
#include <cstddef>
#include <cstdint>
#include <vector>
namespace kv {

// in memory version of a page
class Node {

public:
  void Read(Page &p) noexcept {
    is_leaf_ = (p.Flags() & static_cast<uint32_t>(PageFlag::LeafPage));
    elements_.resize(p.Count());
    for (uint32_t i = 0; i < p.Count(); i++) {
      if (is_leaf_) {
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        auto &element = leaf_p.GetElement(i);
        elements_[i].key_ = leaf_p.GetKey(i);
        elements_[i].val_ = leaf_p.GetVal(i);
      } else {
        auto &branch_p = p.AsPage<BranchPage>();
        auto &elements = branch_p.GetElement(i);
        elements_[i].key_ = branch_p.GetKey(i);
        elements_[i].pgid_ = branch_p.GetPgid(i);
      }
    }
    // save first key for spliting
  }

  void Write(Page &p) const noexcept {
    if (is_leaf_) {
      p.SetFlags(PageFlag::LeafPage);
    } else {
      p.SetFlags(PageFlag::BranchPage);
    }
    p.SetCount(elements_.size());

    Serializer serializer(&p);
    // skip all the header
    uint32_t data_offset =
        sizeof(p) +
        p.Count() * (is_leaf_ ? sizeof(LeafElement) : sizeof(BranchElement));

    serializer.Seek(data_offset);
    for (uint32_t i = 0; i < p.Count(); i++) {
      if (is_leaf_) {
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        auto &e = leaf_p.GetElement(i);

        uint32_t cur_offset = serializer.Offset();
        e.offset_ = cur_offset;

        e.ksize_ = elements_[i].key_.Size();
        e.vsize_ = elements_[i].val_.Size();

        serializer.WriteBytes(elements_[i].key_.Data(), e.ksize_);
        serializer.WriteBytes(elements_[i].val_.Data(), e.vsize_);
      } else {
        BranchPage &branch_p = p.AsPage<BranchPage>();
        auto &e = branch_p.GetElement(i);

        uint32_t cur_offset = serializer.Offset();
        e.offset_ = cur_offset;

        e.ksize_ = elements_[i].key_.Size();
        e.pgid_ = elements_[i].pgid_;

        serializer.WriteBytes(elements_[i].key_.Data(), e.ksize_);
      }
    }
  }

  void Put(Slice &key, Slice &val) noexcept {
    auto index = FindLastLessThan(key) + 1;
    elements_.insert(elements_.begin() + index, {0, key, val});
  }

  void Put(Slice &key, Pgid pgid) noexcept {
    auto index = FindLastLessThan(key) + 1;
    elements_.insert(elements_.begin() + index, {pgid, key, {}});
  }

  // Finds the index of the last element whose key is strictly less than the
  // given key.
  // Returns -1 if all the keys are greater than the input key.
  [[nodiscard]] uint32_t FindLastLessThan(const Slice &key) const noexcept {
    for (int i = elements_.size() - 1; i >= 0; --i) {
      if (elements_[i].key_ < key)
        return i;
    }
    return -1; // key < all existing keys
  }
  [[nodiscard]] std::pair<uint32_t, bool>
  FindFirstGreaterOrEqualTo(const Slice &key) const noexcept {
    for (uint32_t i = 0; i < elements_.size(); ++i) {
      if (!(elements_[i].key_ < key)) { // i.e., elements_[i].key_ >= key
        bool exact = (elements_[i].key_ == key);
        return {i, exact};
      }
    }
    // If not found, return size() and false
    return {static_cast<uint32_t>(elements_.size()), false};
  }

  [[nodiscard]] std::string ToString() const noexcept {
    std::vector<std::string> elements;

    for (const auto &node : elements_) {
      if (is_leaf_) {
        elements.push_back(fmt::format("( key: {}, val: {} )",
                                       node.key_.ToString(),
                                       node.val_.ToString()));
      } else {
        elements.push_back(fmt::format("( key: {}, pgid: {} )",
                                       node.key_.ToString(), node.pgid_));
      }
    }

    return fmt::format("[{}]", fmt::join(elements, ", "));
  }

  struct NodeElement {
    Pgid pgid_;
    Slice key_;
    Slice val_;
  };

  [[nodiscard]] Node *Root() noexcept {
    return parent_ ? parent_->Root() : this;
  }

  [[nodiscard]] Node *ChildAt(int index);

  void SetParent(Node *parent) noexcept { parent_ = parent; }

  void SetDepth(int depth) noexcept { depth_ = depth; }

  [[nodiscard]] int GetDepth() const noexcept { return depth_; }

  [[nodiscard]] bool IsLeaf() const noexcept { return is_leaf_; }

  [[nodiscard]] std::vector<NodeElement> GetElements() const noexcept {
    return elements_;
  }

private:
  std::vector<NodeElement> elements_;
  bool is_leaf_;
  int depth_;
  // TxCache *tx_;
  Node *parent_ = nullptr;
};
} // namespace kv
