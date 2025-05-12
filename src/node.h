#pragma once

#include "fmt/format.h"
#include "page.h"
#include "persist.h"
#include "slice.h"
#include <cstddef>
#include <vector>

namespace kv {

// in memory version of a page
class Node {

private:
  struct NodeElement {
    Pgid pgid_;
    Slice key_;
    Slice val_;
  };
  std::vector<NodeElement> elements_;
  bool is_leaf_ = true;
  std::size_t depth_{0};
  // The node has empty pgid if it is newly created and hasn't claimed a page id
  // yet todo
  std::optional<Pgid> pgid_;
  // the parent node
  Node *parent_ = nullptr;
  // the key that the parent node uses to direct to us
  Slice parent_key_;

public:
  Node(Node *parent = nullptr, bool is_leaf = true) noexcept
      : is_leaf_(is_leaf), parent_(parent) {}
  // prevent copying
  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;

  // allow moving
  Node(Node &&) noexcept = default;
  Node &operator=(Node &&) noexcept = default;

  [[nodiscard]] std::string ToString() const noexcept {
    std::vector<std::string> element_strs;

    for (const auto &node : elements_) {
      if (is_leaf_) {
        element_strs.push_back(fmt::format(
            "(key: {}, val: {})", node.key_.ToString(), node.val_.ToString()));
      } else {
        element_strs.push_back(fmt::format("(key: {}, pgid: {})",
                                           node.key_.ToString(), node.pgid_));
      }
    }

    std::string node_type = is_leaf_ ? "Leaf" : "Branch";
    std::string parent_key_str = parent_ ? parent_key_.ToString() : "None";
    std::string pgid_str = pgid_.has_value() ? std::to_string(*pgid_) : "None";

    return fmt::format(
        "Node(Type: {}, Depth: {}, PageID: {}, ParentKey: {}, Elements: [{}])",
        node_type, depth_, pgid_str, parent_key_str,
        fmt::join(element_strs, ", "));
  }

  void Read(Page &p) noexcept {
    pgid_ = p.Id();
    is_leaf_ = (p.Flags() & static_cast<std::size_t>(PageFlag::LeafPage));
    elements_.resize(p.Count());
    for (std::size_t i = 0; i < p.Count(); i++) {
      if (is_leaf_) {
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        elements_[i].key_ = leaf_p.GetKey(i);
        elements_[i].val_ = leaf_p.GetVal(i);
      } else {
        auto &branch_p = p.AsPage<BranchPage>();
        elements_[i].key_ = branch_p.GetKey(i);
        elements_[i].pgid_ = branch_p.GetPgid(i);
      }
    }
    if (!elements_.empty()) {
      // save first key for spilling
      parent_key_ = elements_.front().key_;
    }
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
    std::size_t data_offset = GetHeaderSize();

    serializer.Seek(data_offset);
    for (std::size_t i = 0; i < elements_.size(); i++) {
      if (is_leaf_) {
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        auto &e = leaf_p.GetElement(i);

        std::size_t cur_offset = serializer.Offset();
        e.offset_ = cur_offset;

        e.ksize_ = elements_[i].key_.Size();
        e.vsize_ = elements_[i].val_.Size();

        serializer.WriteBytes(elements_[i].key_.Data(), e.ksize_);
        serializer.WriteBytes(elements_[i].val_.Data(), e.vsize_);
      } else {
        BranchPage &branch_p = p.AsPage<BranchPage>();
        auto &e = branch_p.GetElement(i);

        std::size_t cur_offset = serializer.Offset();
        e.offset_ = cur_offset;

        e.ksize_ = elements_[i].key_.Size();
        e.pgid_ = elements_[i].pgid_;

        serializer.WriteBytes(elements_[i].key_.Data(), e.ksize_);
        assert(elements_[i].val_.Size() == 0);
      }
    }
    assert(serializer.Offset() == GetStorageSize());
  }

  [[nodiscard]] std::size_t GetStorageSize() const noexcept {
    auto sz = GetHeaderSize();
    for (const auto &e : elements_) {
      sz += e.key_.Size() + e.val_.Size();
    }
    return sz;
  }

  [[nodiscard]] std::size_t GetElementHeaderSize() const noexcept {
    return (is_leaf_ ? sizeof(LeafElement) : sizeof(BranchElement));
  }

  [[nodiscard]] std::size_t GetHeaderSize() const noexcept {
    std::size_t header_size =
        PAGE_HEADER_SIZE + elements_.size() * GetElementHeaderSize();
    return header_size;
  }

  // todo change this to own the memory
  void Put(const Slice &key, const Slice &val) noexcept { Put(key, key, val); }

  // todo change this to own the memory
  void Put(const Slice &key, Pgid pgid) noexcept { Put(key, key, {}, pgid); }

  void Put(const Slice &old_key, const Slice &new_key, const Slice &val,
           Pgid pgid = 0) noexcept {
    auto [index, exact] = FindFirstGreaterOrEqualTo(old_key);
    if (!exact) {
      elements_.insert(elements_.begin() + index, {pgid, new_key, val});
    } else {
      elements_[index] = {pgid, new_key, val};
    }
  }

  [[nodiscard]] std::pair<std::size_t, bool>
  FindFirstGreaterOrEqualTo(const Slice &key) const noexcept {
    for (std::size_t i = 0; i < elements_.size(); ++i) {
      if (elements_[i].key_ >= key) { // i.e., elements_[i].key_ >= key
        bool exact = (elements_[i].key_ == key);
        return {i, exact};
      }
    }
    // If not found, return size() and false
    return {static_cast<std::size_t>(elements_.size()), false};
  }

  [[nodiscard]] Node &Root(std::size_t depth = 0) noexcept {
    LOG_DEBUG("getting root at depth {}", depth);

    // if (depth > 5) {
    //   LOG_ERROR("Exceeded maximum stack depth while traversing to root");
    //   std::abort();
    // }

    if (parent_) {
      LOG_DEBUG("parent {}", parent_->ToString());
      return parent_->Root(depth + 1);
    }

    return *this;
  }

  void SetParent(Node *parent) noexcept {
    assert(parent != this);
    parent_ = parent;
  }

  void SetDepth(int depth) noexcept { depth_ = depth; }

  [[nodiscard]] std::optional<std::reference_wrapper<Node>>
  GetParent() const noexcept {
    assert(parent_ != this);
    // parent_ can only be nullptr when it is the root
    // assert(depth_ == 0 || parent_ != nullptr);
    if (parent_) {
      return std::ref(*parent_);
    } else {
      return std::nullopt;
    }
  }

  [[nodiscard]] Node *GetParentPtr() const noexcept {
    // parent_ can only be nullptr when it is the root
    // assert(depth_ == 0 || parent_ != nullptr);
    return parent_;
  }

  [[nodiscard]] std::size_t GetDepth() const noexcept { return depth_; }

  [[nodiscard]] bool IsLeaf() const noexcept { return is_leaf_; }

  [[nodiscard]] Slice GetParentKey() const noexcept { return parent_key_; }

  [[nodiscard]] std::optional<Pgid> GetPgid() const noexcept { return pgid_; }

  void SetPgid(Pgid pgid) noexcept { pgid_ = pgid; }

  [[nodiscard]] std::vector<NodeElement> &GetElements() noexcept {
    return elements_;
  }

  [[nodiscard]] const std::vector<NodeElement> &GetElements() const noexcept {
    return elements_;
  }
};
} // namespace kv
