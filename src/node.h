#pragma once

#include "page.h"
#include "slice.h"
#include <cstddef>
#include <cstdint>
#include <vector>
namespace kv {

// in memory version of a page
class Node {
public:
  void Read(Page &p) noexcept {
    is_leaf_ = (p.Flags() & static_cast<uint32_t>(PageFlag::LeafPage));
    nodes_.resize(p.Count());
    for (uint32_t i = 0; i < p.Count(); i++) {
      if (is_leaf_) {
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        auto &element = leaf_p.GetElement(i);
        nodes_[i].key_ = leaf_p.GetKey(i);
        nodes_[i].val_ = leaf_p.GetVal(i);
      } else {
        auto &branch_p = p.AsPage<BranchPage>();
        auto &elements = branch_p.GetElement(i);
        nodes_[i].key_ = branch_p.GetKey(i);
        nodes_[i].pgid_ = branch_p.GetPgid(i);
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
    p.SetCount(nodes_.size());
    uint32_t cur_offset = sizeof(p);
    for (uint32_t i = 0; i < p.Count(); i++) {
      if (is_leaf_) {
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        auto &e = leaf_p.GetElement(i);
        e.offset_ = static_cast<uint32_t>(std::uintptr_t(cur_offset) -
                                          std::uintptr_t(std::addressof(p)));
        e.ksize_ = nodes_[i].key_.size();
        e.vsize_ = nodes_[i].val_.size();

        cur_offset += e.ksize_ + e.vsize_;
      } else {
        auto &branch_p = p.AsPage<BranchPage>();
        auto &e = branch_p.GetElement(i);
        e.offset_ = static_cast<uint32_t>(std::uintptr_t(cur_offset) -
                                          std::uintptr_t(std::addressof(p)));

        e.ksize_ = nodes_[i].key_.size();
        e.pgid_ = nodes_[i].pgid_;

        cur_offset += e.ksize_;
      }
    }
  }

  void Put(Slice &key, Slice &val, Pgid pgid) {
    for (uint32_t i = 0; i < nodes_.size(); i++) {
      if (nodes_[i].key_.compare(key)) {
      }
    }
  }

private:
  struct NodeElement {
    Pgid pgid_;
    Slice key_;
    Slice val_;
  };
  std::vector<NodeElement> nodes_;
  bool is_leaf_;
};
} // namespace kv
