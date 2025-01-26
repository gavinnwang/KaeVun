#pragma once

#include "log.h"
#include "page.h"
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
        LOG_INFO("hi");
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        leaf_p.SetElement({1, 1, 1}, 0);
        assert(leaf_p.GetElement(0).ksize_ == 1);
        auto &element = leaf_p.GetElement(i);
        nodes_[i].key_ = leaf_p.GetKey(i);
      } else {
        auto &branch_p = p.AsPage<BranchPage>();
        auto &elements = branch_p.GetElement(i);
        nodes_[i].key_ = branch_p.GetKey(i);
      }
    }
  }

private:
  struct NodeElement {
    std::span<std::byte> key_;
    std::span<std::byte> val_;
  };
  std::vector<NodeElement> nodes_;
  bool is_leaf_;
};
} // namespace kv
