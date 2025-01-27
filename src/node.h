#pragma once

#include "fmt/format.h"
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
    // skip all the header
    uint32_t cur_offset =
        sizeof(p) + nodes_.size() * (is_leaf_ ? sizeof(LeafElement)
                                              : sizeof(BranchElement));
    auto *page_data = reinterpret_cast<std::byte *>(&p);
    for (uint32_t i = 0; i < p.Count(); i++) {
      if (is_leaf_) {
        LeafPage &leaf_p = p.AsPage<LeafPage>();
        auto &e = leaf_p.GetElement(i);

        e.offset_ = cur_offset;
        e.ksize_ = nodes_[i].key_.Size();
        e.vsize_ = nodes_[i].val_.Size();

        // LOG_DEBUG("offset: {}, ksz: {}, vsz: {}", cur_offset, e.ksize_,
        //           e.vsize_);

        std::memcpy(page_data + cur_offset, nodes_[i].key_.Data(), e.ksize_);
        cur_offset += e.ksize_;
        std::memcpy(page_data + cur_offset, nodes_[i].val_.Data(), e.vsize_);
        cur_offset += e.vsize_;
      } else {
        auto &branch_p = p.AsPage<BranchPage>();
        auto &e = branch_p.GetElement(i);

        e.offset_ = cur_offset;
        e.ksize_ = nodes_[i].key_.Size();
        e.pgid_ = nodes_[i].pgid_;

        memcpy(page_data + cur_offset, nodes_[i].key_.Data(), e.ksize_);
        cur_offset += e.ksize_;
      }
    }
  }

  void Put(Slice &key, Slice &val) {
    auto index = Search(key);
    nodes_.insert(nodes_.begin() + index, {0, key, val});
  }

  void Put(Slice &key, Pgid pgid) {
    auto index = Search(key);
    nodes_.insert(nodes_.begin() + index, {pgid, key, {}});
  }

  uint32_t Search(Slice &key) const noexcept {
    int index = -1;
    for (uint32_t i = 0; i < nodes_.size(); i++) {
      if (nodes_[i].key_.Compare(key) < 0) {
        index = i;
      }
    }
    if (index == -1)
      index = nodes_.size();
    return index;
  }

  [[nodiscard]] std::string ToString() const noexcept {
    std::vector<std::string> elements;

    for (const auto &node : nodes_) {
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

private:
  std::vector<NodeElement> nodes_;
  bool is_leaf_;
};
} // namespace kv
