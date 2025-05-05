#pragma once
#include "disk.h"
#include "node.h"
#include "type.h"
#include <unordered_map>
#include <vector>
namespace kv {

class TxCache {
public:
  explicit TxCache(DiskHandler &disk) : disk_(disk) {};

  std::vector<Node> &Pending() noexcept { return pending_; }
  std::unordered_map<Pgid, Page *> &Pages() noexcept { return pages_; }
  std::unordered_map<Pgid, Node> &Nodes() noexcept { return nodes_; }

  // GetPage returns a reference to the page with a given id.
  // If the page has been written to then a temporary bufferred page is
  // returned.
  [[nodiscard]] Page &GetPage(Pgid pgid) noexcept {
    if (auto it = pages_.find(pgid); it != pages_.end()) {
      return *it->second;
    }
    // Return directly from the mmap.
    return disk_.GetPageFromMmap(pgid);
  }

  // GetNode creates a node from a page and associates with a given parent.
  [[nodiscard]] Node &GetNode(Pgid pgid, Node *parent) noexcept {
    // 1. Return existing node if cached.
    if (auto it = nodes_.find(pgid); it != nodes_.end())
      return it->second;

    // 2. Otherwise construct a blank Node in-place inside the map.
    auto [it, ok] = nodes_.emplace(pgid, Node{});
    Node &node = it->second;

    // node.SetTxCache(this);
    node.SetParent(parent);
    if (parent)
      node.SetDepth(parent->GetDepth() + 1);

    // read page into node …
    Page &p = GetPage(pgid);
    node.Read(p);

    return node;
  }

  // Returns in-mmeory node if it exists. Otherwise returns the underlying page.
  [[nodiscard]] std::pair<Page *, Node *>
  GetPageOrNode(Pgid pgid, Node *parent = nullptr) noexcept {
    if (auto it = nodes_.find(pgid); it != nodes_.end()) {
      return {nullptr, std::addressof(it->second)};
    }
    return {std::addressof(GetPage(pgid)), nullptr};
  }

  // Write write any dirty pages to disk.
  [[nodiscard]] std::optional<Error> Write() noexcept {
    // Collect dirty pages
    std::vector<Page *> dirty_pages;
    dirty_pages.reserve(pages_.size());
    for (const auto &[_, page_ptr] : pages_) {
      // dirty_pages.push_back(&page_ptr);
    }

    // Sort pages by their pgid
    std::sort(dirty_pages.begin(), dirty_pages.end(),
              [](const Page *a, const Page *b) { return a->Id() < b->Id(); });

    // Write pages to disk in order
    for (const auto p : dirty_pages) {
      disk_.WritePage(*p);
    }
    disk_.Sync();

    // Clear out the page cache.
    pages_.clear();

    return {};
  }

  [[nodiscard]] std::expected<Page *, Error> Allocate(Meta &meta,
                                                      uint32_t count) {
    auto p_or_err = disk_.Allocate(meta, count);
    if (!p_or_err) {
      return std::unexpected{p_or_err.error()};
    }
    auto p = p_or_err.value();
    return &p.get();
  }

private:
  std::vector<Node> pending_;
  // Page cache
  std::unordered_map<Pgid, Page *> pages_{};
  // nodes_ represents the in-memory version of pages allowing for key value
  // changes.
  std::unordered_map<Pgid, Node> nodes_{};
  DiskHandler &disk_;
};
} // namespace kv
