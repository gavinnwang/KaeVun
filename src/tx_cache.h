#pragma once
#include "disk.h"
#include "node.h"
#include "page.h"
#include "type.h"
#include <unordered_map>
#include <vector>
namespace kv {

class ShadowPageHandler {
public:
  explicit ShadowPageHandler(DiskHandler &disk, bool writable)
      : writable_(writable), disk_(disk) {};

  std::vector<Node> &Pending() noexcept { return pending_; }

  // GetPage returns a reference to the page with a given id.
  // If the page has been written to then a temporary bufferred page is
  // returned.
  [[nodiscard]] Page &GetPage(Pgid pgid) noexcept {
    if (auto it = shadow_pages_.find(pgid); it != shadow_pages_.end()) {
      return it->second.Get();
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
    auto [it, ok] = nodes_.emplace(pgid, Node{parent});
    assert(ok);
    Node &node = it->second;

    if (parent)
      node.SetDepth(parent->GetDepth() + 1);

    // read page into node …
    Page &p = GetPage(pgid);
    node.Read(p);

    return node;
  }

  [[nodiscard]] Node &GetNodeChild(Node &parent, uint32_t index) noexcept {
    assert(!parent.IsLeaf());
    return GetNode(parent.GetElements()[index].pgid_, &parent);
  }

  // Returns in-mmeory node if it exists. Otherwise returns the underlying page.
  [[nodiscard]] std::pair<Page *, Node *> GetPageOrNode(Pgid pgid) noexcept {
    if (auto it = nodes_.find(pgid); it != nodes_.end()) {
      LOG_INFO("found node {}", pgid);
      return {std::addressof(GetPage(pgid)), std::addressof(it->second)};
    }
    return {std::addressof(GetPage(pgid)), nullptr};
  }

  // Write write any dirty pages to disk.
  [[nodiscard]] std::optional<Error> Write() noexcept {
    // Collect dirty pages
    std::vector<Page *> dirty_pages;
    dirty_pages.reserve(shadow_pages_.size());
    for (auto &[_, p] : shadow_pages_) {
      dirty_pages.push_back(&p.Get());
    }

    // Sort pages by their pgid
    std::sort(dirty_pages.begin(), dirty_pages.end(),
              [](const Page *a, const Page *b) { return a->Id() < b->Id(); });

    // Write pages to disk in order
    for (const auto p : dirty_pages) {
      auto e = disk_.WritePage(*p);
      assert(!e);
    }
    auto e = disk_.Sync();
    assert(!e);

    // Clear out the page cache.
    shadow_pages_.clear();

    return {};
  }

  [[nodiscard]] std::expected<std::reference_wrapper<Page>, Error>
  AllocateShadowPage(Meta &meta, uint32_t count) {
    auto p_or_err = disk_.Allocate(meta, count);
    if (!p_or_err) {
      return std::unexpected{p_or_err.error()};
    }
    auto &shadow_page = p_or_err.value();
    auto &p = shadow_page.Get();
    LOG_INFO("Allocated page with id {}, {}", shadow_page.Get().Id(),
             static_cast<const void *>(&shadow_page.Get()));
    // save to page cache
    shadow_pages_.insert({shadow_page.Get().Id(), std::move(shadow_page)});
    return p;
  }

  [[nodiscard]] std::vector<Node> SplitNode(Node &n) noexcept {
    // Ignore the split if the node doesn't have at least enough elements for
    // multiple pages or if the data can fit on a single page.
    if (n.GetElements().size() <= MIN_KEY_PER_PAGE * 2 || n.Get)
      return {};
  }

  [[nodiscard]] std::optional<Error> Spill() noexcept {
    std::vector<Node *> nodes;
    for (auto &[_, n] : nodes_) {
      nodes.push_back(&n);
    }
    // Sort by descending depth
    std::sort(nodes.begin(), nodes.end(),
              [](Node *a, Node *b) { return a->GetDepth() > b->GetDepth(); });

    for (Node *n : nodes) {
    }
    return {};
  }

private:
  std::vector<Node> pending_;
  // Dirty shadow pages, only used for write only transactions
  std::unordered_map<Pgid, ShadowPage> shadow_pages_{};
  // nodes_ represents the in-memory version of pages allowing for key value
  // changes.
  std::unordered_map<Pgid, Node> nodes_{};
  [[maybe_unused]] const bool writable_;
  DiskHandler &disk_;
};
} // namespace kv
