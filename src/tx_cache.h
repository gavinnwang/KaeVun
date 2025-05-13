#pragma once
#include "disk.h"
#include "node.h"
#include "page.h"
#include "type.h"
#include <sys/signal.h>
#include <unordered_map>
#include <vector>
namespace kv {

class Buckets;
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

  // GetOrCreateNode creates a node from a page and associates with a given
  // parent.
  [[nodiscard]] Node &GetOrCreateNode(Pgid pgid, Node *parent) noexcept {
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

  [[nodiscard]] Node &GetNodeChild(Node &parent, std::size_t index) noexcept {
    assert(!parent.IsLeaf());
    return GetOrCreateNode(parent.GetElements()[index].pgid_, &parent);
  }

  // Returns in-memory node if it exists. Otherwise returns the underlying page.
  [[nodiscard]] std::pair<Page *, Node *> GetPageOrNode(Pgid pgid) noexcept {
    if (auto it = nodes_.find(pgid); it != nodes_.end()) {
      LOG_INFO("found node {}", pgid);
      return {std::addressof(GetPage(pgid)), std::addressof(it->second)};
    }
    return {std::addressof(GetPage(pgid)), nullptr};
  }

  // Write any dirty pages to disk.
  [[nodiscard]] std::optional<Error> WriteDirtyPages() noexcept {
    LOG_INFO("Starting Write: flushing {} dirty shadow pages to disk.",
             shadow_pages_.size());

    // Collect dirty pages
    std::vector<Page *> dirty_pages;
    dirty_pages.reserve(shadow_pages_.size());
    for (auto &[pgid, p] : shadow_pages_) {
      LOG_DEBUG("Preparing to flush page with id {}", pgid);
      dirty_pages.push_back(&p.Get());
    }

    // Sort pages by their pgid
    LOG_DEBUG("Sorting pages by page id for sequential write.");
    std::sort(dirty_pages.begin(), dirty_pages.end(),
              [](const Page *a, const Page *b) { return a->Id() < b->Id(); });

    // Write pages to disk in sorted order
    for (const auto *p : dirty_pages) {
      LOG_DEBUG("Writing page with id {} to disk.", p->Id());
      auto e = disk_.WritePage(*p);
      assert(!e);
    }

    // Sync to ensure data is durable
    LOG_INFO("Syncing disk to ensure all writes are durable.");
    auto e = disk_.Sync();
    assert(!e);

    // Clear out the page cache after successful flush
    LOG_INFO("Clearing shadow page cache after successful flush.");
    shadow_pages_.clear();

    LOG_INFO("Write complete: all dirty pages flushed and cache cleared.");
    return {};
  }

  [[nodiscard]] std::expected<std::reference_wrapper<Page>, Error>
  AllocateShadowPage(Meta &meta, std::size_t count) {
    auto p_or_err = disk_.Allocate(meta, count);
    if (!p_or_err) {
      return std::unexpected{p_or_err.error()};
    }
    auto &shadow_page = p_or_err.value();
    auto &p = shadow_page.Get();
    LOG_INFO("Allocated page with id {}, sz {}, {}", shadow_page.Get().Id(),
             count, static_cast<const void *>(&shadow_page.Get()));
    // save to page cache
    shadow_pages_.insert({shadow_page.Get().Id(), std::move(shadow_page)});
    return p;
  }

  [[nodiscard]] std::optional<std::vector<Node>>
  SplitNode(const Node &n) noexcept {
    LOG_INFO("Attempting to split node: {}", n.ToString());

    // Check if split is even needed
    if (n.GetElements().size() <= MIN_KEY_PER_PAGE * 2 ||
        // n.GetStorageSize() < 200) {
        n.GetStorageSize() < disk_.PageSize()) {
      LOG_DEBUG("No split needed. Node has only {} elements and size {} bytes.",
                n.GetElements().size(), n.GetStorageSize());
      return {};
    }

    LOG_DEBUG("Splitting node with {} elements and size {} bytes.",
              n.GetElements().size(), n.GetStorageSize());

    std::vector<Node> nodes;
    // std::size_t threshold = 100;
    std::size_t threshold = disk_.PageSize() / 2;
    std::size_t cur_size = PAGE_HEADER_SIZE;
    Node cur_node{nullptr, n.IsLeaf()};

    std::size_t index = 0;
    for (const auto &e : n.GetElements()) {
      std::size_t e_size =
          n.GetElementHeaderSize() + e.val_.Size() + e.key_.Size();

      bool can_split = cur_node.GetElements().size() >= MIN_KEY_PER_PAGE &&
                       index <= n.GetElements().size() - MIN_KEY_PER_PAGE &&
                       cur_size + e_size >= threshold;

      if (can_split) {
        LOG_DEBUG("Threshold reached. Finalizing current node with {} "
                  "elements, estimated size {} bytes.",
                  cur_node.GetElements().size(), cur_size);
        nodes.push_back(std::move(cur_node));
        cur_node = Node{nullptr, n.IsLeaf()};
        cur_size = PAGE_HEADER_SIZE;
      }

      LOG_DEBUG("Adding element [{}] to current node. Element size: {} bytes.",
                e.key_.ToString(), e_size);

      cur_node.GetElements().push_back(e);
      cur_size += e_size;
      ++index;
    }

    LOG_DEBUG("Finalizing last node with {} elements, estimated size {} bytes.",
              cur_node.GetElements().size(), cur_size);

    nodes.push_back(std::move(cur_node));

    LOG_INFO("Splitting complete. Generated {} new node(s).", nodes.size());
    for (const auto &n : nodes) {
      LOG_DEBUG("node: {}", n.ToString());
    }
    return nodes;
  }

  [[nodiscard]] std::optional<Error> Spill(Meta &meta,
                                           Buckets &buckets) noexcept;

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
