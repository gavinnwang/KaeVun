#pragma once
#include "disk.h"
#include "node.h"
#include "page.h"
#include "type.h"
#include <sys/signal.h>
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
  [[nodiscard]] std::optional<Error> Write() noexcept {
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
        n.GetStorageSize() < disk_.PageSize()) {
      LOG_DEBUG("No split needed. Node has only {} elements and size {} bytes.",
                n.GetElements().size(), n.GetStorageSize());
      return {};
    }

    LOG_DEBUG("Splitting node with {} elements and size {} bytes.",
              n.GetElements().size(), n.GetStorageSize());

    std::vector<Node> nodes;
    std::size_t threshold = disk_.PageSize() / 2;
    std::size_t cur_size = PAGE_HEADER_SIZE;
    Node cur_node;

    std::size_t index = 0;
    for (const auto &e : n.GetElements()) {
      std::size_t e_size = n.GetElementSize() + e.val_.Size() + e.key_.Size();

      bool can_split = cur_node.GetElements().size() > MIN_KEY_PER_PAGE &&
                       index < n.GetElements().size() - MIN_KEY_PER_PAGE &&
                       cur_size + e_size > threshold;

      if (can_split) {
        LOG_DEBUG("Threshold reached. Finalizing current node with {} "
                  "elements, estimated size {} bytes.",
                  cur_node.GetElements().size(), cur_size);
        nodes.push_back(std::move(cur_node));
        cur_node = Node{};
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
    return nodes;
  }

  [[nodiscard]] std::optional<Error> Spill(Meta &meta) noexcept {
    LOG_INFO("Starting Spill: preparing nodes for persistence.");

    std::vector<Node *> nodes;
    std::vector<Node> new_roots;
    for (auto &[pgid, n] : nodes_) {
      LOG_DEBUG("Collecting node with pgid {}", pgid);
      nodes.push_back(&n);
    }

    LOG_DEBUG("Collected {} nodes. Sorting by descending depth.", nodes.size());

    // Sort by descending depth to ensure children are spilled before parents
    std::sort(nodes.begin(), nodes.end(),
              [](Node *a, Node *b) { return a->GetDepth() > b->GetDepth(); });

    for (std::size_t i = 0; i < nodes.size(); i++) {
      auto &n = *nodes[i];
      LOG_INFO("Processing node at depth {}: {}", n.GetDepth(), n.ToString());

      if (!n.GetParent().has_value()) { // node is root
      }

      auto new_nodes_opt = SplitNode(n);

      if (!n.GetParent().has_value()) {
        LOG_DEBUG("Node has no parent");
        if (n.GetPgid().has_value()) {
          LOG_DEBUG("pgid {}", n.GetPgid().value());
        } else {
          LOG_DEBUG("node has no pgid weird");
        }
        if (new_nodes_opt.has_value()) {
          LOG_DEBUG("created new root {}");
          auto new_root = Node{nullptr};
          nodes.push_back(&new_root);
          new_roots.push_back(std::move(new_root));
        }
      }

      if (new_nodes_opt.has_value()) {
        LOG_INFO("Node split into {} sub-nodes.", new_nodes_opt->size());
        if (!n.GetParent().has_value()) { // node is root
          // n.SetParent()
        }

        for (std::size_t j = 0; j < new_nodes_opt->size(); j++) {
          auto &new_node = new_nodes_opt->at(j);
          std::size_t sz = new_node.GetStorageSize();

          LOG_DEBUG("Allocating page for sub-node {} of size {} bytes.", j, sz);

          auto p_or_err = AllocateShadowPage(meta, (sz / disk_.PageSize()) + 1);
          if (!p_or_err) {
            LOG_INFO("Failed to allocate page for sub-node {}.", j);
            return p_or_err.error();
          }

          auto &p = p_or_err.value().get();
          new_node.Write(p);
          new_node.SetPgid(p.Id());
          new_node.SetParent(n.GetParentPtr());

          LOG_DEBUG("Sub-node {} written to page {}.", j, p.Id());

          Slice old_key =
              (j == 0) ? n.GetParentKey() : new_node.GetElements()[0].key_;
          auto parent = new_node.GetParent();
          if (parent.has_value()) {
            LOG_DEBUG("Updating parent with new key mapping: old key '{}', new "
                      "key '{}'.",
                      old_key.ToString(),
                      new_node.GetElements()[0].key_.ToString());

            parent.value().get().Put(old_key, new_node.GetElements()[0].key_,
                                     {}, new_node.GetPgid().value());
          }
        }

      } else {
        LOG_INFO("Node did not require splitting. Writing as is.");

        std::size_t sz = n.GetStorageSize();

        LOG_DEBUG("Allocating page for node of size {} bytes.", sz);

        auto p_or_err = AllocateShadowPage(meta, (sz / disk_.PageSize()) + 1);
        if (!p_or_err) {
          LOG_INFO("Failed to allocate page for node.");
          return p_or_err.error();
        }

        auto &p = p_or_err.value().get();
        n.Write(p);
        n.SetPgid(p.Id());

        LOG_DEBUG("Node written to page {}.", p.Id());

        auto parent = n.GetParent();
        if (parent.has_value()) {
          LOG_DEBUG("Updating parent with key '{}'.",
                    n.GetElements()[0].key_.ToString());

          parent.value().get().Put(n.GetParentKey(), n.GetElements()[0].key_,
                                   {}, n.GetPgid().value());
        }
      }
    }

    LOG_INFO("Spill complete. All nodes persisted.");
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
