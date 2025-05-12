#include "tx_cache.h"
#include "bucket.h"
namespace kv {

[[nodiscard]] std::optional<Error>
ShadowPageHandler::Spill(Meta &meta, Buckets &buckets) noexcept {
  LOG_INFO("Starting Spill: preparing nodes for persistence.");

  std::vector<Node *> nodes;
  std::vector<Node> new_roots;
  std::vector<Node *> old_roots;
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
      if (new_nodes_opt.has_value()) {
        if (n.GetPgid().has_value()) {
          LOG_DEBUG("pgid {}", n.GetPgid().value());
          old_roots.push_back(&n);
          LOG_DEBUG("old root got {}", n.ToString());
        } else {
          LOG_DEBUG("node has no pgid weird");
        }
        LOG_DEBUG("created new root {}");
        auto new_root = Node{nullptr, false};
        nodes.push_back(&new_root);
        n.SetParent(&new_root);
        new_roots.push_back(std::move(new_root));
      }
    }

    if (new_nodes_opt.has_value()) {
      LOG_INFO("Node split into {} sub-nodes.", new_nodes_opt->size());
      // assert(new_nodes_opt->size() > 1);

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

          auto &pn = parent.value().get();
          pn.Put(old_key, new_node.GetElements()[0].key_, {},
                 new_node.GetPgid().value());
          LOG_DEBUG("parent after update {}", pn.ToString());
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

      if (!n.GetParent().has_value()) {
        if (n.GetPgid().has_value()) {
          LOG_DEBUG("Node has no parent and has pgid updating root");
          buckets.UpdateRoot(n.GetPgid().value(), p.Id());
        }
      }
      n.SetPgid(p.Id());

      LOG_DEBUG("Node written to page {}.", p.Id());

      auto parent = n.GetParent();
      if (parent.has_value()) {
        LOG_DEBUG("Updating parent with key '{}'.",
                  n.GetElements()[0].key_.ToString());

        parent.value().get().Put(n.GetParentKey(), n.GetElements()[0].key_, {},
                                 p.Id());
      }
    }
  }

  for (auto *old_root : old_roots) {
    LOG_DEBUG("old root{}", old_root->ToString());
    LOG_DEBUG("Updating bucket root from {} to {}", old_root->GetPgid().value(),
              old_root->Root().GetPgid().value());
    buckets.UpdateRoot(old_root->GetPgid().value(),
                       old_root->Root().GetPgid().value());
  }

  LOG_INFO("Spill complete. All nodes persisted.");
  return {};
}
} // namespace kv
