#include "tx_cache.h"
#include "bucket.h"
namespace kv {

[[nodiscard]] std::optional<Error>
ShadowPageHandler::Spill(Meta &meta, Buckets &buckets) noexcept {
  LOG_INFO("Starting Spill: preparing nodes for persistence.");

  std::vector<Node *> nodes_to_process;
  std::vector<std::unique_ptr<Node>> owned_new_roots;
  std::vector<Node *> old_roots;

  // Collect raw pointers to existing nodes
  for (auto &[pgid, n] : nodes_) {
    LOG_DEBUG("Collecting node with pgid {}", pgid);
    nodes_to_process.push_back(&n);
  }

  LOG_DEBUG("Collected {} nodes. Sorting by descending depth.",
            nodes_to_process.size());

  std::sort(nodes_to_process.begin(), nodes_to_process.end(),
            [](Node *a, Node *b) { return a->GetDepth() > b->GetDepth(); });

  std::size_t i = 0;
  while (i < nodes_to_process.size()) {
    Node &n = *nodes_to_process[i++];
    LOG_INFO("Processing node at depth {}: {}", n.GetDepth(), n.ToString());

    auto new_nodes_opt = SplitNode(n);

    if (new_nodes_opt.has_value()) {
      LOG_INFO("Node split into {} sub-nodes.", new_nodes_opt->size());

      if (!n.GetParent().has_value()) {
        LOG_DEBUG("Node has no parent -> it is root");

        if (n.GetPgid().has_value()) {
          old_roots.push_back(&n);
        }

        owned_new_roots.push_back(std::make_unique<Node>(nullptr, false));
        Node *new_root_ptr = owned_new_roots.back().get();
        n.SetParent(new_root_ptr);

        // Only process new root after the current pass completes
        nodes_to_process.push_back(new_root_ptr);
      }

      for (auto &new_node : *new_nodes_opt) {
        std::size_t sz = new_node.GetStorageSize();
        auto p_or_err = AllocateShadowPage(meta, (sz / disk_.PageSize()) + 1);
        if (!p_or_err) {
          return p_or_err.error();
        }

        auto &p = p_or_err.value().get();
        new_node.Write(p);
        new_node.SetPgid(p.Id());
        new_node.SetParent(n.GetParentPtr());

        LOG_DEBUG("Sub-node written to page {}.", p.Id());

        Slice old_key = (&new_node == &new_nodes_opt->at(0))
                            ? n.GetParentKey()
                            : new_node.GetElements()[0].key_;
        if (auto parent = new_node.GetParent()) {
          Node &pn = parent.value().get();
          pn.Put(old_key, new_node.GetElements()[0].key_, {},
                 new_node.GetPgid().value());
        }
      }

    } else {
      LOG_INFO("Node did not require splitting. Writing as is: {}",
               n.ToString());

      std::size_t sz = n.GetStorageSize();
      auto p_or_err = AllocateShadowPage(meta, (sz / disk_.PageSize()) + 1);
      if (!p_or_err) {
        return p_or_err.error();
      }

      auto &p = p_or_err.value().get();
      n.Write(p);
      LOG_DEBUG("{}", n.ToString());

      if (!n.GetParent().has_value() && n.GetPgid().has_value()) {
        LOG_DEBUG("Node has no parent and has pgid updating root");
        buckets.UpdateRoot(n.GetPgid().value(), p.Id());
      }

      n.SetPgid(p.Id());

      if (auto parent = n.GetParent()) {
        Node &pn = parent.value().get();
        pn.Put(n.GetParentKey(), n.GetElements()[0].key_, {}, p.Id());
      }
    }
  }

  for (auto *old_root : old_roots) {
    LOG_DEBUG("Updating bucket root from {} to {}", old_root->GetPgid().value(),
              old_root->Root().GetPgid().value());
    buckets.UpdateRoot(old_root->GetPgid().value(),
                       old_root->Root().GetPgid().value());
  }

  LOG_INFO("Spill complete. All nodes persisted.");
  return {};
}
} // namespace kv
