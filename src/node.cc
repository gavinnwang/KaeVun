#include "node.h"
#include "tx.h"

namespace kv {

Node *Node::ChildAt(int index) {
  assert(!is_leaf_);
  return tx_->GetNode(elements_[index].pgid_, this);
}
} // namespace kv
