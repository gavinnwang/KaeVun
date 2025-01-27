#pragma once

#include "bucket.h"
#include "node.h"
#include "page.h"
#include <vector>
namespace kv {
class Cursor {
public:
  Cursor() = default;

private:
  struct TreeNode {
    Page *p_;
    Node *n_;
  };
  BucketTx *b_;
  std::vector<TreeNode> stack_;
};
} // namespace kv
