#pragma once

#include "bucket.h"
#include "node.h"
#include "page.h"
#include <vector>
namespace kv {
class Cursor {
public:
  explicit Cursor() = default;

private:
  struct Element {
    Page *p_;
    Node *n_;
  };
  Bucket *b_;
  std::vector<Element> stack_;
};
} // namespace kv
