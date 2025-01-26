#pragma once

#include <cstddef>
namespace kv {
class Node {
public:
  explicit Node() = default;

private:
  bool is_leaf_;
  std::byte *key_;
};
} // namespace kv
