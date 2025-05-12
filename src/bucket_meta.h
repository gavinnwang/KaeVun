#pragma once

#include "type.h"
namespace kv {

class BucketMeta {
public:
  explicit BucketMeta(Pgid root) : root_(root) {}

  [[nodiscard]] Pgid Root() const noexcept { return root_; }
  void SetRoot(Pgid id) noexcept { root_ = id; }

private:
  Pgid root_;
};
} // namespace kv
