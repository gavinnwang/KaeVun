#pragma once

#include "type.h"
namespace kv {

class BucketMeta {
public:
  BucketMeta(Pgid root) : root_(root) {}

  [[nodiscard]] Pgid Root() const noexcept { return root_; }

private:
  Pgid root_;
};
} // namespace kv
