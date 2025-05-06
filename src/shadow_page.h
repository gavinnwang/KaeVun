#pragma once

#include "page.h"
#include <cassert>
#include <expected>

namespace kv {

class ShadowPage {
public:
  ShadowPage(PageBuffer buf) noexcept : buffer_(std::move(buf)) {}

  [[nodiscard]] Page &Get() noexcept { return buffer_.GetPage(0); }

private:
  PageBuffer buffer_; // owns the memory backing `page_`
};

} // namespace kv
