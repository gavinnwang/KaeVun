#pragma once

#include "page.h"
#include <cassert>
#include <expected>

namespace kv {

class ShadowPage {
public:
  // Constructor of ShadowPage only accepts rvalue
  ShadowPage(PageBuffer &&buf) noexcept : buffer_(std::move(buf)) {}
  // prevent copying
  ShadowPage(const ShadowPage &) = delete;
  ShadowPage &operator=(const ShadowPage &) = delete;

  // allow moving
  ShadowPage(ShadowPage &&) noexcept = default;
  ShadowPage &operator=(ShadowPage &&) noexcept = default;

  [[nodiscard]] Page &Get() noexcept { return buffer_.GetPage(0); }

private:
  PageBuffer buffer_; // owns the memory backing `page_`
};

} // namespace kv
