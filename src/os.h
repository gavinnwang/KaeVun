#pragma once

#include "error.h"
#include "log.h"
#include <cstdint>
#include <expected>
#include <filesystem>
#include <system_error>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace kv {
class OS {
public:
  static constexpr uint32_t DEFAULT_PAGE_SIZE = 4096;
  static uint32_t OSPageSize() noexcept {
    static const uint32_t page_size = []() noexcept -> uint32_t {
#ifdef _WIN32
      SYSTEM_INFO sysInfo;
      GetSystemInfo(&sysInfo);
      return static_cast<uint32_t>(sysInfo.dwPageSize);
#else
      long sz = ::sysconf(_SC_PAGE_SIZE);
      if (sz < 0) {
        // if error, fall back to default page size
        return DEFAULT_PAGE_SIZE;
      }
      return static_cast<uint32_t>(sz);
#endif
    }();

    LOG_INFO("OS page size: {}", page_size);

    return page_size;
  }

  static std::expected<uint64_t, Error>
  FileSize(const std::filesystem::path &path) noexcept {
    std::error_code ec;
    auto file_sz = std::filesystem::file_size(path, ec);
    LOG_INFO("Current db file size {}", file_sz);
    if (ec) {
      return std::unexpected{Error{"Failed to check for file size"}};
    }
    return file_sz;
  }
};

} // namespace kv
