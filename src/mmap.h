#pragma once

#include "log.h"
#include <cassert>
#include <cstdint>
#include <sys/mman.h>
#include <unistd.h>

namespace kv {

class MmapDataHandle {
public:
  explicit MmapDataHandle() noexcept {}
  explicit MmapDataHandle(void *mmap_ptr, size_t size) noexcept
      : mmap_ptr_(static_cast<std::byte *>(mmap_ptr)), size_(size) {
    assert(mmap_ptr != nullptr && mmap_ptr != MAP_FAILED);
  }

  MmapDataHandle(const MmapDataHandle &) = delete;
  MmapDataHandle &operator=(const MmapDataHandle &) = delete;

  MmapDataHandle(MmapDataHandle &&other) noexcept
      : mmap_ptr_(other.mmap_ptr_), size_(other.size_) {
    other.mmap_ptr_ = nullptr;
    other.size_ = 0;
  }

  MmapDataHandle &operator=(MmapDataHandle &&other) noexcept {
    if (this != &other) {
      Unmap(); // unmap before taking on new ownership

      mmap_ptr_ = other.mmap_ptr_;
      size_ = other.size_;

      other.mmap_ptr_ = nullptr;
      other.size_ = 0;
    }
    return *this;
  }

  ~MmapDataHandle() { Unmap(); }

  [[nodiscard]] void *MmapPtr() const noexcept { return mmap_ptr_; }
  [[nodiscard]] uint64_t Size() const noexcept { return size_; }

  void Reset() noexcept {
    Unmap();
    mmap_ptr_ = nullptr;
    size_ = 0;
  }

  void Unmap() noexcept {
    if (mmap_ptr_ && mmap_ptr_ != MAP_FAILED) {
      LOG_INFO("Releasing mmap data");
      munmap(mmap_ptr_, size_);
      mmap_ptr_ = nullptr;
      size_ = 0;
    }
  }

private:
  void *mmap_ptr_{nullptr};
  uint64_t size_{0};
};

} // namespace kv
