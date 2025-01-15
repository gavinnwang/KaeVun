#pragma once

#include "log.h"
#include <cassert>
#include <span>
#include <sys/mman.h>
#include <unistd.h>

namespace kv {

class MmapDataHandle {
public:
  explicit MmapDataHandle() noexcept {}
  explicit MmapDataHandle(void *mmap_ptr, size_t size) noexcept
      : mmap_ptr_(static_cast<std::byte *>(mmap_ptr)), size_(size) {
    assert(mmap_ptr != nullptr && mmap_ptr != MAP_FAILED);
    data_ = std::span<std::byte>(mmap_ptr_, size_);
  }

  MmapDataHandle(const MmapDataHandle &) = delete;
  MmapDataHandle &operator=(const MmapDataHandle &) = delete;

  MmapDataHandle(MmapDataHandle &&other) noexcept
      : mmap_ptr_(other.mmap_ptr_), data_(other.data_), size_(other.size_) {
    other.mmap_ptr_ = nullptr;
    other.data_ = std::span<std::byte>();
    other.size_ = 0;
  }

  MmapDataHandle &operator=(MmapDataHandle &&other) noexcept {
    if (this != &other) {
      Unmap(); // unmap before taking on new ownership

      mmap_ptr_ = other.mmap_ptr_;
      data_ = other.data_;
      size_ = other.size_;

      other.mmap_ptr_ = nullptr;
      other.data_ = std::span<std::byte>();
      other.size_ = 0;
    }
    return *this;
  }

  ~MmapDataHandle() { Unmap(); }

  std::span<std::byte> Data() const noexcept { return data_; }

  void Reset(void *new_mmap_ptr = nullptr, size_t new_size = 0) noexcept {
    Unmap();

    if (new_mmap_ptr && new_mmap_ptr != MAP_FAILED) {
      mmap_ptr_ = static_cast<std::byte *>(new_mmap_ptr);
      size_ = new_size;
      data_ = std::span<std::byte>(mmap_ptr_, size_);
    } else {
      mmap_ptr_ = nullptr;
      size_ = 0;
      data_ = std::span<std::byte>();
    }
  }

  void Unmap() noexcept {
    if (mmap_ptr_ && mmap_ptr_ != MAP_FAILED) {
      LOG_INFO("Releasing mmap data");
      munmap(mmap_ptr_, size_);
      mmap_ptr_ = nullptr;
      data_ = std::span<std::byte>();
      size_ = 0;
    }
  }

private:
  std::byte *mmap_ptr_{nullptr};
  std::span<std::byte> data_;
  size_t size_{0};
};

} // namespace kv
