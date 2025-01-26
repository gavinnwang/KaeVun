#pragma once

#include "error.h"
#include "log.h"
#include "os.h"
#include <cassert>
#include <cstdint>
#include <mutex>
#include <optional>
#include <sys/mman.h>
#include <unistd.h>

namespace kv {

// RAII wrapper for a mmap void * ptr also maintains the mmap size
class MmapDataHandle {
public:
  MmapDataHandle() = default;

  explicit MmapDataHandle(uint64_t page_size) noexcept
      : page_size_(page_size) {}

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

  [[nodiscard]] std::optional<Error> Mmap(std::filesystem::path path, int fd,
                                          uint64_t min_sz) noexcept {
    std::lock_guard mmaplock(mmaplock_);
    auto file_sz_or_err = OS::FileSize(path);
    if (!file_sz_or_err) {
      return file_sz_or_err.error();
    }
    auto file_sz = file_sz_or_err.value();
    auto mmap_sz = MmapSize(fmax(min_sz, file_sz));
    LOG_INFO("Mmaping size {}", mmap_sz);

    // deference all existing mmap reference which are the nodes

    Unmap(); // unmap previous before mmpaing

    void *b = mmap(nullptr, mmap_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (b == MAP_FAILED) {
      return Error("Failed to mmap");
    }

    size_ = mmap_sz;
    mmap_ptr_ = b;

    int result = madvise(mmap_ptr_, mmap_sz, MADV_RANDOM);
    if (result == -1) {
      return Error("Mmap advise failed");
    }

    LOG_INFO("Successfully created mmap memory of size {}", size_);

    return std::nullopt;
  }

  [[nodiscard]] uint64_t MmapSize(uint64_t request_sz) const noexcept {
    uint64_t step = 1 << 30; // 1GB
    if (request_sz <= step) {
      for (uint32_t i = 15; i <= 30; ++i) {
        if (request_sz <= 1 << i) {
          return 1 << i;
        }
      }
      return step;
    } else {
      uint64_t sz = request_sz;
      uint64_t remainder = request_sz % step;
      if (step > 0)
        sz += step - remainder;

      // ensure sz is a multiple of page size
      if (sz % page_size_ != 0) {
        sz = ((sz / page_size_) + 1) * page_size_;
      }
      return sz;
    }
  }

  [[nodiscard]] void *MmapPtr() const noexcept { return mmap_ptr_; }
  [[nodiscard]] uint64_t Size() const noexcept { return size_; }
  [[nodiscard]] bool Valid() const noexcept { return mmap_ptr_ != nullptr; }

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

  // std::optional<Error> Grow(uint64_t new_sz) noexcept {
  //   if (!Valid())
  //     return Error{"Invalid mmap ptr"};
  //     assert(new_sz > size_);
  //     // void * new_ptr = mmap()
  // }

private:
  uint32_t page_size_{OS::DEFAULT_PAGE_SIZE};
  void *mmap_ptr_{nullptr};
  uint64_t size_{0};
  // mutex to protect mmap access
  std::mutex mmaplock_;
};

} // namespace kv
