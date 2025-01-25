#pragma once

#include "error.h"
#include "log.h"
#include "type.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <fstream>
#include <optional>
#include <vector>
namespace kv {

constexpr uint64_t VERSION_NUMBER = 1;

constexpr uint32_t MAGIC = 0xED0CDAED;

constexpr Pgid META_PAGE_ID = 0;
constexpr Pgid FREELIST_PAGE_ID = 1;
constexpr Pgid BUCKET_PAGE_ID = 2;

enum class PageFlag : uint32_t {
  None = 0x00,
  BranchPage = 0x01,
  LeafPage = 0x02,
  MetaPage = 0x04,
  BucketPage = 0x08,
  FreelistPage = 0x10
};

class Meta {
public:
  // get the high watermark page id
  [[nodiscard]] Pgid GetWatermark() const noexcept { return watermark_; }
  void SetMagic(uint64_t magic) noexcept { magic_ = magic; }
  void SetVersion(uint64_t ver) noexcept { version_ = ver; }
  void SetPageSize(uint32_t size) noexcept { page_size_ = size; }
  void SetFreelist(Pgid f) noexcept { freelist_ = f; }
  void SetBuckets(Pgid b) noexcept { buckets_ = b; }
  void SetChecksum(uint64_t csum) noexcept { checksum_ = csum; }
  void SetWatermark(Pgid id) noexcept { watermark_ = id; }
  void SetTxid(Txid tx) noexcept { txid_ = tx; }

  // calculate the Fowler–Noll–Vo hash of meta
  [[nodiscard]] uint64_t Sum64() const noexcept {
    constexpr uint64_t FNV_OFFSET_BASIS_64 = 14695981039346656037ULL;
    constexpr uint64_t FNV_PRIME_64 = 1099511628211;

    const auto ptr = reinterpret_cast<const uint8_t *>(this);
    constexpr std::size_t length = offsetof(Meta, checksum_);

    uint64_t hash = FNV_OFFSET_BASIS_64;
    for (std::size_t i = 0; i < length; ++i) {
      hash ^= ptr[i];
      hash *= FNV_PRIME_64;
    }

    return hash;
  }

  [[nodiscard]] std::optional<Error> Validate() {
    LOG_DEBUG("Validating magic: {:02x} == {:02x} version: {:02x} == {:02x} "
              "checksum: {:02x} == {:02x}",
              magic_, MAGIC, version_, VERSION_NUMBER, checksum_, Sum64());
    if (magic_ == MAGIC && version_ == VERSION_NUMBER && checksum_ == Sum64()) {
      return std::nullopt;
    }
    return Error{"Meta validation failed"};
  }

private:
  uint64_t magic_;
  uint64_t version_;
  uint32_t page_size_;
  Pgid freelist_;
  Pgid buckets_;
  Pgid watermark_; // high watermark page id
  Txid txid_;
  uint64_t checksum_;
};

class Page {
public:
  Page() = default;

  void SetId(Pgid id) noexcept { id_ = id; }
  void SetFlags(PageFlag flags) noexcept {
    flags_ = static_cast<uint32_t>(flags);
  }
  void SetCount(uint32_t count) noexcept { count_ = count; }
  void SetOverflow(uint32_t overflow) noexcept { overflow_ = overflow; }

  [[nodiscard]] Meta &Meta() noexcept {
    auto base = reinterpret_cast<std::byte *>(this);
    return *reinterpret_cast<class Meta *>(base + sizeof(Page));
  }

  [[nodiscard]] uint32_t Count() const noexcept { return count_; }
  [[nodiscard]] uint32_t Flags() const noexcept { return flags_; }
  [[nodiscard]] Pgid Id() const noexcept { return id_; }
  [[nodiscard]] uint32_t Overflow() const noexcept { return overflow_; }

  template <typename T> [[nodiscard]] T *GetDataAs() noexcept {
    return reinterpret_cast<T *>(Data());
  }

  // write access to the data section
  [[nodiscard]] void *Data() noexcept {
    return reinterpret_cast<void *>(reinterpret_cast<std::byte *>(this) +
                                    sizeof(Page));
  }

  // read access
  [[nodiscard]] const void *Data() const noexcept {
    return reinterpret_cast<const void *>(
        reinterpret_cast<const std::byte *>(this) + sizeof(Page));
  }

private:
  Pgid id_;
  uint32_t flags_;
  uint32_t overflow_;
  uint32_t count_;
};

// an interface that defines abstract methods for accessing pages
class PageHandler {
public:
  virtual ~PageHandler() noexcept = default;
  [[nodiscard]] virtual Page &GetPage(Pgid pgid) noexcept = 0;
};

// temporary in memory page
class PageBuffer final : public PageHandler {
public:
  // construct an empty page buffer for use
  PageBuffer(uint32_t size, uint32_t page_size) noexcept
      : size_(size), page_size_(page_size), buffer_(size * page_size) {}

  ~PageBuffer() noexcept = default;

  [[nodiscard]] std::vector<std::byte> &GetBuffer() noexcept { return buffer_; }

  // get a page from the buffer
  [[nodiscard]] Page &GetPage(Pgid pgid) noexcept override {
    assert(pgid < size_);
    return *reinterpret_cast<Page *>(buffer_.data() + pgid * page_size_);
  }

  [[nodiscard]] std::vector<std::byte> &GetData() noexcept { return buffer_; }

private:
  uint32_t size_;
  uint32_t page_size_;
  std::vector<std::byte> buffer_;
};

} // namespace kv
