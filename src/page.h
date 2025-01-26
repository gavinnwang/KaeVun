#pragma once

#include "error.h"
#include "log.h"
#include "slice.h"
#include "type.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <type_traits>
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
  [[nodiscard]] Pgid GetWatermark() const noexcept { return watermark_; }
  void SetMagic(uint64_t magic) noexcept { magic_ = magic; }
  void SetVersion(uint64_t ver) noexcept { version_ = ver; }
  void SetPageSize(uint32_t size) noexcept { page_size_ = size; }
  void SetFreelist(Pgid f) noexcept { freelist_ = f; }
  void SetBuckets(Pgid b) noexcept { buckets_ = b; }
  void SetChecksum(uint64_t csum) noexcept { checksum_ = csum; }
  void SetWatermark(Pgid id) noexcept { watermark_ = id; }
  void SetTxid(Txid tx) noexcept { txid_ = tx; }

  [[nodiscard]] uint64_t Sum64() const noexcept {
    constexpr uint64_t FNV_OFFSET_BASIS_64 = 14695981039346656037ULL;
    constexpr uint64_t FNV_PRIME_64 = 1099511628211;

    const auto *ptr = reinterpret_cast<const uint8_t *>(this);
    constexpr std::size_t length = offsetof(Meta, checksum_);

    uint64_t hash = FNV_OFFSET_BASIS_64;
    for (std::size_t i = 0; i < length; ++i) {
      hash ^= ptr[i];
      hash *= FNV_PRIME_64;
    }
    return hash;
  }

  [[nodiscard]] std::optional<Error> Validate() const noexcept {
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
  Pgid watermark_;
  Txid txid_;
  uint64_t checksum_;
};

template <typename T>
concept IsValidPage =
    std::same_as<T, class LeafPage> || std::same_as<T, class BranchPage>;

class Page {
public:
  Page() = default;

  Page(const Page &other) = delete;
  Page &operator=(const Page &other) = delete;
  Page(Page &&other) = delete;
  Page &operator=(Page &&other) noexcept = delete;
  ~Page() = default;

  void SetId(Pgid id) noexcept { pgid_ = id; }
  void SetFlags(PageFlag flags) noexcept {
    flags_ = static_cast<uint32_t>(flags);
  }
  void SetCount(uint32_t count) noexcept { count_ = count; }
  void SetOverflow(uint32_t overflow) noexcept { overflow_ = overflow; }

  [[nodiscard]] uint32_t Count() const noexcept { return count_; }
  [[nodiscard]] uint32_t Flags() const noexcept { return flags_; }
  [[nodiscard]] Pgid Id() const noexcept { return pgid_; }
  [[nodiscard]] uint32_t Overflow() const noexcept { return overflow_; }

  template <typename T> [[nodiscard]] T *GetDataAs() noexcept {
    return reinterpret_cast<T *>(Data());
  }

  [[nodiscard]] void *Data() noexcept {
    return reinterpret_cast<void *>(reinterpret_cast<std::byte *>(this) +
                                    sizeof(Page));
  }

  [[nodiscard]] const void *Data() const noexcept {
    return reinterpret_cast<const void *>(
        reinterpret_cast<const std::byte *>(this) + sizeof(Page));
  }

  template <IsValidPage T> [[nodiscard]] T &AsPage() noexcept {
    return *reinterpret_cast<T *>(this);
  }

protected:
  Pgid pgid_;
  uint32_t flags_;
  uint32_t overflow_;
  uint32_t count_;
};

struct LeafElement {
  uint32_t offset_; // the offset between the start of page and the start of
                    // the key address
  uint32_t ksize_;
  uint32_t vsize_;
};

struct BranchElement {
  uint32_t offset_;
  uint32_t ksize_;
  Pgid pgid_;
};

template <typename T>
concept IsValidPageElement =
    std::same_as<T, LeafElement> || std::same_as<T, BranchElement>;

template <IsValidPageElement T> class ElementPage : public Page {
public:
  ElementPage(const ElementPage &other) = delete;
  ElementPage &operator=(const ElementPage &other) = delete;
  ElementPage(ElementPage &&other) = delete;
  ElementPage &operator=(ElementPage &&other) noexcept = delete;
  ~ElementPage() = delete;

  [[nodiscard]] std::span<T> GetElements() noexcept {
    return {&elements_[0], count_};
  }

  [[nodiscard]] T &GetElement(uint32_t i) noexcept { return elements_[i]; }

  void SetElement(T e, uint32_t i) noexcept { elements_[i] = e; }

  [[nodiscard]] Slice GetKey(uint32_t i) noexcept {
    return {reinterpret_cast<std::byte *>(this) + elements_[i].offset_,
            elements_[i].ksize_};
  }

protected:
  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(std::is_standard_layout_v<T>);
  T elements_[0];
};

class LeafPage final : public ElementPage<LeafElement> {
public:
  ~LeafPage() = delete;
  [[nodiscard]] Slice GetVal(uint32_t i) noexcept {
    return {reinterpret_cast<std::byte *>(this) + elements_[i].offset_ +
                elements_[i].ksize_,
            elements_[i].vsize_};
  }
};

class BranchPage final : public ElementPage<BranchElement> {
public:
  ~BranchPage() = delete;
  [[nodiscard]] Pgid GetPgid(uint32_t i) noexcept { return elements_[i].pgid_; }
};

class PageBuffer final {
public:
  PageBuffer(uint32_t size, uint32_t page_size) noexcept
      : size_(size), page_size_(page_size), buffer_(size * page_size) {}

  ~PageBuffer() noexcept = default;

  [[nodiscard]] std::vector<std::byte> &GetBuffer() noexcept { return buffer_; }

  [[nodiscard]] Page &GetPage(Pgid pgid) noexcept {
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
