#pragma once

#include "error.h"
#include "log.h"
#include "slice.h"
#include "type.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>

namespace kv {

constexpr std::size_t VERSION_NUMBER = 1;
constexpr std::size_t MAGIC = 0xED0CDAED;

constexpr Pgid META_PAGE_ID = 0;
constexpr Pgid FREELIST_PAGE_ID = 1;
constexpr Pgid BUCKET_PAGE_ID = 2;
constexpr std::size_t MIN_KEY_PER_PAGE = 2;

enum class PageFlag : std::size_t {
  None = 0x00,
  BranchPage = 0x01,
  LeafPage = 0x02,
  MetaPage = 0x04,
  BucketPage = 0x08,
  FreelistPage = 0x10
};

template <typename T>
concept IsValidPage =
    std::same_as<T, class LeafPage> || std::same_as<T, class BranchPage>;

class Page {
protected:
  Pgid pgid_;
  std::size_t flags_;
  std::size_t overflow_;
  std::size_t count_;
  std::size_t magic_;

public:
  Page() = default;
  Page(const Page &other) = delete;
  Page &operator=(const Page &other) = delete;
  Page(Page &&other) = delete;
  Page &operator=(Page &&other) noexcept = delete;
  ~Page() = default;
  void AssertMagic() const noexcept { assert(magic_ == MAGIC); }
  void SetMagic() noexcept { magic_ = MAGIC; }

  void SetId(Pgid id) noexcept { pgid_ = id; }
  void SetFlags(PageFlag flags) noexcept {
    flags_ = static_cast<std::size_t>(flags);
  }
  void SetCount(std::size_t count) noexcept { count_ = count; }
  void SetOverflow(std::size_t overflow) noexcept { overflow_ = overflow; }

  [[nodiscard]] std::size_t Count() const noexcept { return count_; }
  [[nodiscard]] std::size_t Flags() const noexcept { return flags_; }
  [[nodiscard]] Pgid Id() const noexcept { return pgid_; }
  [[nodiscard]] std::size_t Overflow() const noexcept { return overflow_; }

  template <typename T> [[nodiscard]] T *GetDataAs() noexcept {
    return reinterpret_cast<T *>(Data());
  }

  [[nodiscard]] void *Data() noexcept {
    AssertMagic();
    return reinterpret_cast<void *>(reinterpret_cast<std::byte *>(this) +
                                    sizeof(Page));
  }

  [[nodiscard]] const void *Data() const noexcept {
    AssertMagic();
    return reinterpret_cast<const void *>(
        reinterpret_cast<const std::byte *>(this) + sizeof(Page));
  }

  template <IsValidPage T> [[nodiscard]] T &AsPage() noexcept {
    AssertMagic();
    return *reinterpret_cast<T *>(this);
  }
};
constexpr std::size_t PAGE_HEADER_SIZE = sizeof(Page);

struct LeafElement {
  std::size_t offset_; // the offset between the start of page and the start of
                       // the key address
  std::size_t ksize_;
  std::size_t vsize_;
};

struct BranchElement {
  std::size_t offset_;
  std::size_t ksize_;
  Pgid pgid_;
};

constexpr std::size_t BRANCH_ELEMENT_SIZE = sizeof(BranchElement);
constexpr std::size_t LEAF_ELEMENT_SIZE = sizeof(LeafElement);

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

  [[nodiscard]] T &GetElement(std::size_t i) noexcept { return elements_[i]; }

  void SetElement(T e, std::size_t i) noexcept { elements_[i] = e; }

  [[nodiscard]] Slice GetKey(std::size_t i) const noexcept {
    return {reinterpret_cast<const std::byte *>(this) + elements_[i].offset_,
            elements_[i].ksize_};
  }

protected:
  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(std::is_standard_layout_v<T>);
  T elements_[0];
};

class LeafPage final : public ElementPage<LeafElement> {
public:
  LeafPage() = delete;
  LeafPage(const LeafPage &) = delete;
  LeafPage &operator=(const LeafPage &) = delete;
  LeafPage(LeafPage &&) = delete;
  LeafPage &operator=(LeafPage &&) = delete;
  ~LeafPage() = delete;
  [[nodiscard]] Slice GetVal(std::size_t i) const noexcept {
    return {reinterpret_cast<const std::byte *>(this) + elements_[i].offset_ +
                elements_[i].ksize_,
            elements_[i].vsize_};
  }
  [[nodiscard]] int FindLastLessThan(const Slice &key) const noexcept {
    for (int i = static_cast<int>(Count()) - 1; i >= 0; --i) {
      if (GetKey(i) < key) {
        return i;
      }
    }
    return -1; // key is less than all keys
  }

  [[nodiscard]] std::string ToString() const noexcept {
    std::string result = "LeafPage[";
    for (std::size_t i = 0; i < Count(); ++i) {
      result += fmt::format("{{key: '{}', val: '{}'}}", GetKey(i).ToString(),
                            GetVal(i).ToString());
      if (i != Count() - 1) {
        result += ", ";
      }
    }
    result += "]";
    return result;
  }
  // New verbose inspector
  [[nodiscard]] std::string ToStringVerbose() const noexcept {
    std::string result =
        fmt::format("LeafPage(pgid: {}, count: {}) [\n", this->pgid_, Count());
    for (std::size_t i = 0; i < Count(); ++i) {
      const auto &e = elements_[i];
      result +=
          fmt::format("  {{ index: {}, offset: {}, ksize: {}, vsize: {} }}\n",
                      i, e.offset_, e.ksize_, e.vsize_);
    }
    result += "]";
    return result;
  }
};

class BranchPage final : public ElementPage<BranchElement> {
public:
  ~BranchPage() = delete;
  [[nodiscard]] Pgid GetPgid(std::size_t i) noexcept {
    return elements_[i].pgid_;
  }

  [[nodiscard]] std::pair<std::size_t, bool>
  FindFirstGreaterOrEqualTo(const Slice &key) const noexcept {
    for (int i = 0; i < static_cast<int>(Count()); ++i) {
      Slice cur_key = GetKey(i);
      if (!(cur_key < key)) { // i.e., cur_key >= key
        bool exact = (cur_key == key);
        return {i, exact};
      }
    }
    // If all keys are less than the input key, return Count() as insertion
    // point
    return {static_cast<int>(Count()), false};
  }
  [[nodiscard]] std::string ToString() const noexcept {
    std::string result = "BranchPage[";
    for (std::size_t i = 0; i < Count(); ++i) {
      result += fmt::format("{{key: '{}', pgid: {}}}", GetKey(i).ToString(),
                            elements_[i].pgid_);
      if (i != Count() - 1) {
        result += ", ";
      }
    }
    result += "]";
    return result;
  }
};

class PageBuffer final {
public:
  PageBuffer(std::size_t size, std::size_t page_size) noexcept
      : size_(size), page_size_(page_size), total_bytes_(size * page_size),
        buffer_(std::make_unique<std::byte[]>(total_bytes_)) {
    for (std::size_t i = 0; i < size; i++) {
      GetPage(i).SetMagic();
    }
  }

  // Move constructor
  PageBuffer(PageBuffer &&other) noexcept = default;
  // Move assignment
  PageBuffer &operator=(PageBuffer &&other) noexcept = default;

  // Delete copy
  PageBuffer(const PageBuffer &) = delete;
  PageBuffer &operator=(const PageBuffer &) = delete;

  ~PageBuffer() noexcept = default;

  [[nodiscard]] std::span<std::byte> GetBuffer() noexcept {
    return std::span<std::byte>(buffer_.get(), total_bytes_);
  }

  [[nodiscard]] std::span<std::byte> GetPageSpan(Pgid pgid) noexcept {
    assert(pgid < size_);
    return std::span<std::byte>(buffer_.get() + pgid * page_size_, page_size_);
  }

  [[nodiscard]] Page &GetPage(Pgid pgid) noexcept {
    assert(pgid < size_);
    return *reinterpret_cast<Page *>(buffer_.get() + pgid * page_size_);
  }

private:
  std::size_t size_;
  std::size_t page_size_;
  std::size_t total_bytes_;
  std::unique_ptr<std::byte[]> buffer_;
};
;

class Meta {

private:
  std::size_t magic_;
  std::size_t version_;
  std::size_t page_size_;
  Pgid freelist_;
  Pgid buckets_;
  Pgid watermark_;
  Txid txid_;
  std::size_t checksum_;

public:
  [[nodiscard]] Pgid GetWatermark() const noexcept { return watermark_; }
  [[nodiscard]] Pgid GetBuckets() const noexcept { return buckets_; }
  void SetMagic(std::size_t magic) noexcept { magic_ = magic; }
  void SetVersion(std::size_t ver) noexcept { version_ = ver; }
  void SetPageSize(std::size_t size) noexcept { page_size_ = size; }
  void SetFreelist(Pgid f) noexcept { freelist_ = f; }
  void SetBuckets(Pgid b) noexcept { buckets_ = b; }
  void SetChecksum(std::size_t csum) noexcept { checksum_ = csum; }
  void SetWatermark(Pgid id) noexcept { watermark_ = id; }
  void SetTxid(Txid id) noexcept { txid_ = id; }
  void IncrementTxid() noexcept { txid_++; }

  [[nodiscard]] std::string ToString() const noexcept {
    return fmt::format("Meta(magic: {:#x}, version: {}, page_size: {}, "
                       "freelist: {}, buckets: {}, "
                       "watermark: {}, txid: {}, checksum: {:#x})",
                       magic_, version_, page_size_, freelist_, buckets_,
                       watermark_, txid_, checksum_);
  }

  [[nodiscard]] std::size_t Sum64() const noexcept {
    constexpr std::size_t FNV_OFFSET_BASIS_64 = 14695981039346656037ULL;
    constexpr std::size_t FNV_PRIME_64 = 1099511628211;

    const auto *ptr = reinterpret_cast<const uint8_t *>(this);
    constexpr std::size_t length = offsetof(Meta, checksum_);

    std::size_t hash = FNV_OFFSET_BASIS_64;
    for (std::size_t i = 0; i < length; ++i) {
      hash ^= ptr[i];
      hash *= FNV_PRIME_64;
    }
    return hash;
  }

  void Write(Page &p) noexcept {
    // Page id is either going to be 0 or 1 which we can determine by the
    // transaction
    p.SetId(META_PAGE_ID);
    LOG_INFO("tx meta page written to {}", p.Id());
    p.SetFlags(PageFlag::MetaPage);

    // Compute and store the checksum before copying data to page
    checksum_ = Sum64();

    // Write this Meta object into the page memory
    auto *page_meta = p.GetDataAs<Meta>();
    *page_meta = *this;
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
};
} // namespace kv
