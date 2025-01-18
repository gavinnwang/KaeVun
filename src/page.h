#pragma once

#include "log.h"
#include "type.h"
#include <cstddef>
#include <cstdint>
namespace kv {

constexpr uint64_t VERSION_NUMBER = 1;

constexpr uint32_t MAGIC = 0xED0CDAED;

enum class PageFlag : uint32_t {
  None = 0x00,
  BranchPage = 0x01,
  LeafPage = 0x02,
  MetaPage = 0x04,
  BucketPage = 0x08,
  FreelistPage = 0x10
};

class Meta {
private:
  uint64_t magic_;
  uint64_t version_;
  uint32_t pageSize_;
  Pgid freelist_;
  Pgid buckets_;
  Pgid pgid_;
  Txid txid_;
  uint64_t checksum_;

public:
  void SetMagic(uint64_t magic) noexcept { magic_ = magic; }
  void SetVersion(uint64_t ver) noexcept { version_ = ver; }
  void SetPageSize(uint32_t size) noexcept { pageSize_ = size; }
  void SetFreelist(Pgid f) noexcept { freelist_ = f; }
  void SetBuckets(Pgid b) noexcept { buckets_ = b; }
  void SetChecksum(uint64_t csum) noexcept { checksum_ = csum; }
  void SetPgid(Pgid id) noexcept { pgid_ = id; }
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

  [[nodiscard]] bool Validate() {
    LOG_INFO("Validating magic: {:02x} == {:02x} version: {:02x} == {:02x} "
             "checksum: {:02x} == {:02x}",
             magic_, MAGIC, version_, VERSION_NUMBER, checksum_, Sum64());
    return magic_ == MAGIC && version_ == VERSION_NUMBER &&
           checksum_ == Sum64();
  }
};

class Page {
public:
  Page() = default;

  void SetId(Pgid id) noexcept { id_ = id; }
  void SetFlags(PageFlag flags) noexcept {
    flags_ = static_cast<uint32_t>(flags);
  }
  void SetCount(uint32_t count) noexcept { count_ = count; }

  [[nodiscard]] Meta &Meta() noexcept {
    auto base = reinterpret_cast<std::byte *>(this);
    return *reinterpret_cast<class Meta *>(base + sizeof(Page));
  }
  [[nodiscard]] uint32_t Count() const noexcept { return count_; }
  [[nodiscard]] uint32_t Flags() const noexcept { return flags_; }

  template <typename T> [[nodiscard]] T *GetDataAs() noexcept {
    return reinterpret_cast<T>(Data());
  }

private:
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
  uint32_t count_;
};

} // namespace kv
