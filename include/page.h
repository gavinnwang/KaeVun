#pragma once

#include "type.h"
#include <cstddef>
#include <cstdint>
namespace kv {

static constexpr uint32_t MAGIC = 0xED0CDAED;

struct Page {
  Pgid id_;
  uint32_t flags_;

  void setId(Pgid id) noexcept { id_ = id; }
  void setFlags(uint32_t flags) noexcept { flags_ = flags; }

  struct Meta *meta() noexcept {
    auto base = reinterpret_cast<std::byte *>(this);
    return reinterpret_cast<struct Meta *>(base + sizeof(Page));
  }
};

struct Meta {
  uint64_t magic_;
  uint64_t version_;
  uint32_t pageSize_;
  Pgid freelist_;
  Pgid pgid_;
  Txid txid_;
  uint64_t checksum_;

  void setMagic(uint64_t magic) noexcept { magic_ = magic; }
  void setVersion(uint64_t ver) noexcept { version_ = ver; }
  void setPageSize(uint32_t size) noexcept { pageSize_ = size; }
  void setFreelist(Pgid f) noexcept { freelist_ = f; }
  void setChecksum(uint64_t csum) noexcept { checksum_ = csum; }
  void setPgid(Pgid id) noexcept { pgid_ = id; }
  void setTxid(Txid tx) noexcept { txid_ = tx; }

  // calculate the Fowler–Noll–Vo hash of meta
  uint64_t sum64() const noexcept {
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
};

} // namespace kv
