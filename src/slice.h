#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>

namespace kv {

// Non owning view of byte data
class Slice {
public:
  constexpr Slice() noexcept = default;

  // construct from raw data and size
  [[nodiscard]] constexpr Slice(const std::byte *data, size_t size) noexcept
      : data_(data, size) {}

  [[nodiscard]] Slice(const std::string &str) noexcept
      : data_(reinterpret_cast<const std::byte *>(str.data()), str.size()) {}

  [[nodiscard]] Slice(const char *str) noexcept
      : data_(reinterpret_cast<const std::byte *>(str), std::strlen(str)) {}

  [[nodiscard]] constexpr const std::byte *Data() const noexcept {
    return data_.data();
  }

  [[nodiscard]] constexpr size_t Size() const noexcept { return data_.size(); }

  [[nodiscard]] constexpr std::byte operator[](size_t index) const noexcept {
    assert(index < Size());
    return data_[index];
  }

  // return a copy of data as a string
  [[nodiscard]] std::string ToString() const {
    // LOG_DEBUG("{}", reinterpret_cast<const void *>(Data()));
    return std::string(reinterpret_cast<const char *>(Data()), Size());
  }

  [[nodiscard]] int Compare(const Slice &other) const noexcept {
    auto min_len = std::min(Size(), other.Size());
    auto cmp = std::memcmp(Data(), other.Data(), min_len);
    if (cmp == 0) {
      if (Size() < other.Size())
        return -1;
      if (Size() > other.Size())
        return 1;
    }
    return cmp;
  }

  [[nodiscard]] friend constexpr bool operator==(const Slice &lhs,
                                                 const Slice &rhs) noexcept {
    return lhs.Size() == rhs.Size() &&
           std::memcmp(lhs.Data(), rhs.Data(), lhs.Size()) == 0;
  }

  [[nodiscard]] friend constexpr bool operator!=(const Slice &lhs,
                                                 const Slice &rhs) noexcept {
    return !(lhs == rhs);
  }

private:
  std::span<const std::byte> data_;
};

} // namespace kv
