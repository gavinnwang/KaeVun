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

  [[nodiscard]] constexpr const std::byte *data() const noexcept {
    return data_.data();
  }

  [[nodiscard]] constexpr size_t size() const noexcept { return data_.size(); }

  [[nodiscard]] constexpr bool empty() const noexcept { return data_.empty(); }

  [[nodiscard]] constexpr std::byte operator[](size_t index) const noexcept {
    assert(index < size());
    return data_[index];
  }

  // return a copy of data as a string
  [[nodiscard]] std::string ToString() const {
    return std::string(reinterpret_cast<const char *>(data()), size());
  }

  constexpr void clear() noexcept { data_ = {}; }

  [[nodiscard]] int compare(const Slice &other) const noexcept {
    auto min_len = std::min(size(), other.size());
    auto cmp = std::memcmp(data(), other.data(), min_len);
    if (cmp == 0) {
      if (size() < other.size())
        return -1;
      if (size() > other.size())
        return 1;
    }
    return cmp;
  }

  [[nodiscard]] friend constexpr bool operator==(const Slice &lhs,
                                                 const Slice &rhs) noexcept {
    return lhs.size() == rhs.size() &&
           std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
  }

  [[nodiscard]] friend constexpr bool operator!=(const Slice &lhs,
                                                 const Slice &rhs) noexcept {
    return !(lhs == rhs);
  }

private:
  std::span<const std::byte> data_;
};

} // namespace kv
