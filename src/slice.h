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

  [[nodiscard]] std::strong_ordering
  operator<=>(const Slice &other) const noexcept {
    const size_t min_len = std::min(Size(), other.Size());
    const int cmp = std::memcmp(Data(), other.Data(), min_len);

    if (cmp < 0)
      return std::strong_ordering::less;
    if (cmp > 0)
      return std::strong_ordering::greater;

    // If equal up to min_len, shorter slice is less
    return Size() <=> other.Size();
  }

  [[nodiscard]] bool operator==(const Slice &other) const noexcept {
    return (*this <=> other) == std::strong_ordering::equal;
  }

  [[nodiscard]] std::string ToHex() const {
    static constexpr char hex_digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(Size() * 2);

    for (std::byte b : data_) {
      unsigned char byte = static_cast<unsigned char>(b);
      result.push_back(hex_digits[byte >> 4]);
      result.push_back(hex_digits[byte & 0x0F]);
    }

    return result;
  }

private:
  std::span<const std::byte> data_;
};

} // namespace kv
