#pragma once

#include <vector>
#include <string>
#include <cstring>
#include <compare>
#include <cassert>

namespace kv {

// Owning byte sequence
class Slice {
public:
  Slice() noexcept = default;

  // Construct from raw data and size
  Slice(const std::byte* data, size_t size)
      : data_(data, data + size) {}

  // Construct from std::string
  Slice(const std::string& str)
      : data_(reinterpret_cast<const std::byte*>(str.data()),
              reinterpret_cast<const std::byte*>(str.data()) + str.size()) {}

  // Construct from const char*
  Slice(const char* str)
      : data_(reinterpret_cast<const std::byte*>(str),
              reinterpret_cast<const std::byte*>(str) + std::strlen(str)) {}

  const std::byte* Data() const noexcept { return data_.data(); }
  size_t Size() const noexcept { return data_.size(); }

  std::byte operator[](size_t index) const noexcept {
    assert(index < Size());
    return data_[index];
  }

  std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(Data()), Size());
  }

  std::strong_ordering operator<=>(const Slice& other) const noexcept {
    const size_t min_len = std::min(Size(), other.Size());
    const int cmp = std::memcmp(Data(), other.Data(), min_len);

    if (cmp < 0) return std::strong_ordering::less;
    if (cmp > 0) return std::strong_ordering::greater;
    return Size() <=> other.Size();
  }

  bool operator==(const Slice& other) const noexcept {
    return (*this <=> other) == std::strong_ordering::equal;
  }

  std::string ToHex() const {
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
  std::vector<std::byte> data_;
};

} // namespace kv
