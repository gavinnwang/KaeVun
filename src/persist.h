#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace kv {

class Serializer {
public:
  explicit Serializer(void *ptr)
      : ptr_(reinterpret_cast<std::byte *>(ptr)), offset_(0) {}

  template <typename T> void Write(const T &data) noexcept {
    static_assert(std::is_trivially_copyable_v<T>,
                  "Write<T> only supports trivially copyable types");
    *reinterpret_cast<T *>(ptr_ + offset_) = data;
    offset_ += sizeof(T);
  }

  void Write(const std::string &data) noexcept {
    uint32_t sz = static_cast<uint32_t>(data.size());
    Write<uint32_t>(sz);
    std::memcpy(ptr_ + offset_, data.data(), data.size());
    offset_ += data.size();
  }

  void WriteBytes(const void *src, uint32_t size) noexcept {
    std::memcpy(ptr_ + offset_, src, size);
    offset_ += size;
  }

  void Seek(uint32_t new_offset) noexcept { offset_ = new_offset; }
  uint32_t Offset() const noexcept { return offset_; }

private:
  std::byte *ptr_;
  uint32_t offset_;
};

class Deserializer {
public:
  explicit Deserializer(const void *ptr)
      : ptr_(reinterpret_cast<const std::byte *>(ptr)), offset_(0) {}

  template <typename T> T Read() noexcept {
    const T *val = reinterpret_cast<const T *>(ptr_ + offset_);
    offset_ += sizeof(T);
    return *val;
  }

  template <> std::string Read<std::string>() noexcept {
    uint32_t sz = Read<uint32_t>();
    const char *data_ptr = reinterpret_cast<const char *>(ptr_ + offset_);
    std::string str(data_ptr, sz);
    offset_ += sz;
    return str;
  }

private:
  const std::byte *ptr_;
  uint32_t offset_;
};

} // namespace kv
