#pragma once

#include "page.h"
#include <cstddef>
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
    std::size_t sz = static_cast<std::size_t>(data.size());
    Write<std::size_t>(sz);
    std::memcpy(ptr_ + offset_, data.data(), data.size());
    offset_ += data.size();
  }

  void WriteBytes(const void *src, std::size_t size) noexcept {
    std::memcpy(ptr_ + offset_, src, size);
    offset_ += size;
  }

  void Seek(std::size_t new_offset) noexcept { offset_ = new_offset; }
  std::size_t Offset() const noexcept { return offset_; }

private:
  std::byte *ptr_;
  std::size_t offset_;
};

class Deserializer {
public:
  explicit Deserializer(const void *ptr)
      : ptr_(reinterpret_cast<const std::byte *>(ptr)), offset_(0) {}

  explicit Deserializer(const Page &p) : Deserializer(p.Data()) {}

  template <typename T> T Read() noexcept {
    const T *val = reinterpret_cast<const T *>(ptr_ + offset_);
    offset_ += sizeof(T);
    return *val;
  }

  template <> std::string Read<std::string>() noexcept {
    std::size_t sz = Read<std::size_t>();
    const char *data_ptr = reinterpret_cast<const char *>(ptr_ + offset_);
    std::string str(data_ptr, sz);
    offset_ += sz;
    return str;
  }

private:
  const std::byte *ptr_;
  std::size_t offset_;
};

} // namespace kv
