#pragma once

#include "error.h"
#include <optional>
#include <unistd.h>

namespace kv {

// RAII wrapper for file descriptors
class Fd {
public:
  Fd() noexcept : fd_(-1) {}

  [[nodiscard]] explicit Fd(int fd) noexcept : fd_(fd) {}

  [[nodiscard]] std::optional<Error> Reset() noexcept {
    if (fd_ != -1) {
      if (::close(fd_) == -1) {
        return Error("Error releasing fd");
      }
      fd_ = -1;
    }
    return std::nullopt;
  }

  [[nodiscard]] std::optional<Error> Sync() const noexcept {
    if (!IsValid()) {
      return Error{"Invalid file descriptor"};
    }
    if (::fsync(fd_) == -1) {
      return Error{"Error syncing fd"};
    }
    return std::nullopt;
  }

  [[nodiscard]] int GetFd() const noexcept { return fd_; }

  [[nodiscard]] bool IsValid() const noexcept { return fd_ != -1; }

  // move constructors
  Fd(Fd &&other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  Fd &operator=(Fd &&other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  // delete the copy constructors
  Fd(const Fd &) = delete;
  Fd &operator=(const Fd &) = delete;

  ~Fd() { Reset(); }

private:
  int fd_;
};

} // namespace kv
