#pragma once

#include <error.h>
#include <optional>
#include <unistd.h>

namespace kv {

// RAII wrapper for file descriptors
class Fd {
public:
  Fd() noexcept : fd_(-1) {}

  [[nodiscard]] explicit Fd(int fd) noexcept : fd_(fd) {}

  std::optional<Error> Close() noexcept {
    if (fd_ != -1) {
      if (::close(fd_) == -1) {
        return Error("Error releasing fd");
      }
      fd_ = -1;
    }
    return std::nullopt;
  }

  int GetFd() const noexcept { return fd_; }

  bool IsValid() const noexcept { return fd_ != -1; }

  Fd(Fd &&other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  Fd &operator=(Fd &&other) noexcept {
    if (this != &other) {
      Close();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  Fd(const Fd &) = delete;
  Fd &operator=(const Fd &) = delete;

  ~Fd() { Close(); }

private:
  int fd_;
};

} // namespace kv
