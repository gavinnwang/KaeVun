#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace kv {

class Status {
public:
  enum class Code { OK = 0, Error };

private:
  Code code_;
  std::optional<std::string> message_;

public:
  [[nodiscard]] explicit Status() noexcept : code_(Code::OK) {}
  [[nodiscard]] explicit Status(Code code, std::string_view msg) noexcept
      : code_(code), message_(msg) {}
  [[nodiscard]] static Status OK() { return Status(); }
  [[nodiscard]] static Status Error(const std::string_view msg) {
    return Status(Code::Error, msg);
  }

  // Check if the status is ok
  [[nodiscard]] bool ok() const noexcept { return code_ == Code::OK; }
  [[nodiscard]] const std::optional<std::string> &message() const noexcept {
    return message_;
  }
};
} // namespace kv
