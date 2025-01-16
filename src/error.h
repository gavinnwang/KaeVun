#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace kv {

class Error {
public:
  enum class Code { Error = 0 };

  static constexpr std::string EMPTY_MSG = "";

private:
  Code code_;
  std::optional<std::string> message_;

public:
  [[nodiscard]] explicit Error() noexcept : code_(Code::Error) {}
  [[nodiscard]] explicit Error(std::string_view msg) noexcept
      : code_(Code::Error), message_(msg) {}
  [[nodiscard]] explicit Error(Code code, std::string_view msg) noexcept
      : code_(code), message_(msg) {}

  [[nodiscard]] const std::string &message() const noexcept {
    return message_ ? *message_ : EMPTY_MSG;
  }
};
} // namespace kv
