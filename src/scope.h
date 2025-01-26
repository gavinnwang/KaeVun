#include <functional>
namespace kv {

// Invoke a noexcept function when guard goes out of scope similar to defer
class ScopeGuard {
  std::function<void()> func_;
  bool active_;

public:
  explicit ScopeGuard(std::function<void()> func)
      : func_(std::move(func)), active_(true) {}

  ScopeGuard(const ScopeGuard &) = delete;
  ScopeGuard &operator=(const ScopeGuard &) = delete;
  ScopeGuard(ScopeGuard &&other) noexcept
      : func_(std::move(other.func_)), active_(other.active_) {
    other.active_ = false;
  }
  ScopeGuard &operator=(ScopeGuard &&other) noexcept {
    if (this != &other) {
      if (active_)
        func_();
      func_ = std::move(other.func_);
      active_ = other.active_;
      other.active_ = false;
    }
    return *this;
  }

  ~ScopeGuard() {
    if (active_) {
      func_();
    }
  }

  void dismiss() noexcept { active_ = false; }
};

template <typename F> ScopeGuard Defer(F &&func) {
  static_assert(std::is_nothrow_invocable_v<F>, "Function must be noexcept");
  return ScopeGuard(std::forward<F>(func));
}
} // namespace kv
