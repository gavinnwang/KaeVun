#include <functional>
namespace kv {

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

  ~ScopeGuard() {
    if (active_) {
      func_();
    }
  }

  void dismiss() noexcept { active_ = false; }
};

template <typename F> ScopeGuard Defer(F &&func) {
  return ScopeGuard(std::forward<F>(func));
}
} // namespace kv
