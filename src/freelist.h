#pragma once

#include "page.h"
#include "type.h"
#include <unordered_map>
#include <vector>
namespace kv {
class Freelist {
public:
  Freelist() = default;

  [[nodiscard]] std::vector<Pgid> All() const noexcept {
    auto ids = ids_;
    for (const auto &[tx, p_ids] : pending_) {
      ids.insert(ids.end(), p_ids.begin(), p_ids.end());
    }
    return ids;
  }

  void Read(Page &p) noexcept {
    if (p.Count() == 0) {
      return;
    }
    auto *ids = reinterpret_cast<Pgid *>(&p);
    ids_ = std::vector<Pgid>(ids, ids + p.Count());
  }

  void Write(Page &p) const noexcept {
    auto ids = All();
    p.SetFlags(PageFlag::FreelistPage);
    p.SetCount(ids.size());
    std::copy(ids.begin(), ids.end(), p.GetDataAs<Pgid>());
  }

private:
  std::vector<Pgid> ids_;
  std::unordered_map<Txid, std::vector<Pgid>> pending_;
};
} // namespace kv
