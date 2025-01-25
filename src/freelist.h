#pragma once

#include "page.h"
#include "type.h"
#include <algorithm>
#include <cassert>
#include <cstdint>
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
    auto *ids = p.GetDataAs<Pgid>();
    ids_ = std::vector<Pgid>(ids, ids + p.Count());
  }

  void Write(Page &p) const noexcept {
    auto ids = All();
    p.SetFlags(PageFlag::FreelistPage);
    p.SetCount(ids.size());
    std::copy(ids.begin(), ids.end(), p.GetDataAs<Pgid>());
  }

  [[nodiscard]] std::optional<Pgid> Allocate(uint32_t size) const noexcept {
    uint32_t cnt = 0;
    Pgid prev_id = 0;
    for (const auto id : ids_) {
      if (prev_id == 0 || prev_id != id - 1) {
        cnt = 1;
      }
      if (cnt == size) {
        assert(id > 3);
        return id;
      }
      cnt++;
      prev_id = id;
    }
    return std::nullopt;
  }

  void Free(Txid txid, Page &p) noexcept {
    assert(p.Id() > 3);
    for (Pgid i = p.Id(); i <= p.Id() + p.Overflow(); ++i) {
      pending_[txid].push_back(i);
    }
  }

  void Release(Txid txid) noexcept {
    for (const auto id : pending_[txid]) {
      ids_.push_back(id);
    }
    pending_.erase(txid);
    std::sort(ids_.begin(), ids_.end());
  }

private:
  std::vector<Pgid> ids_;
  std::unordered_map<Txid, std::vector<Pgid>> pending_;
};
} // namespace kv
