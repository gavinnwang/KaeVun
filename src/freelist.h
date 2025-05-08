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

  [[nodiscard]] std::optional<Pgid> Allocate(std::size_t count) noexcept {
    // the count of ids in the current continuous segment
    std::size_t cnt = 0;
    Pgid prev_id = 0;
    for (std::size_t i = 0; i < ids_.size(); ++i) {
      auto id = ids_[i];
      // if the current is no longer continuous, reset
      if (prev_id != id - 1) {
        cnt = 0;
      }
      cnt++;
      if (cnt == count) {
        assert(id > 3);
        // remove the segment from ids
        ids_.erase(ids_.begin() + i - cnt + 1, ids_.begin() + i + 1);
        return id;
      }
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
