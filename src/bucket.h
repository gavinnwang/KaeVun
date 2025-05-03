#pragma once

#include "page.h"
#include "persist.h"
#include "type.h"
#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

namespace kv {

class Tx;

class BucketMeta {
public:
  BucketMeta(Pgid root, uint64_t auto_id) : root_(root), auto_id_(auto_id) {}

  [[nodiscard]] Pgid Root() const noexcept { return root_; }

  [[nodiscard]] uint64_t AutoId() const noexcept { return auto_id_; }

private:
  Pgid root_;
  uint64_t auto_id_;
};

// Bucket associated with a tx
class Bucket {
public:
  explicit Bucket(Tx &tx, const std::string &name, BucketMeta meta)
      : tx_(tx), name_(name), meta_(std::move(meta)) {}

  [[nodiscard]] const std::string &Name() const noexcept { return name_; }

  [[nodiscard]] Tx &Transaction() const noexcept { return tx_; }

private:
  Tx &tx_;
  const std::string &name_;
  BucketMeta meta_;
};

// In memory representation of the buckets meta page
class Buckets {
public:
  explicit Buckets(Page &p) noexcept { Read(p); };

  Buckets(Buckets &&other) = default;
  Buckets &operator=(Buckets &&other) noexcept = default;
  Buckets(const Buckets &other) = delete;
  Buckets &operator=(const Buckets &other) = delete;

  [[nodiscard]] uint16_t Size() const noexcept { return buckets_.size(); }

  [[nodiscard]] std::optional<std::reference_wrapper<BucketMeta>>
  Bucket(const std::string &name) noexcept {
    if (buckets_.find(name) == buckets_.end()) {
      return std::nullopt;
    }
    return std::ref(buckets_.at(name));
  }

private:
  void Read(Page &p) noexcept {
    Deserializer d(&p);
    for (uint32_t i = 0; i < p.Count(); i++) {
      auto name = d.Read<std::string>();
      const auto auto_id = d.Read<uint64_t>();
      const auto root = d.Read<Pgid>();
      assert(buckets_.find(name) == buckets_.end() &&
             "bucket names should not be duplicate");
      buckets_.emplace(std::move(name), BucketMeta{root, auto_id});
    }
  }

  void Write(Page &p) const noexcept {
    p.SetFlags(PageFlag::BucketPage);
    p.SetCount(buckets_.size());
    Serializer s{p.Data()};
    for (const auto &[name, b] : buckets_) {
      s.Write(name);
      s.Write(b.AutoId());
      s.Write(b.Root());
    }
  }

  std::unordered_map<std::string, BucketMeta> buckets_{};
};

} // namespace kv
