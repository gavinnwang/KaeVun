#pragma once

#include "cursor.h"
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
  Bucket(TxBPlusTreeHandler &tx_handler, const std::string &name,
         const BucketMeta &meta) noexcept
      : tx_handler_(tx_handler), name_(name), meta_(meta) {}

  Bucket(const Bucket &) = delete;
  Bucket &operator=(const Bucket &) = delete;
  Bucket(Bucket &&) = default;
  Bucket &operator=(Bucket &&) = delete;
  ~Bucket() = default;

  [[nodiscard]] const std::string &Name() const noexcept { return name_; }
  [[nodiscard]] bool Writable() const noexcept;
  [[nodiscard]] Cursor CreateCursor() const noexcept {
    // todo: if tx is closed return err
    auto c = Cursor{tx_handler_};
    return c;
  }
  [[nodiscard]] Slice Get(const Slice &key) const noexcept {
    auto c = CreateCursor();
    return {};
  }

private:
  TxBPlusTreeHandler &tx_handler_;
  const std::string &name_;
  const BucketMeta &meta_;
};

// In memory representation of the buckets meta page
class Buckets {
public:
  explicit Buckets(Page &p) noexcept { Read(p); };

  Buckets(Buckets &&other) = default;
  Buckets &operator=(Buckets &&other) noexcept = default;
  Buckets(const Buckets &other) = delete;
  Buckets &operator=(const Buckets &other) = delete;

  // Size returns the number of buckets.
  [[nodiscard]] uint16_t Size() const noexcept { return buckets_.size(); }

  [[nodiscard]] std::optional<std::reference_wrapper<const BucketMeta>>
  GetBucket(const std::string &name) const noexcept {
    if (buckets_.find(name) == buckets_.end()) {
      return {};
    }
    return std::cref(buckets_.at(name));
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
