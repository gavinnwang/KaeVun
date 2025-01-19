#pragma once

#include "page.h"
#include "persist.h"
#include "type.h"
#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace kv {

class Tx;

class BucketTx {
public:
  explicit BucketTx(Tx *tx, const std::string *name) : tx_(tx), name_(name) {}

  [[nodiscard]] const std::string &Name() const noexcept {
    assert(name_ != nullptr);
    return *name_;
  }

  [[nodiscard]] Tx *Transaction() const noexcept { return tx_; }

private:
  Tx *tx_;
  const std::string *name_;
};

class Bucket {
public:
  Bucket(Pgid root, uint64_t auto_id) : root_(root), auto_id_(auto_id) {}

  [[nodiscard]] Pgid Root() const noexcept { return root_; }

  [[nodiscard]] uint64_t AutoId() const noexcept { return auto_id_; }

private:
  Pgid root_;
  uint64_t auto_id_;
};

// In memory representation of the buckets meta page
class Buckets {
public:
  explicit Buckets(Page &p) noexcept : p_(p) { Read(); };

  [[nodiscard]] uint16_t Size() const noexcept { return buckets_.size(); }

  [[nodiscard]] std::optional<std::reference_wrapper<Bucket>>
  GetBucket(const std::string &name) noexcept {
    if (buckets_.find(name) == buckets_.end()) {
      return std::nullopt;
    }
    return std::ref(buckets_.at(name));
  }

private:
  void Read() noexcept {
    Deserializer d(&p_);
    for (uint32_t i = 0; i < p_.Count(); i++) {
      std::string name = d.Read<std::string>();
      uint64_t auto_id = d.Read<uint64_t>();
      Pgid root = d.Read<Pgid>();
      assert(buckets_.find(name) == buckets_.end());
      buckets_.emplace(std::move(name), Bucket(root, auto_id));
    }
  }

  void Write() noexcept {
    p_.SetCount(buckets_.size());
    Serializer s{&p_};
    for (const auto &[name, b] : buckets_) {
      s.Write(name);
      s.Write(b.AutoId());
      s.Write(b.Root());
    }
  }

  Page &p_;
  std::unordered_map<std::string, Bucket> buckets_{};
};

} // namespace kv
