#pragma once

#include "cursor.h"
#include "page.h"
#include "persist.h"
#include "type.h"
#include <cassert>
#include <optional>
#include <string>
#include <unordered_map>

namespace kv {

// Bucket associated with a tx
class Bucket {
public:
  Bucket(ShadowPageHandler &sp_handler, const std::string &name,
         const BucketMeta &meta) noexcept
      : sp_handler_(sp_handler), name_(name), meta_(meta) {}

  Bucket(const Bucket &) = delete;
  Bucket &operator=(const Bucket &) = delete;
  Bucket(Bucket &&) = default;
  Bucket &operator=(Bucket &&) = delete;
  ~Bucket() = default;

  [[nodiscard]] const std::string &Name() const noexcept { return name_; }
  // [[nodiscard]] bool Writable() const noexcept { return meta_.;};
  [[nodiscard]] Cursor CreateCursor() const noexcept {
    // todo: if tx is closed return err
    auto c = Cursor{sp_handler_, meta_};
    return c;
  }
  [[nodiscard]] std::optional<Slice> Get(const Slice &key) const noexcept {
    // validations
    LOG_INFO("getting {}", key.ToString());
    auto c = CreateCursor();
    auto opt = c.Seek(key);
    if (!opt.has_value())
      return std::nullopt;
    auto [k, v] = opt.value();
    if (k != key) {
      return std::nullopt;
    }
    return v;
  }
  [[nodiscard]] std::optional<Error> Put(const Slice &key,
                                         const Slice &val) noexcept {
    LOG_INFO("putting {}", key.ToString());
    // validations
    if (key.Size() == 0) {
      return Error{"Key size cannot be zero."};
    }
    auto c = CreateCursor();
    auto _ = c.Seek(key);
    auto &n = c.GetNode();
    n.Put(key, val);

    LOG_INFO("done putting {} {}", key.ToString(), n.ToString());
    return {};
  }

private:
  // only used for write tx
  ShadowPageHandler &sp_handler_;
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
  [[nodiscard]] std::size_t Size() const noexcept { return buckets_.size(); }

  [[nodiscard]] std::optional<std::reference_wrapper<const BucketMeta>>
  GetBucket(const std::string &name) const noexcept {
    auto b_it = buckets_.find(name);
    if (b_it == buckets_.end()) {
      return {};
    }
    return std::cref(b_it->second);
  }

  // Adds a new bucket. Returns false if the bucket already exists.
  [[nodiscard]] std::expected<std::reference_wrapper<const BucketMeta>,
                              std::string>
  AddBucket(std::string name, BucketMeta meta) noexcept {
    if (buckets_.find(name) != buckets_.end()) {
      return std::unexpected{"bucket already exists: " + name};
    }

    auto [it, _] = buckets_.emplace(std::move(name), std::move(meta));
    return std::cref(it->second);
  }
  [[nodiscard]] std::size_t GetStorageSize() const noexcept {
    auto sz = PAGE_HEADER_SIZE;
    sz += sizeof(BucketMeta) * buckets_.size();
    for (const auto &[name, _] : buckets_) {
      sz += name.size();
    }
    return sz;
  }

  void Write(Page &p) const noexcept {
    p.SetMagic();
    p.SetFlags(PageFlag::BucketPage);
    p.SetCount(buckets_.size());
    Serializer s{p.Data()};
    for (const auto &[name, b] : buckets_) {
      s.Write(name);
      s.Write(b.Root());
    }
  }

private:
  void Read(Page &p) noexcept {
    LOG_DEBUG("Starting to read bucket metadata from page with id {}", p.Id());

    Deserializer d(p);
    for (std::size_t i = 0; i < p.Count(); i++) {
      auto name = d.Read<std::string>();
      const auto root = d.Read<Pgid>();

      LOG_DEBUG("Deserialized bucket {} with root page id {}", name, root);
      assert(name.size() > 0 && root > 2);

      assert(buckets_.find(name) == buckets_.end() &&
             "bucket names should not be duplicate");
      buckets_.emplace(std::move(name), BucketMeta{root});
    }

    LOG_DEBUG("Finished reading {} bucket(s) from page {}", p.Count(), p.Id());
  }

  std::unordered_map<std::string, BucketMeta> buckets_{};
};

} // namespace kv
