#pragma once

#include "bucket.h"
#include "disk.h"
#include "error.h"
#include "node.h"
#include "page.h"
#include "type.h"
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <unordered_map>
namespace kv {

class Tx {

public:
  Tx(DiskHandler &disk, bool writable) noexcept
      : open_(true), disk_(&disk), writable_(writable),
        buckets_(Buckets{disk_->GetPage(3)}) {};

  Tx(const Tx &) = delete;
  Tx &operator=(const Tx &) = delete;

  Tx(Tx &&) = default;
  Tx &operator=(Tx &&) noexcept = default;

  void Rollback() noexcept { LOG_INFO("Rolling back tx"); };

  [[nodiscard]] std::optional<Error> Commit() noexcept { return std::nullopt; }

  [[nodiscard]] std::optional<BucketMeta>
  GetBucket(const std::string &name) noexcept {
    return buckets_.Bucket(name);
  }

  [[nodiscard]] Page &GetPage(Pgid id) noexcept;

  [[nodiscard]] std::expected<BucketMeta, Error>
  CreateBucket(const std::string &name) noexcept {
    if (!open_) {
      return std::unexpected{Error{"Tx not open"}};
    }
    if (!writable_) {
      return std::unexpected{Error{"Tx not writable"}};
    }
    if (buckets_.Bucket(name)) {
      return std::unexpected{Error{"Bucket exists"}};
    }
    if (name.size() == 0) {
      return std::unexpected{Error{"Bucket name required"}};
    }

    return std::unexpected{Error{"Tx not writable"}};
  }

  [[nodiscard]] Meta &GetMeta() noexcept { return meta_; }

  [[nodiscard]] Node *GetNode(Pgid pgid, Node *parent) noexcept {
    // 1. Return existing node if cached.
    if (auto it = nodes_.find(pgid); it != nodes_.end())
      return &it->second;

    // 2. Otherwise construct a blank Node in-place inside the map.
    auto [it, ok] = nodes_.emplace(pgid, Node{});
    Node &node = it->second;

    node.SetTx(this); // fix is here
    node.SetParent(parent);
    if (parent)
      node.SetDepth(parent->GetDepth() + 1);

    // read page into node …
    Page &p = GetPage(pgid);
    node.Read(p);

    return &node;
  }

private:
  [[nodiscard]] std::expected<Page *, Error> Allocate(uint32_t count) {
    return nullptr;
  }

  bool open_{false};
  DiskHandler *disk_;
  bool writable_{false};
  std::vector<Node> pending_;
  std::unordered_map<Pgid, Page> pages_{};
  std::unordered_map<Pgid, Node> nodes_{};
  Buckets buckets_;
  Meta meta_;
};
} // namespace kv
