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
  Tx(DiskHandler &disk, bool writable, Meta db_meta) noexcept
      : open_(true), disk_(&disk), writable_(writable), meta_(db_meta),
        buckets_(Buckets{disk_->GetPage(meta_.GetBuckets())}) {
    if (writable_) {
      meta_.IncrementTxid();
    }
  };

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

private:
  [[nodiscard]] Page &GetPage(Pgid id) noexcept;
  [[nodiscard]] Meta &GetMeta() noexcept { return meta_; }

  // GetNode creates a node from  apage and associates with a given parent.
  [[nodiscard]] Node &GetNode(Pgid pgid, Node *parent) noexcept {
    // 1. Return existing node if cached.
    if (auto it = nodes_.find(pgid); it != nodes_.end())
      return it->second;

    // 2. Otherwise construct a blank Node in-place inside the map.
    auto [it, ok] = nodes_.emplace(pgid, Node{});
    Node &node = it->second;

    node.SetTx(this);
    node.SetParent(parent);
    if (parent)
      node.SetDepth(parent->GetDepth() + 1);

    // read page into node …
    Page &p = GetPage(pgid);
    node.Read(p);

    return node;
  }

  // Write write any dirty pages to disk.
  [[nodiscard]] std::optional<Error> Write() noexcept {
    // Collect dirty pages
    std::vector<Page *> dirty_pages;
    dirty_pages.reserve(pages_.size());
    for (const auto &[_, page_ptr] : pages_) {
      dirty_pages.push_back(page_ptr);
    }

    // Sort pages by their pgid
    std::sort(dirty_pages.begin(), dirty_pages.end(),
              [](const Page *a, const Page *b) { return a->Id() < b->Id(); });

    // Write pages to disk in order
    for (const auto p : dirty_pages) {
      disk_->WritePage(*p);
    }
    disk_->Sync();

    // Clear out the page cache.
    pages_.clear();

    return {};
  }

  // WriteMeta writes the meta to the disk.
  [[nodiscard]] std::optional<Error> WriteMeta() noexcept {
    PageBuffer buf{1, disk_->PageSize()};
    auto &p = buf.GetPage(0);
    meta_.Write(p);
    // Write the meta page to file.
    auto err = disk_->WritePage(p);
    if (err) {
      return err;
    }
    err = disk_->Sync();
    if (err) {
      return err;
    }
    return {};
  }

  [[nodiscard]] std::expected<Page *, Error> Allocate(uint32_t count) {
    return nullptr;
  }

  bool open_{false};
  DiskHandler *disk_;
  bool writable_{false};
  Meta meta_;
  std::vector<Node> pending_;
  // Page cache
  std::unordered_map<Pgid, Page *> pages_{};
  // nodes_ represents the in-memory version of pages allowing for key value
  // changes.
  std::unordered_map<Pgid, Node> nodes_{};
  Buckets buckets_;
};
} // namespace kv
