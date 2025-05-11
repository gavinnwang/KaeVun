
#include "bucket_meta.h"
#include "db.h"
#include "page.h"
#include <cassert>
#include <gtest/gtest.h>
namespace test {

[[nodiscard]] kv::DB::RAII_DB
GetTmpDB(const std::filesystem::path &path = "./db.db") {
  auto db_or_err = kv::DB::Open(path);
  assert(db_or_err);
  return std::move(*db_or_err);
}

[[nodiscard]] std::optional<kv::Error>
DeleteDBFile(const std::filesystem::path &path = "./db.db") noexcept {
  if (!std::filesystem::exists(path)) {
    return std::nullopt;
  }

  std::error_code ec;
  std::filesystem::remove(path, ec);
  if (ec) {
    return kv::Error{"Failed to delete DB file: " + ec.message()};
  }

  return std::nullopt;
}

TEST(BucketTest, BucketsPageTest) {
  kv::PageBuffer buf{1, kv::OS::DEFAULT_PAGE_SIZE};
  auto &p = buf.GetPage(0);
  kv::Buckets b{p};
  auto e = b.AddBucket("bucket1", kv::BucketMeta{5});
  b.Write(p);

  kv::Buckets b1{p};
  auto m = b1.GetBucket("bucket1");
  assert(m.has_value());
}

TEST(BucketTest, Test1) {
  auto e = DeleteDBFile();
  if (e) {
    LOG_DEBUG("failed to remove db file");
    std::abort();
  }
  auto db = GetTmpDB();
  auto err = db->Update([](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket_result = tx.CreateBucket("bucket");
    if (!bucket_result.has_value()) {
      LOG_DEBUG("CreateBucket failed: {}", bucket_result.error().message());
    } else {
      LOG_DEBUG("CreateBucket success bucket root {}",
                bucket_result.value().Root());
    }

    auto bucket_opt = tx.GetBucket("bucket");
    if (!bucket_opt.has_value()) {
      LOG_ERROR("GetBucket failed: bucket 'bucket' not found");
      std::abort();
    }

    auto &b = bucket_opt.value(); // Safe now
    auto e = b.Put("key1", "val1");
    e = b.Put("key2", "val2");
    e = b.Put("key3", "val3");
    e = b.Put("key4", "val4");
    e = b.Put("key0", "val0");

    assert(!e);

    auto slice_or_err = b.Get("key1");
    LOG_INFO("return {}", slice_or_err.value().ToString());
    return {};
  });
  assert(!err);
  err = db->Update([](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket_result = tx.CreateBucket("bucket");
    if (!bucket_result.has_value()) {
      LOG_DEBUG("CreateBucket failed: {}", bucket_result.error().message());
    } else {
      LOG_DEBUG("CreateBucket success bucket root {}",
                bucket_result.value().Root());
    }

    auto bucket_opt = tx.GetBucket("bucket");
    if (!bucket_opt.has_value()) {
      LOG_ERROR("GetBucket failed: bucket 'bucket' not found");
      std::abort();
    }

    auto &b = bucket_opt.value(); // Safe now
    auto e = b.Put("key1", "val1");
    e = b.Put("key2", "val2");
    e = b.Put("key3", "val3");
    e = b.Put("key4", "val4");
    e = b.Put("key0", "val0");

    assert(!e);

    auto slice_or_err = b.Get("key1");
    LOG_INFO("return {}", slice_or_err.value().ToString());
    return {};
  });
  assert(!err);
}
} // namespace test
