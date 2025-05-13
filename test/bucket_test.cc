#include "db.h"
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

// TEST(BucketTest, BucketsPageTest) {
//   kv::PageBuffer buf{1, kv::OS::DEFAULT_PAGE_SIZE};
//   auto &p = buf.GetPage(0);
//   kv::Buckets b{p};
//   auto e = b.AddBucket("bucket1", kv::BucketMeta{5});
//   b.Write(p);
//
//   kv::Buckets b1{p};
//   auto m = b1.GetBucket("bucket1");
//   assert(m.has_value());
// }

TEST(BucketTest, BucketCreateAndReadTest) {
  // Clean up any previous test artifacts.
  auto err = DeleteDBFile();
  if (err) {
    LOG_DEBUG("Failed to remove existing db file");
    std::abort();
  }

  auto db = GetTmpDB();

  auto keys_and_vals = std::vector<std::pair<std::string, std::string>>{
      {"key1", "val1"}, {"key2", "val2"}, {"key3", "val3"},
      {"key4", "val4"}, {"key0", "val0"},
  };

  auto insert_keys = [&](kv::Bucket &bucket) {
    for (const auto &[key, val] : keys_and_vals) {
      assert(!bucket.Put(key, val));
    }
  };

  // Initial bucket creation and insertion test.
  err = db->Update([&](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket_result = tx.CreateBucket("bucket");
    if (!bucket_result.has_value()) {
      LOG_DEBUG("CreateBucket failed: {}", bucket_result.error().message());
      std::abort();
    }

    auto bucket_opt = tx.GetBucket("bucket");
    if (!bucket_opt.has_value()) {
      LOG_ERROR("GetBucket failed: bucket 'bucket' not found");
      std::abort();
    }

    auto &bucket = bucket_opt.value();
    insert_keys(bucket);

    for (const auto &[key, val] : keys_and_vals) {
      auto get_result = bucket.Get(key);
      assert(get_result.has_value() && get_result.value() == val);
      LOG_INFO("Inserted and verified key '{}' with value '{}'", key, val);
    }

    return {};
  });
  assert(!err);

  // Reopen the bucket and validate data persists for all keys.
  err = db->Update([&](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket_opt = tx.GetBucket("bucket");
    if (!bucket_opt.has_value()) {
      LOG_ERROR("GetBucket failed: bucket 'bucket' not found");
      std::abort();
    }

    auto &bucket = bucket_opt.value();
    for (const auto &[key, val] : keys_and_vals) {
      auto get_result = bucket.Get(key);
      assert(get_result.has_value() && get_result.value() == val);
      LOG_INFO("Persisted key '{}' has value '{}'", key, val);
    }

    return {};
  });
  assert(!err);
}

TEST(BucketTest, BucketCreateAndReadLargeTest) {
  // Clean up any previous test artifacts.
  auto err = DeleteDBFile();
  if (err) {
    LOG_DEBUG("Failed to remove existing db file");
    std::abort();
  }

  auto keys_and_vals = [&]() {
    std::vector<std::pair<std::string, std::string>> result;
    for (int i = 0; i <= 200; ++i) {
      std::ostringstream key_stream;
      key_stream << "key" << std::setw(5) << std::setfill('0') << i;

      std::ostringstream val_stream;
      val_stream << "val" << std::setw(5) << std::setfill('0') << i;

      result.emplace_back(key_stream.str(), val_stream.str());
    }
    return result;
  }();
  {
    auto db = GetTmpDB();

    auto insert_keys = [&](kv::Bucket &bucket) {
      for (const auto &[key, val] : keys_and_vals) {
        auto e = bucket.Put(key, val);
        assert(!e);
      }
    };

    // Initial bucket creation and insertion test.
    err = db->Update([&](kv::Tx &tx) -> std::optional<kv::Error> {
      auto bucket_result = tx.CreateBucket("bucket");
      if (!bucket_result.has_value()) {
        LOG_DEBUG("CreateBucket failed: {}", bucket_result.error().message());
        std::abort();
      }

      auto bucket_opt = tx.GetBucket("bucket");
      if (!bucket_opt.has_value()) {
        LOG_ERROR("GetBucket failed: bucket 'bucket' not found");
        std::abort();
      }

      auto &bucket = bucket_opt.value();
      insert_keys(bucket);

      for (const auto &[key, val] : keys_and_vals) {
        auto get_result = bucket.Get(key);
        assert(get_result.has_value() && get_result.value() == val);
        LOG_INFO("Inserted and verified key '{}' with value '{}'", key, val);
      }

      return {};
    });
    assert(!err);
    db->DebugPrintBucketPages("bucket");
  }

  auto db1 = GetTmpDB();
  // Reopen the bucket and validate data persists for all keys.
  err = db1->Update([&](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket_opt = tx.GetBucket("bucket");
    if (!bucket_opt.has_value()) {
      LOG_ERROR("GetBucket failed: bucket 'bucket' not found");
      std::abort();
    }
    db1->DebugPrintBucketPages("bucket");

    auto &bucket = bucket_opt.value();
    for (const auto &[key, val] : keys_and_vals) {
      auto get_result = bucket.Get(key);
      assert(get_result.has_value() && get_result.value() == val);
      LOG_WARN("Persisted key '{}' has value '{}'", key,
               get_result.value().ToString());
    }

    return {};
  });
  assert(!err);
}
} // namespace test
