
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

TEST(BucketTest, TransactionalInsertAndReadSequence) {
  // Clean up previous DB
  auto err = DeleteDBFile();
  ASSERT_FALSE(err.has_value());

  auto db = GetTmpDB();

  // Create bucket first
  err = db->Update([&](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket_result = tx.CreateBucket("bucket");
    if (!bucket_result.has_value()) {
      return kv::Error{"Failed to create bucket"};
    }
    return {};
  });
  ASSERT_FALSE(err.has_value());

  // Prepare keys
  std::vector<std::pair<std::string, std::string>> keys_and_vals;
  for (int i = 0; i < 2000; ++i) {
    keys_and_vals.emplace_back("key" + std::to_string(i),
                               "val" + std::to_string(i));
  }

  // For each key, insert and verify in separate transactions
  for (const auto &[key, val] : keys_and_vals) {
    // Insert in tx
    err = db->Update([&](kv::Tx &tx) -> std::optional<kv::Error> {
      auto bucket_opt = tx.GetBucket("bucket");
      if (!bucket_opt.has_value()) {
        return kv::Error{"Bucket not found"};
      }
      auto &bucket = bucket_opt.value();
      auto put_err = bucket.Put(key, val);
      if (put_err.has_value()) {
        return put_err;
      }
      return {};
    });
    ASSERT_FALSE(err.has_value());

    // Read in next tx
    err = db->Update([&](kv::Tx &tx) -> std::optional<kv::Error> {
      auto bucket_opt = tx.GetBucket("bucket");
      if (!bucket_opt.has_value()) {
        return kv::Error{"Bucket not found"};
      }
      auto &bucket = bucket_opt.value();
      auto get_result = bucket.Get(key);
      if (!get_result.has_value() || get_result.value() != val) {
        LOG_DEBUG("value not found");
        return kv::Error{"Value verification failed"};
      }
      return {};
    });
    ASSERT_FALSE(err.has_value());
  }
}
} // namespace test
