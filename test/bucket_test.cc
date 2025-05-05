
#include "db.h"
#include <cassert>
#include <gtest/gtest.h>
namespace test {

[[nodiscard]] kv::DB::RAII_DB
GetTmpDB(const std::filesystem::path &path = "./db.db") {
  auto db_or_err = kv::DB::Open(path);
  assert(db_or_err && db_or_err.error().message().c_str());
  return std::move(*db_or_err);
}

TEST(BucketTest, Test1) {
  auto db = GetTmpDB();
  auto err = db->Update([](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket_result = tx.CreateBucket("bucket");
    if (!bucket_result.has_value()) {
      LOG_ERROR("CreateBucket failed: {}", bucket_result.error().message());
      std::abort();
    }

    auto bucket_opt = tx.GetBucket("bucket");
    if (!bucket_opt.has_value()) {
      LOG_ERROR("GetBucket failed: bucket 'bucket' not found");
      std::abort();
    }

    auto &b = bucket_opt.value(); // Safe now
    auto s = b.Get("Bucket");
    return {};
  });
  assert(!err);
}
} // namespace test
