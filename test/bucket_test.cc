
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
  db->Update([](kv::Tx &tx) -> std::optional<kv::Error> {
    auto bucket = tx.CreateBucket("buck").value();
    auto b = tx.GetBucket("bucket").value();
    return {};
  });
}
} // namespace test
