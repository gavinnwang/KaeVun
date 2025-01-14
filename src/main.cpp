
#include "db.h"
#include "log.h"
#include <iostream>

int main() {
  std::cout << "Hello World" << std::endl;
  auto db_res = kv::DB::Open("./db.db");
  if (!db_res) {
    auto error = db_res.error();
  }

  std::unique_ptr<kv::DB> db = std::move(db_res.value());
  auto res = db->Begin(false);

  if (res.has_value()) {
    LOG_INFO("db works fine");
  } else {
    LOG_INFO("db works not fine");
  }
  return 0;
}
