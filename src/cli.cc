#include "db.h"
#include <iostream>
#include <sstream>

using namespace kv;

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "Usage: kv_cli <db_path>" << std::endl;
    return 1;
  }

  auto db_or_err = DB::Open(argv[1]);
  if (!db_or_err) {
    std::cerr << "Failed to open DB: " << db_or_err.error().message()
              << std::endl;
    return 1;
  }

  auto db = std::move(db_or_err.value());

  std::cout << "Welcome to the KV CLI. Type 'exit' to quit." << std::endl;

  std::string line;
  while (true) {
    std::cout << "# ";
    if (!std::getline(std::cin, line))
      break;

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "exit") {
      break;
    } else if (command == "get") {
      std::string bucket, key;
      iss >> bucket >> key;
      if (bucket.empty() || key.empty()) {
        std::cout << "Usage: get <bucket> <key>" << std::endl;
        continue;
      }

      auto err = db->Update([&](Tx &tx) -> std::optional<Error> {
        auto bucket_opt = tx.GetBucket(bucket);
        if (!bucket_opt.has_value()) {
          std::cout << "Bucket not found" << std::endl;
          return {};
        }
        auto &b = bucket_opt.value();
        auto val_opt = b.Get(key);
        if (val_opt.has_value()) {
          std::cout << val_opt.value().ToString() << std::endl;
        } else {
          std::cout << "Key not found" << std::endl;
        }
        return {};
      });
      if (err.has_value()) {
        std::cerr << "Error: " << err.value().message() << std::endl;
      }

    } else if (command == "scan") {
      std::string bucket;
      iss >> bucket;
      if (bucket.empty()) {
        std::cout << "Usage: scan <bucket>" << std::endl;
        continue;
      }
      db->DebugPrintBucketPages(bucket);
    } else {
      std::cout << "Unknown command. Supported: get, scan, exit" << std::endl;
    }
  }

  return 0;
}
