#include "tx.h"
#include "db.h"

namespace kv {

Tx::Tx(DB *db, bool writable) noexcept
    : db_(db), writable_(writable), buckets_(Buckets{db_->GetPage(3)}) {
  // copy meta
  // buckets page should read from the meta owned by the tx
  // get all the buckets by reading the buckets meta page
  // if writable increment the tx id in meta
}

[[nodiscard]] Page &Tx::GetPage(Pgid id) noexcept {
  if (page_.find(id) != page_.end()) {
    return page_.at(id);
  }
  return db_->GetPage(id);
}

} // namespace kv
