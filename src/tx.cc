#include "tx.h"
#include "db.h"

namespace kv {

Tx::Tx(DB *db, bool writable) noexcept : db_(db), writable_(writable) {
  // copy meta
  // get all the buckets by reading the buckets meta page
  buckets.read(tx.page(tx.meta.buckets))
  // if writable increment the tx id in meta
}

[[nodiscard]] Page &Tx::GetPage(Pgid id) noexcept {
  if (page_.find(id) != page_.end()) {
    return page_.at(id);
  }
  return db_->GetPage(id);
}

} // namespace kv
