#include "tx.h"
#include "db.h"

namespace kv {

Tx::Tx(DB &db) noexcept : db_(db) {
  // copy meta
  // get a new bucket for this tx
  // if writable increment the tx id in meta
}

} // namespace kv
