#include "tx.h"
#include "db.h"

namespace kv {

Tx::Tx(DB &db) noexcept : db_(db) {}

} // namespace kv
