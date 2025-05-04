#include "bucket.h"
#include "tx.h"
namespace kv {

[[nodiscard]] bool Bucket::Writable() const noexcept { return tx_.Writable(); }

} // namespace kv
