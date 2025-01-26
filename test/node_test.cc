
#include "node.h"
#include "os.h"
#include "page.h"
#include "gtest/gtest.h"

namespace test {

TEST(NodeTest, Test1) {
  kv::Node n{};
  kv::PageBuffer buf{1, kv::OS::DEFAULT_PAGE_SIZE};
  auto &p = buf.GetPage(0);
  p.SetCount(2);
  p.SetFlags(kv::PageFlag::LeafPage);
  n.Read(p);
}
} // namespace test
