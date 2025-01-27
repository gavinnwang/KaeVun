#include "log.h"
#include "node.h"
#include "os.h"
#include "page.h"
#include "gtest/gtest.h"

namespace test {

TEST(NodeTest, TestOneElement) {
  kv::Node n{};
  kv::PageBuffer buf{1, kv::OS::DEFAULT_PAGE_SIZE};
  auto &p = buf.GetPage(0);
  p.SetFlags(kv::PageFlag::LeafPage);
  n.Read(p);
  LOG_DEBUG(n.ToString());

  kv::Slice s1{"hi"};
  kv::Slice s2{"wsg"};
  kv::Slice val{"value"};
  n.Put(s1, val);
  n.Put(s2, val);
  n.Write(p);
  LOG_DEBUG(n.ToString());

  kv::Node n1{};
  n1.Read(p);
  LOG_DEBUG(n1.ToString());

  ASSERT_EQ("[( key: wsg, val: value ), ( key: hi, val: value )]",
            n1.ToString());
}

} // namespace test
