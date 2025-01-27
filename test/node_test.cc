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

  const std::array<std::byte, 2> data1 = {std::byte{0x08}, std::byte{0x69}};
  kv::Slice s3(data1.data(), data1.size());
  n1.Put(s3, s3);
  // LOG_DEBUG(n1.ToString());
  LOG_DEBUG(n1.ToString());
  // why does this write change to string
  kv::PageBuffer buf1{1, kv::OS::DEFAULT_PAGE_SIZE};
  auto &p1 = buf1.GetPage(0);
  n1.Write(p1);
  LOG_DEBUG(n1.ToString());
  // LOG_DEBUG(s1.ToString());
  // LOG_DEBUG(s2.ToString());
  // LOG_DEBUG(s3.ToString());

  kv::Node n2{};
  n2.Read(p1);
  LOG_DEBUG(n2.ToString());

  ASSERT_EQ(n1.ToString(), n2.ToString());
}

} // namespace test
