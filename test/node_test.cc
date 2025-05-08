#include "log.h"
#include "node.h"
#include "os.h"
#include "page.h"
#include "gtest/gtest.h"
#include <random>

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

  ASSERT_EQ("[( key: hi, val: value ), ( key: wsg, val: value )]",
            n1.ToString());

  const std::array<std::byte, 2> data1 = {std::byte{0x08}, std::byte{0x69}};
  kv::Slice s3(data1.data(), data1.size());
  n1.Put(s3, s3);
  LOG_DEBUG(n1.ToString());
  // why does this write change to string
  kv::PageBuffer buf1{1, kv::OS::DEFAULT_PAGE_SIZE};
  auto &p1 = buf1.GetPage(0);
  n1.Write(p1);
  LOG_DEBUG(n1.ToString());

  kv::Node n2{};
  n2.Read(p1);
  LOG_DEBUG(n2.ToString());

  ASSERT_EQ(n1.ToString(), n2.ToString());
}

TEST(NodeTest, WriteAndReadLeafPagePreservesOrder) {
  kv::Node n{};
  kv::PageBuffer buf{1, kv::OS::DEFAULT_PAGE_SIZE};
  auto &p = buf.GetPage(0);
  p.SetFlags(kv::PageFlag::LeafPage);
  n.Read(p);

  const std::byte k1[] = {std::byte{0x01}};
  const std::byte v1[] = {std::byte{0x0A}};
  const std::byte k2[] = {std::byte{0x02}};
  const std::byte v2[] = {std::byte{0x0B}};
  const std::byte k3[] = {std::byte{0x03}};
  const std::byte v3[] = {std::byte{0x0C}};

  kv::Slice key1(k1, sizeof(k1)), val1(v1, sizeof(v1));
  kv::Slice key2(k2, sizeof(k2)), val2(v2, sizeof(v2));
  kv::Slice key3(k3, sizeof(k3)), val3(v3, sizeof(v3));

  n.Put(key2, val2);
  n.Put(key3, val3);
  n.Put(key1, val1); // inserted out of order on purpose

  n.Write(p);

  kv::Node n2{};
  n2.Read(p);

  const auto &nodes = n2.GetElements();
  ASSERT_EQ(nodes.size(), 3);

  EXPECT_EQ(nodes[0].key_.ToHex(), "01");
  EXPECT_EQ(nodes[0].val_.ToHex(), "0a");
  EXPECT_EQ(nodes[1].key_.ToHex(), "02");
  EXPECT_EQ(nodes[1].val_.ToHex(), "0b");
  EXPECT_EQ(nodes[2].key_.ToHex(), "03");
  EXPECT_EQ(nodes[2].val_.ToHex(), "0c");
}

TEST(NodeTest, RandomByteInsertionsPreserveOrder) {
  constexpr size_t num_keys = 100;

  // Generate random 2-byte keys
  std::mt19937 rng(42); // fixed seed for reproducibility
  std::uniform_int_distribution<std::size_t> dist(0, 0xFFFF);

  std::set<std::vector<std::byte>> unique_keys;

  while (unique_keys.size() < num_keys) {
    std::size_t key_val = dist(rng);
    std::vector<std::byte> key_bytes = {
        static_cast<std::byte>((key_val >> 8) & 0xFF),
        static_cast<std::byte>(key_val & 0xFF)};
    unique_keys.insert(std::move(key_bytes));
  }

  kv::Node n{};
  kv::PageBuffer buf{1, kv::OS::DEFAULT_PAGE_SIZE};
  auto &p = buf.GetPage(0);
  p.SetFlags(kv::PageFlag::LeafPage);
  n.Read(p);

  const std::byte val[] = {std::byte{0xFF}};
  kv::Slice dummy_val(val, sizeof(val));

  for (const auto &key_vec : unique_keys) {
    kv::Slice key(key_vec.data(), key_vec.size());
    n.Put(key, dummy_val);
  }

  n.Write(p);

  kv::Node n2{};
  n2.Read(p);

  const auto &nodes = n2.GetElements();
  ASSERT_EQ(nodes.size(), unique_keys.size());

  for (size_t i = 1; i < nodes.size(); ++i) {
    ASSERT_TRUE(nodes[i - 1].key_ < nodes[i].key_)
        << "Keys not in order at index " << i;
  }
}

} // namespace test
