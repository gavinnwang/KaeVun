#include "slice.h"
#include "gtest/gtest.h"

namespace test {

TEST(SliceTest, Test1) {
  kv::Slice s1{"hi"};
  kv::Slice s2{"abc"};
  ASSERT_TRUE(s1.Compare(s2) > 0);
  ASSERT_TRUE(s2.Compare(s1) < 0);

  kv::Slice s3{"hi"};
  ASSERT_TRUE(s1.Compare(s3) == 0);

  const std::byte data1[] = {std::byte{0x08}, std::byte{0x69}};
  const std::byte data2[] = {std::byte{0x11}, std::byte{0x62}, std::byte{0x63}};
  kv::Slice s4(data1, sizeof(data1));
  kv::Slice s5(data2, sizeof(data2));

  ASSERT_TRUE(s4.Compare(s5) < 0);
  ASSERT_TRUE(s5.Compare(s4) > 0);

  const std::byte data3[] = {std::byte{0x11}, std::byte{0x62}};
  const std::byte data4[] = {std::byte{0x11}, std::byte{0x62}, std::byte{0x63}};
  kv::Slice s6(data3, sizeof(data3));
  kv::Slice s7(data4, sizeof(data4));

  ASSERT_TRUE(s6.Compare(s7) < 0);
  ASSERT_TRUE(s7.Compare(s6) > 0);
}
} // namespace test
