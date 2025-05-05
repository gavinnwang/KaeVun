#include "slice.h"
#include "gtest/gtest.h"

namespace test {

TEST(SliceTest, StrongOrdering) {
  kv::Slice s1{"hi"};
  kv::Slice s2{"abc"};

  ASSERT_TRUE(s1 > s2);
  ASSERT_TRUE(s2 < s1);

  kv::Slice s3{"hi"};
  ASSERT_TRUE(s1 == s3);

  const std::byte data1[] = {std::byte{0x08}, std::byte{0x69}};
  const std::byte data2[] = {std::byte{0x11}, std::byte{0x62}, std::byte{0x63}};
  kv::Slice s4(data1, sizeof(data1));
  kv::Slice s5(data2, sizeof(data2));

  ASSERT_TRUE(s4 < s5);
  ASSERT_TRUE(s5 > s4);

  const std::byte data3[] = {std::byte{0x11}, std::byte{0x62}};
  const std::byte data4[] = {std::byte{0x11}, std::byte{0x62}, std::byte{0x63}};
  kv::Slice s6(data3, sizeof(data3));
  kv::Slice s7(data4, sizeof(data4));

  ASSERT_TRUE(s6 < s7);
  ASSERT_TRUE(s7 > s6);
}
} // namespace test
