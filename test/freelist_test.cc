#include "freelist.h"
#include "os.h"
#include "page.h"
#include "gtest/gtest.h"

namespace test {

TEST(FreelistTest, PersistTest) {
  kv::PageBuffer buf{1, kv::OS::DEFAULT_PAGE_SIZE};

  auto &p = buf.GetPage(0);
  p.SetId(kv::FREELIST_PAGE_ID);
  p.SetFlags(kv::PageFlag::FreelistPage);
  kv::Freelist f;
  kv::Page p1{};
  p1.SetId(12);
  p1.SetOverflow(1);

  kv::Page p2{};
  p2.SetId(9);

  kv::Page p3{};
  p3.SetId(39);

  f.Free(100, p1);
  f.Free(100, p2);
  f.Free(102, p3);
  f.Release(100);
  f.Release(102);

  std::vector<kv::Pgid> v{9, 12, 13, 39};
  EXPECT_EQ(f.All(), v);

  f.Write(p);
  ASSERT_EQ(p.Id(), kv::FREELIST_PAGE_ID);
  ASSERT_EQ(p.Flags(), static_cast<uint32_t>(kv::PageFlag::FreelistPage));
  ASSERT_EQ(p.Count(), 4);

  kv::Freelist f1{};
  f1.Read(p);

  EXPECT_EQ(f1.All(), v);
}
} // namespace test
