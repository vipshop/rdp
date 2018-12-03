//
//Copyright 2018 vip.com.
//
//Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
//the License. You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
//an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
//specific language governing permissions and limitations under the License.
//

#include "util_test.h"

namespace rdp_comm {

TEST_F(UtilTest, ParentPath) {
  string path = rdp_comm::ParentPath(string("/a/b/c"));
  EXPECT_STREQ("/a/b", path.c_str());
}

TEST_F(UtilTest, CharToUint64) {
  uint64_t x = rdp_comm::CharToInt64("123456");
  EXPECT_EQ(123456ul, x);
}

TEST_F(UtilTest, GetIntervalMs) {
  struct timeval a;
  struct timeval b;
  gettimeofday(&a, NULL);
  memcpy(&b, &a, sizeof(struct timeval));
  b.tv_sec += 1;
  uint64_t x = rdp_comm::GetIntervalMs(&a, &b);
  EXPECT_EQ(x, 1000ul);
}

TEST_F(UtilTest, GetIntervalUs) {
  struct timeval a;
  struct timeval b;
  gettimeofday(&a, NULL);
  memcpy(&b, &a, sizeof(struct timeval));
  b.tv_usec += 100;
  uint64_t x = rdp_comm::GetIntervalUs(&a, &b);
  EXPECT_EQ(x, 100ul);
}

TEST_F(UtilTest, SplitString) {
  vector<string> rs;
  rdp_comm::SplitString(string("abcd"), " ", &rs);
  ASSERT_EQ(1ul, rs.size()) << " Vector Size:" << rs.size();
  ASSERT_STREQ("abcd", rs[0].c_str()) << " Vector:" << rs[0];

  rs.clear();
  rdp_comm::SplitString(string("a/b/d/"), "/", &rs);
  ASSERT_EQ(4ul, rs.size()) << " Vector Size:" << rs.size();
  ASSERT_STREQ("a", rs[0].c_str()) << " Vector:" << rs[0];
  ASSERT_STREQ("b", rs[1].c_str()) << " Vector:" << rs[1];
  ASSERT_STREQ("d", rs[2].c_str()) << " Vector:" << rs[2];
  ASSERT_TRUE(true == rs[3].empty());

  rs.clear();
  rdp_comm::SplitString(string("a/b/d"), "/", &rs);
  ASSERT_EQ(3ul, rs.size()) << " Vector Size:" << rs.size();
  ASSERT_STREQ("a", rs[0].c_str()) << " Vector:" << rs[0];
  ASSERT_STREQ("b", rs[1].c_str()) << " Vector:" << rs[1];
  ASSERT_STREQ("d", rs[2].c_str()) << " Vector:" << rs[2];

  rs.clear();
  rdp_comm::SplitString(string("/a/b/d"), "/", &rs);
  ASSERT_EQ(4ul, rs.size()) << " Vector Size:" << rs.size();
  ASSERT_TRUE(true == rs[0].empty());
  ASSERT_STREQ("a", rs[1].c_str()) << " Vector:" << rs[1];
  ASSERT_STREQ("b", rs[2].c_str()) << " Vector:" << rs[2];
  ASSERT_STREQ("d", rs[3].c_str()) << " Vector:" << rs[3];

  rs.clear();
  rdp_comm::SplitString(string("a//b//d/"), "/", &rs);
  ASSERT_EQ(6ul, rs.size()) << " Vector Size:" << rs.size();
  ASSERT_STREQ("a", rs[0].c_str()) << " Vector:" << rs[0];
  ASSERT_TRUE(true == rs[1].empty());
  ASSERT_STREQ("b", rs[2].c_str()) << " Vector:" << rs[2];
  ASSERT_TRUE(true == rs[3].empty());
  ASSERT_STREQ("d", rs[4].c_str()) << " Vector:" << rs[4];
  ASSERT_TRUE(true == rs[5].empty());

  rs.clear();
  rdp_comm::SplitString(string("//a//b//d//"), "//", &rs);
  ASSERT_EQ(5ul, rs.size()) << " Vector Size:" << rs.size();
  ASSERT_TRUE(true == rs[0].empty());
  ASSERT_STREQ("a", rs[1].c_str()) << " Vector:" << rs[1];
  ASSERT_STREQ("b", rs[2].c_str()) << " Vector:" << rs[2];
  ASSERT_STREQ("d", rs[3].c_str()) << " Vector:" << rs[3]; 
  ASSERT_TRUE(true == rs[4].empty());
}

TEST_F(UtilTest, StrToLower) {
  string rs = rdp_comm::StrToLower(string("ABCDE"));
  EXPECT_STREQ("abcde", rs.c_str());
  rs.clear();
  rs = rdp_comm::StrToLower(string("AbCDe"));
  EXPECT_STREQ("abcde", rs.c_str());
  rs.clear();
  rs = rdp_comm::StrToLower(string("abcde"));
  EXPECT_STREQ("abcde", rs.c_str());
  rs.clear();
  rs = rdp_comm::StrToLower(string("aBcDe"));
  EXPECT_STREQ("abcde", rs.c_str());
}

TEST_F(UtilTest, CompareNoCase) {
  EXPECT_TRUE(true == rdp_comm::CompareNoCase(string("abc"), string("abc")));
  EXPECT_TRUE(true == rdp_comm::CompareNoCase(string("ABC"), string("abc")));
  EXPECT_TRUE(true == rdp_comm::CompareNoCase(string("abc"), string("ABC")));
  EXPECT_TRUE(true == rdp_comm::CompareNoCase(string("ABC"), string("ABC")));
  EXPECT_TRUE(true == rdp_comm::CompareNoCase(string("AbC"), string("ABc")));
  EXPECT_TRUE(false == rdp_comm::CompareNoCase(string("ABd"), string("ABC")));
  EXPECT_TRUE(false == rdp_comm::CompareNoCase(string("ABc"), string("ABd")));
}

} //namespace rdp_comm
