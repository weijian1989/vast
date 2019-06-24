/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE print

#include "vast/print.hpp"

#include "vast/test/test.hpp"

using namespace vast;

namespace {

template <class T>
auto tostr(const T& x) {
  std::string result;
  print(result, x);
  return result;
}

struct fixture {};

} // namespace

#define CHECK_TYPE(type, val) CHECK_EQUAL(tostr(type{val}), #val)

FIXTURE_SCOPE(print_tests, fixture)

TEST(print count) {
  CHECK_TYPE(count, 0);
  CHECK_TYPE(count, 1);
  CHECK_TYPE(count, 12);
  CHECK_TYPE(count, 123);
  CHECK_EQUAL(tostr(std::numeric_limits<count>::max()), "18446744073709551615");
}

TEST(print integer) {
  CHECK_TYPE(integer, -123);
  CHECK_TYPE(integer, -12);
  CHECK_TYPE(integer, -1);
  CHECK_TYPE(integer, 0);
  CHECK_TYPE(integer, 1);
  CHECK_TYPE(integer, 12);
  CHECK_TYPE(integer, 123);
  CHECK_EQUAL(tostr(std::numeric_limits<integer>::min()),
              "-9223372036854775808");
  CHECK_EQUAL(tostr(std::numeric_limits<integer>::max()),
              "9223372036854775807");
}

TEST(print real) {
  CHECK_TYPE(real, 0.0);
  CHECK_TYPE(real, 0.005);
  CHECK_TYPE(real, -0.005);
  CHECK_TYPE(real, 1.0);
  CHECK_TYPE(real, -1.0);
  CHECK_TYPE(real, 123.456);
  CHECK_TYPE(real, -123.456);
  CHECK_TYPE(real, 123456.123456789);
  CHECK_TYPE(real, -123456.123456789);
}

TEST(print string) {
  CHECK_TYPE(std::string, "foobar");
}

FIXTURE_SCOPE_END()
