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

#include "vast/print.hpp"

#include <array>
#include <chrono>
#include <iterator>
#include <limits>
#include <type_traits>

#include "caf/detail/scope_guard.hpp"
#include "caf/ip_address.hpp"
#include "caf/ip_subnet.hpp"

#include "vast/address.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/coding.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/pattern.hpp"
#include "vast/port.hpp"
#include "vast/subnet.hpp"
#include "vast/view.hpp"

namespace vast {

namespace {

template <class T>
void print_number(std::string& buf, T x, size_t min_digits = 1) {
  static_assert(std::is_integral_v<T>);
  if (x == 0) {
    buf.insert(buf.end(), min_digits, '0');
    return;
  }
  std::array<char, std::numeric_limits<T>::digits10 + 1> tmp;
  auto ptr = tmp.data();
  auto g = caf::detail::make_scope_guard([&] {
    auto n = detail::narrow_cast<size_t>(std::distance(tmp.data(), ptr));
    if (n < min_digits)
      buf.insert(buf.end(), min_digits - n, '0');
    buf.insert(buf.end(), std::make_reverse_iterator(ptr),
               std::make_reverse_iterator(tmp.data()));
  });
  if constexpr (std::is_signed_v<T>) {
    if (x < 0) {
      buf += '-';
      while (x < 0) {
        *ptr++ = detail::byte_to_char(-(x % 10));
        x /= 10;
      }
      return;
    }
  }
  while (x > 0) {
    *ptr++ = detail::byte_to_char(x % 10);
    x /= 10;
  }
}

template <char Open, char Close, char Fill, class T>
void print_range(std::string& buf, const T& xs) {
  auto print_empty = [&] {
    buf += Open;
    if constexpr (Fill != '\0')
      buf += Fill;
    buf += Close;
  };
  if constexpr (is_container_view_handle<T>::value)
    if (!xs)
      return print_empty();
  auto i = xs.begin();
  auto e = xs.end();
  if (i == e)
    return print_empty();
  buf += Open;
  print(buf, *i);
  for (; i != e; ++i) {
    buf += ", ";
    print(buf, *i);
  }
  buf += Close;
}

template <class To, class R, class P>
bool is_at_least(std::chrono::duration<R, P> x) {
  return std::chrono::duration_cast<To>(std::chrono::abs(x)) >= To{1};
}

template <class To, class R, class P>
auto fractional_count(std::chrono::duration<R, P> x) {
  using fractional = std::chrono::duration<double, typename To::period>;
  return std::chrono::duration_cast<fractional>(x).count();
}

} // namespace

void print(std::string& buf, caf::none_t) {
  buf += "nil";
}

void print(std::string& buf, boolean x) {
  buf += x ? 'T' : 'F';
}

void print(std::string& buf, integer x) {
  print_number(buf, x);
}

void print(std::string& buf, count x) {
  print_number(buf, x);
}

void print(std::string& buf, real x, size_t max_digits) {
  // negative = positive + sign
  if (x < 0) {
    buf += '-';
    x = -x;
  }
  real left;
  uint64_t right = std::round(std::modf(x, &left) * std::pow(10, max_digits));
  print_number(buf, detail::narrow_cast<uint64_t>(left));
  buf += '.';
  // Add leading decimal zeros.
  auto magnitude = right == 0 ? max_digits : std::log10(right);
  for (auto i = 1.0; i < max_digits - magnitude; ++i)
    buf += '0';
  // Chop off trailing zeros of the decimal digits.
  while (right > 0 && right % 10 == 0)
    right /= 10;
  print_number(buf, right);
}

void print(std::string& buf, timespan x) {
  using namespace std::chrono;
  if (is_at_least<days>(x)) {
    print(buf, fractional_count<days>(x), 2);
    buf += 'd';
  } else if (is_at_least<hours>(x)) {
    print(buf, fractional_count<hours>(x), 2);
    buf += 'h';
  } else if (is_at_least<minutes>(x)) {
    print(buf, fractional_count<minutes>(x), 2);
    buf += 'm';
  } else if (is_at_least<seconds>(x)) {
    print(buf, fractional_count<seconds>(x), 2);
    buf += 's';
  } else if (is_at_least<milliseconds>(x)) {
    print(buf, fractional_count<milliseconds>(x), 2);
    buf += "ms";
  } else if (is_at_least<microseconds>(x)) {
    print(buf, fractional_count<microseconds>(x), 2);
    buf += "us";
  } else {
    print(buf, x.count());
    buf += "ns";
  }
}

void print(std::string& buf, timestamp x) {
  // TODO: consider using CAF's builtin printer; breaking change, since VAST
  //       currently prints '2009-11-18+09:00:21.486' while CAF would print
  //       '2009-11-18T09:00:21.486'
  // caf::detail::stringification_inspector f{buf};
  // f(x);
  using namespace std::chrono;
  auto sd = floor<days>(x);
  auto [Y, M, D] = from_days(duration_cast<days>(sd - timestamp{}));
  auto t = x - sd;
  auto h = duration_cast<hours>(t);
  auto m = duration_cast<minutes>(t - h);
  auto s = duration_cast<seconds>(t - h - m);
  auto sub_secs = duration_cast<milliseconds>(t - h - m - s);
  print_number(buf, integer{Y}, 2);
  buf += '-';
  print_number(buf, integer{M}, 2);
  buf += '-';
  print_number(buf, integer{D}, 2);
  buf += '+';
  print_number(buf, integer{h.count()}, 2);
  buf += ':';
  print_number(buf, integer{m.count()}, 2);
  buf += ':';
  print_number(buf, integer{s.count()}, 2);
  buf += '.';
  print_number(buf, integer{sub_secs.count()});
}

void print(std::string& buf, const std::string& x) {
  print(buf, std::string_view{x});
}

void print(std::string& buf, std::string_view x) {
  buf += '"';
  for (auto c : x) {
    switch (c) {
      case '"':
        buf += "\\\"";
        break;
      case '\t':
        buf += "\\t";
        break;
      case '\n':
        buf += "\\n";
        break;
      default:
        buf += c;
    }
  }
  buf += '"';
}

void print(std::string& buf, const pattern& x) {
  buf += '/';
  print(buf, x.string());
  buf += '/';
}

void print(std::string& buf, const pattern_view& x) {
  buf += '/';
  print(buf, x.string());
  buf += '/';
}

void print(std::string& buf, address x) {
  buf += to_string(caf::ip_address{x.data()});
}

void print(std::string& buf, subnet x) {
  caf::ip_subnet tmp{caf::ip_address{x.network().data()}, x.length()};
  buf += to_string(tmp);
}

void print(std::string& buf, const data& x) {
  print(buf, x.get_data());
}

void print(std::string& buf, port x) {
  print(buf, count{x.number()});
  switch (x.type()) {
    case port::tcp:
      buf += "/tcp";
      break;
    case port::udp:
      buf += "/udp";
      break;
    case port::icmp:
      buf += "/icmp";
      break;
    default:
      buf += "/?";
  }
}

void print(std::string& buf, const vector& xs) {
  print_range<'[', ']', '\0'>(buf, xs);
}

void print(std::string& buf, const set& xs) {
  print_range<'{', '}', '\0'>(buf, xs);
}

void print(std::string& buf, const map& xs) {
  print_range<'{', '}', '-'>(buf, xs);
}

void print(std::string& buf, const vector_view_handle& xs) {
  print_range<'[', ']', '\0'>(buf, xs);
}

void print(std::string& buf, const set_view_handle& xs) {
  print_range<'{', '}', '\0'>(buf, xs);
}

void print(std::string& buf, const map_view_handle& xs) {
  print_range<'{', '}', '-'>(buf, xs);
}

} // namespace vast
