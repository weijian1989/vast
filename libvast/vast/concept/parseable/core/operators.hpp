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

#pragma once

#include <type_traits>

#include "vast/concept/parseable/core/to_parser.hpp"

namespace vast {

template <class>
class and_parser;

template <class>
class maybe_parser;

template <class>
class not_parser;

template <class>
class optional_parser;

template <class>
class kleene_parser;

template <class>
class plus_parser;

template <class, class>
class difference_parser;

template <class, class>
class list_parser;

template <class, class>
class sequence_parser;

template <class, class>
class choice_parser;

//
// Unary
//

template <class T>
constexpr auto operator&(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      and_parser<std::decay_t<T>>> {
  return and_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator!(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      not_parser<std::decay_t<T>>> {
  return not_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator-(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      optional_parser<std::decay_t<T>>> {
  return optional_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator*(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      kleene_parser<std::decay_t<T>>> {
  return kleene_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator+(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      plus_parser<std::decay_t<T>>> {
  return plus_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator~(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      maybe_parser<std::decay_t<T>>> {
  return maybe_parser<std::decay_t<T>>{std::forward<T>(x)};
}

//
// Binary
//

template <class LHS, class RHS>
constexpr auto operator-(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<difference_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
constexpr auto operator%(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<list_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
constexpr auto operator>>(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<sequence_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
constexpr auto operator|(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<choice_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

} // namespace vast
