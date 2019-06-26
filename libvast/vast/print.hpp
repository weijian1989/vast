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

#include <string>

#include <caf/none.hpp>
#include <caf/variant.hpp>

#include "vast/aliases.hpp"
#include "vast/fwd.hpp"
#include "vast/time.hpp"

namespace vast {

void print(std::string& buf, caf::none_t x);

void print(std::string& buf, boolean x);

void print(std::string& buf, integer x);

void print(std::string& buf, count x);

void print(std::string& buf, real x, size_t max_digits = 10);

void print(std::string& buf, timespan x);

void print(std::string& buf, timestamp x);

void print(std::string& buf, const std::string& x);

void print(std::string& buf, std::string_view x);

void print(std::string& buf, const pattern& x);

void print(std::string& buf, const pattern_view& x);

void print(std::string& buf, address x);

void print(std::string& buf, subnet x);

void print(std::string& buf, port x);

void print(std::string& buf, const data& x);

void print(std::string& buf, const vector& xs);

void print(std::string& buf, const set& xs);

void print(std::string& buf, const map& xs);

void print(std::string& buf, const vector_view_handle& xs);

void print(std::string& buf, const set_view_handle& xs);

void print(std::string& buf, const map_view_handle& xs);

template <class T, class U>
void print(std::string& buf, const std::pair<T, U>& x) {
  print(buf, x.first);
  buf += " -> ";
  print(buf, x.second);
}

template <class... Ts>
void print(std::string& buf, const caf::variant<Ts...>& x) {
  auto f = [&](const auto& x) {
    print(buf, x);
  };
  caf::visit(f, x);
}

} // namespace vast
