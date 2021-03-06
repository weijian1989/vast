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
#include <type_traits>

#include "vast/concept/printable/print.hpp"

namespace vast {

template <class From, class... Opts>
auto to_string(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       is_printable_v<
          std::back_insert_iterator<std::string>, std::decay_t<From>
        >,
       std::string
     > {
  std::string str;
  print(std::back_inserter(str), from, std::forward<Opts>(opts)...);
  return str;
}

} // namespace vast

