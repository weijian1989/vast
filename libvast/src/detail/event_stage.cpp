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

#include "vast/detail/event_stage.hpp"

namespace vast::detail {

// -- constructors, destructors, and assignment operators ----------------------

event_stage::~event_stage() noexcept {
  // nop
}

// -- default implementations --------------------------------------------------

void event_stage::pull(size_t n) {
  source().pull(n);
}

size_t event_stage::query(const ids& xs) {
  return source().query(xs);
}

size_t event_stage::available() const noexcept {
  return source().available();
}

size_t event_stage::pending() const noexcept {
  return source().pending();
}

void event_stage::push(std::vector<event>&& xs) {
  sink().push(std::move(xs));
}

} // namespace vast::detail
