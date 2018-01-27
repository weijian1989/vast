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

#include "vast/detail/event_sink.hpp"

namespace vast::detail {

// -- constructors, destructors, and assignment operators ----------------------

event_sink::event_sink() : source_(nullptr) {
  // nop
}

event_sink::~event_sink() noexcept {
  // nop
}

// -- member functions ---------------------------------------------------------

void event_sink::init_source_ref(event_source* ptr) {
  VAST_ASSERT(source_ == nullptr);
  VAST_ASSERT(ptr != nullptr);
  source_ = ptr;
}

} // namespace vast::detail
