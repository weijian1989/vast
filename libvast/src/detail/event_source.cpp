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

#include "vast/detail/event_source.hpp"

namespace vast::detail {

// -- constructors, destructors, and assignment operators ----------------------

event_source::event_source() : sink_(nullptr) {
  // nop
}

event_source::~event_source() noexcept {
  // nop
}

// -- member functions ---------------------------------------------------------

void event_source::init_sink_ref(event_sink* ptr) {
  VAST_ASSERT(sink_ == nullptr);
  VAST_ASSERT(ptr != nullptr);
  sink_ = ptr;
}

} // namespace vast::detail
