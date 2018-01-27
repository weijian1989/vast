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

#ifndef VAST_DETAIL_EVENT_SINK_HPP
#define VAST_DETAIL_EVENT_SINK_HPP

#include "vast/event.hpp"

#include "vast/detail/assert.hpp"

namespace vast::detail {

class event_source;

/// A sink for batches.
class event_sink {
public:
  // -- constructors, destructors, and assignment operators --------------------

  event_sink();

  virtual ~event_sink() noexcept;

  // -- pure virtual member functions ------------------------------------------

  /// Consumes `xs`.
  virtual void push(std::vector<event>&& xs) = 0;

  // -- member functions -------------------------------------------------------

  /// Initializes the reference to the source.
  /// @warning Call exactly once after constructing and before using any other
  /// member function.
  void init_source_ref(event_source* ptr);

  // -- inline member functions ------------------------------------------------

  /// Returns the previous step in the pipeline.
  inline event_source& source() {
    VAST_ASSERT(source_ != nullptr);
    return *source_;
  }

  /// Returns the previous step in the pipeline.
  inline const event_source& source() const {
    VAST_ASSERT(source_ != nullptr);
    return *source_;
  }

protected:
  event_source* source_;
};

} // namespace vast::detail

#endif // VAST_DETAIL_EVENT_SINK_HPP
