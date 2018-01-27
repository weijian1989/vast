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

#ifndef VAST_DETAIL_EVENT_SOURCE_HPP
#define VAST_DETAIL_EVENT_SOURCE_HPP

#include "vast/ids.hpp"

#include "vast/detail/assert.hpp"

namespace vast::detail {

class event_sink;

/// A source for VAST events.
class event_source {
public:
  // -- constructors, destructors, and assignment operators --------------------

  event_source();

  virtual ~event_source() noexcept;

  // -- pure virtual member functions ------------------------------------------

  /// Asks the source to materialize `n` more entries.
  virtual void pull(size_t n) = 0;

  /// Asks the source to prepare to load data for the IDs `xs` and returns the
  /// number of newly queried events.
  virtual size_t query(const ids& xs) = 0;

  /// Returns the number of elements that the source could push immediately,
  /// given sufficient credit.
  virtual size_t available() const noexcept = 0;

  /// Returns the number of elements that the source is materializing.
  virtual size_t pending() const noexcept = 0;

  // -- member functions -------------------------------------------------------

  /// Initializes the reference to the source.
  /// @warning Call exactly once after constructing and before using any other
  /// member function.
  void init_sink_ref(event_sink* ptr);

  // -- inline member functions ------------------------------------------------

  /// Returns the next step in the pipeline.
  inline event_sink& sink() {
    VAST_ASSERT(sink_ != nullptr);
    return *sink_;
  }

  /// Returns the next step in the pipeline.
  inline const event_sink& sink() const {
    VAST_ASSERT(sink_ != nullptr);
    return *sink_;
  }

  /// Returns true if data is neither available nor pending.
  inline bool at_end() const noexcept {
    return available() + pending() == 0;
  }

protected:
  // -- member variables -------------------------------------------------------

  event_sink* sink_;
};

} // namespace vast::detail

#endif // VAST_DETAIL_EVENT_SOURCE_HPP
