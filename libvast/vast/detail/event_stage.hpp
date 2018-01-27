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

#ifndef VAST_DETAIL_EVENT_STAGE_HPP
#define VAST_DETAIL_EVENT_STAGE_HPP

#include "vast/ids.hpp"

#include "vast/detail/event_sink.hpp"
#include "vast/detail/event_source.hpp"

namespace vast::detail {

class event_stage : public event_source, public event_sink {
public:
  // -- constructors, destructors, and assignment operators --------------------

  ~event_stage() noexcept override;

  // -- default implementations ------------------------------------------------

  void pull(size_t n) override;

  size_t query(const ids& xs) override;

  size_t available() const noexcept override;

  size_t pending() const noexcept override;

  void push(std::vector<event>&& xs) override;
};

} // namespace vast::detail

#endif // VAST_DETAIL_EVENT_STAGE_HPP
