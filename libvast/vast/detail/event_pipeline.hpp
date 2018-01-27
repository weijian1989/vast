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

#ifndef VAST_DETAIL_EVENT_PIPELINE_HPP
#define VAST_DETAIL_EVENT_PIPELINE_HPP

#include <caf/detail/type_list.hpp>

#include "vast/ids.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/event_source.hpp"

namespace vast::detail {

/// Convenience helper for managing a pipeline consisting of one source, any
/// number of stages, and one sink.
template <class Source, class Sink>
class event_pipeline {
public:
  // -- constructors, destructors, and assignment operators --------------------

  template <class... Ts>
  event_pipeline(Source& src, Ts&... stages) : source_(&src) {
    init(&src, &stages...);
  }

  event_pipeline(const event_pipeline&) = default;

  // -- member functions -------------------------------------------------------

  /// Asks the source to prepare to load data for the IDs `xs`.
  void query(const ids& xs) {
    sink().source().query(xs);
  }

  /// Allows the sink to pull `num` more elements from the source.
  void add_credit(size_t num) {
    sink().source().pull(num);
  }

  /// Queries whether the source reached the end.
  bool at_end() const noexcept {
    return sink().source().at_end();
  }

  /// Returns the producer of the pipeline.
  Source& source() {
    VAST_ASSERT(source_ != nullptr);
    return *source_;
  }

  /// Returns the producer of the pipeline.
  const Source& source() const {
    VAST_ASSERT(source_ != nullptr);
    return *source_;
  }

  /// Returns the consumer of the pipeline.
  Sink& sink() {
    VAST_ASSERT(sink_ != nullptr);
    return *sink_;
  }

  /// Returns the consumer of the pipeline.
  const Sink& sink() const {
    VAST_ASSERT(sink_ != nullptr);
    return *sink_;
  }

private:
  // -- initialization helper functions ----------------------------------------

  void init(Sink* snk) {
    sink_ = snk;
  }

  template <class T, class... Ts>
  void init(event_source* src, T* stage, Ts*... xs) {
    src->init_sink_ref(stage);
    stage->init_source_ref(src);
    init(stage, xs...);
  }

  // -- member variables -------------------------------------------------------

  Source* source_;
  Sink* sink_;
};

template <class Source, class... Stages>
event_pipeline<Source, typename caf::detail::tl_back<
                         caf::detail::type_list<Stages...>>::type>
make_event_pipeline(Source& src, Stages&... stages) {
  return {src, stages...};
}

} // namespace vast::detail

#endif // VAST_DETAIL_EVENT_PIPELINE_HPP
