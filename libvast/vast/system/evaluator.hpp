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

#include <unordered_map>
#include <vector>

#include <caf/fwd.hpp>
#include <caf/typed_response_promise.hpp>

#include "vast/expression.hpp"
#include "vast/ids.hpp"

namespace vast::system {

/// @relates evaluator
struct evaluator_state {
  /// Stores hits for the expression.
  ids hits;

  /// Stores hits per predicate in the expression.
  std::unordered_map<predicate, ids> sub_hits;

  /// Stores the number of requests that did not receive a response yet.
  size_t pending_responses = 0;

  /// Allows delaying the response until we could collect all INDEXER results.
  caf::typed_response_promise<ids> promise;

  /// Gives this actor a recognizable name in logging output.
  static inline const char* name = "evaluator";
};

/// Wraps a query expression in an actor. Upon receiving hits from INDEXER
/// actors, re-evaluates the expression and relays new hits to its sinks.
caf::behavior evaluator(caf::stateful_actor<evaluator_state>* self,
                        std::vector<caf::actor> indexers);

} // namespace vast::system
