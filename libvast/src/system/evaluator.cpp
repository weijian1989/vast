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

#include "vast/system/evaluator.hpp"

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"

namespace vast::system {

namespace {

/// Concatenates IDs according to given predicates. In paticular, resolves
/// conjunctions, disjunctions, and negations.
struct ids_evaluator {
  ids_evaluator(const std::unordered_map<predicate, ids>& xs) : xs{xs} {
    // nop
  }

  ids operator()(caf::none_t) const {
    return {};
  }

  ids operator()(const conjunction& c) const {
    auto result = caf::visit(*this, c[0]);
    if (result.empty() || all<0>(result))
      return {};
    for (size_t i = 1; i < c.size(); ++i) {
      result &= caf::visit(*this, c[i]);
      if (result.empty() || all<0>(result)) // short-circuit
        return {};
    }
    return result;
  }

  ids operator()(const disjunction& d) const {
    ids result;
    for (auto& op : d) {
      result |= caf::visit(*this, op);
      if (all<1>(result)) // short-circuit
        break;
    }
    return result;
  }

  ids operator()(const negation& n) const {
    auto result = caf::visit(*this, n.expr());
    result.flip();
    return result;
  }

  ids operator()(const predicate& pred) const {
    auto i = xs.find(pred);
    return i != xs.end() ? i->second : ids{};
  }

  const std::unordered_map<predicate, ids>& xs;
};

} // namespace

caf::behavior evaluator(caf::stateful_actor<evaluator_state>* self,
                        std::vector<caf::actor> indexers) {
  // Skip incoming queries when already processing one.
  return {[=](const expression& expr) -> caf::result<ids> {
    // TODO: we might want to locate the smallest subset of indexers (checking
    //       whether the predicate could match the type of the indexer) instead
    //       of always querying all indexers.
    auto predicates = caf::visit(predicatizer{}, expr);
    if (predicates.empty()) {
      VAST_DEBUG(self, "could not generate any predicates from expression");
      return ids{};
    }
    self->state.promise = self->make_response_promise<ids>();
    for (auto& x : indexers)
      for (auto& pred : predicates) {
        ++self->state.pending_responses;
        self->request(x, caf::infinite, pred)
          .then(
            [=](const ids& hits) {
              auto& st = self->state;
              st.sub_hits.emplace(std::move(pred), std::move(hits));
              auto expr_hits = caf::visit(ids_evaluator{self->state.sub_hits},
                                          expr);
              st.hits |= hits;
              // We're done with evaluation if all INDEXER actors have reported
              // their hits.
              if (--st.pending_responses == 0) {
                VAST_DEBUG(self, "completed expression evaluation");
                st.promise.deliver(std::move(hits));
              }
            },
            [=]([[maybe_unused]] caf::error& err) {
              VAST_ERROR(self, "received an INDEXER error:",
                         self->system().render(err));
              // We don't abort the entire query and always try to produce at
              // least some result.
              auto& st = self->state;
              if (--st.pending_responses == 0)
                VAST_DEBUG(self, "completed expression evaluation");
                st.promise.deliver(std::move(st.hits));
            });
      }
    // We can only deal with exactly one expression at the moment.
    self->unbecome();
  return self->state.promise;
  }};
}

} // namespace vast::system
