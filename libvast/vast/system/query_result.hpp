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

#ifndef VAST_SYSTEM_QUERY_RESULT_HPP
#define VAST_SYSTEM_QUERY_RESULT_HPP

#include "vast/system/archive.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/query.hpp"
#include "vast/system/index.hpp"

namespace vast::system {

/// Lazily materializes data from the backend. Implements event-driven,
/// on-the-fly iteration of a query result. The query result pulls data from
/// its source and pushes to its sink. The sink controls the amount of data it
/// receives by calling `grant_credit`. The result can receive more IDs at
/// runtime until it is properly sealed.
template <class Policy>
class query_result {
public:
  static constexpr size_t max_cursor_cache_size = 100;

  /// Denotes the lifetime of a cursor.
  enum class state {
    pending,  ///< Source and sink are active and New results can still arrive.
    sealed,   ///< Source is done but the sink is not.
    finalized ///< Both source and sink are done.
  };

  template <class... Ts>
  query_result(Ts&&... xs) : policy_(std::forward<Ts...>(xs)...) {
    // nop
  }

  /// Adds new IDs to the result set.
  void add_ids(const bitmap& xs) {
    VAST_ASSERT(!sealed());
    hits_ |= xs;
    pull();
  }

  /// Seals the result, i.e., singalizes that the source finished and no longer
  /// calls `add_ids`.
  void seal() {
    state_ = state::sealed;
  }

  /// Returns whether the result has an active source.
  inline bool pending() const {
    return state_ == state::pending;
  }

  /// Returns whether the source is done.
  inline bool sealed() const {
    return state_ == state::sealed;
  }

  /// Returns whether both source and sink are done.
  inline bool finalized() const {
    return state_ == state::finalized;
  }

  /// Materializes new data. Called from the backend.
  /// @pre `pending() == true`.
  /// @pre `first != last`.
  template <class Iter>
  void materialize(Iter first, Iter last) {
    VAST_ASSERT(!finalized());
    bitmap mask;
    for (; first != last; ++first) {
      // We remove the event from the candidate set regardless of hit or not.
      if (push(*first)) {
        mask.append_bits(false, first->id() - mask.size());
        mask.append_bit(true);
      }
    }
    // Remove received IDs from unprocessed hits and try to push to the sink.
    hits_ -= mask;
    push();
  }

  /// Asks the query result to push `amount` more items to the sink.
  /// @pre `amount > 0`
  void fetch_more(size_t amount) {
    VAST_ASSERT(amount > 0);
    credit_ += amount;
    pull(amount);
  }

private:
  /// Pulls more data from the source.
  /// @pre `credit > 0`
  void pull() {
    policy_.pull(*this);
  }

  /// Pulls more data from the source and granting new credit to the sink.
  /// @pre `credit > 0`
  void pull(size_t new_credit) {
    policy_.pull(*this, new_credit);
  }

  /// Pushes new data from the cache into the sink.
  /// @pre `cache.empty() == false`
  bool push(event x) {
    return policy_.push(*this, std::move(x));
  }


  /// Pushes new data from the cache into the sink.
  /// @pre `cache.empty() == false`
  void push() {
    policy_.push(*this);
  }

  /// Informs the sink that it reached the end.
  void finalize() {
    state_ = state::finalized;
    policy_.finalize(*this);
  }

  /// Returns whether `x` passes a candidate check.
  bool selected(const event& x) const {
    return policy_.selected(*this, x);
  }

  /// Finalizes the query result if it delivered everything to the sink.
  void try_finalize() {
    if (sealed() && hits_.empty() && cache_.empty())
      finalize();
  }

  /// Keeps track of sink and source status.
  state state_ = pending;

  /// All result IDs.
  bitmap hits_;

  /// Amount of events we are allowed to send to the sink right away.
  size_t credit_ = 0;

  /// Implements the behavior of `pull`, `push`, `finalize`, and `selected`.
  Policy policy_;
};

} // namespace vast::system

#endif // VAST_SYSTEM_QUERY_RESULT_HPP
