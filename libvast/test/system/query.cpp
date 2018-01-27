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

#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/query.hpp"
#include "vast/system/index.hpp"

#define SUITE query
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;
using namespace std::chrono;
using namespace std::string_literals;

FIXTURE_SCOPE(query_tests, fixtures::actor_system_and_events)

/// Base class for historic and continuous queries.
class query {

};

/*
class query_result_policy {
  bool selected(query_result_cursor&, const event&) const;

  void push(query_result_cursor&);

  void pull(query_result_cursor&);
};
*/

/// Implements event-driven, on-the-fly iteration of a query result. The cursor
/// pulls data from its source and pushes to its sink. The sink controls the
/// amount of data it receives by calling `grant_credit`. The cursor can
/// receive more result IDs at runtime until it is properly sealed.
template <class Policy>
class query_result_cursor {
public:
  static constexpr size_t max_cursor_cache_size = 100;

  /// Denotes the lifetime of a cursor.
  enum class state {
    active,   ///< The cursor can receive demand and new results.
    sealed,   ///< No more results can get added to the cursor.
    finalized ///< The cursor has called finalize() and can be destroyed.
  };

  template <class... Ts>
  query_result_cursor(Ts&&... xs) : policy_(std::forward<Ts...>(xs)...) {
    // nop
  }

  virtual ~query_result_cursor() {
    // nop
  }

  /// Adds new IDs to the result set.
  void add_ids(const bitmap& xs) {
    VAST_ASSERT(!sealed());
    hits_ |= xs;
    pull();
  }

  ///
  void seal() {
    state_ = state::sealed;
  }

  inline bool active() const {
    return state_ == state::active;
  }

  inline bool sealed() const {
    return state_ == state::sealed;
  }

  inline bool finalized() const {
    return state_ == state::finalized;
  }

  /// Returns the number of cached events.
  inline size_t cached() const {
    return cache_.size();
  }

  /// Pushes new data into the cache.
  /// @pre `!finalized()`.
  template <class Iter>
  void push(Iter first, Iter last) {
    VAST_ASSERT(!finalized());
    bitmap mask;
    for (; first != last; ++first) {
      // We remove the event from the candidate set regardless of hit or not.
      mask.append_bits(false, first->id() - mask.size());
      mask.append_bit(true);
      // Only append to the cache if the policy gives green light.
      if (selected(*first))
        cache_.emplace_back(*first);
    }
    // Remove received IDs from unprocessed hits and try to push to the sink.
    hits_ -= mask;
    push();
  }

  /// Increases demand and calls `push`.
  /// @pre `amount > 0`
  void grant_credit(size_t amount) {
    VAST_ASSERT(amount > 0);
    credit_ += amount;
    pull();
  }

  /// Resets the credit count to 0 and returns its previous value.
  size_t fetch_credit() {
    auto result = credit_;
    credit_ = 0;
    return credit_;
  }

  /// Fetches more data from the archive.
  void fetch_more() {
    if (cache.size() >= max_cursor_cache_size | unprocessed.empty())
      return;
  }

  inline std::vector<event>& cache() {
    return cache_;
  }

private:
  /// Pulls more data from the source.
  /// @pre `credit > 0`
  void pull() {
    policy_.pull(*this);
  }

  /// Pushes new data from the cache into the sink.
  /// @pre `cache.empty() == false`
  void push() {
    policy_.push(*this);
  }

  /// Informs the sink that it reached the end.
  void finalize() {
    policy_.finalize(*this);
  }

  ///
  bool selected(const event& x) const {
    return policy_.selected(*this, x);
  }

  void try_finalize() {
    if (state_ == sealed && hits_.empty() && cache_.empty()) {
      state_ = finalized;
      finalize();
    }
  }

  state state_ = active;

  /// All result IDs.
  bitmap hits_;

  /// Cached data.
  std::vector<event> cache_;

  /// Amount of events we are allowed to send to the sink right away.
  size_t credit_ = 0;

  /// Implements the behavior of `pull`, `push`, `finalize`, and `selected`.
  Policy policy_;
};

class test_query_result_cursor {
public:
  using buffer = std::vector<event>;

  test_query_result_cursor(buffer input) : input(std::move(input_)) {
    pos_ = input_.begin();
  }

  bool selected(query_result_cursor&, const event&) const {
    return true;
  }

  void push(query_result_cursor& qrc) {
    auto mmi = [](auto i) { return std::make_move_iterator(i); };
    auto& xs = qrc.cache();
    output_.insert(output_.end(), mmi(xs.begin()), mmi(xs.end()));
    xs.clear();
  }

  void pull(query_result_cursor& qrc) {
    auto remaining = static_cast<size_t>(std::distance(pos_, input_.end()));
    auto n = std::min(remaining, qrc.fetch_credit());
    if (n == 0)
      return;
    qrc.push(pos_, pos_ + n);
    pos_ += n;
    if (pos_ == input_.end())
      qrc.seal();
  }

  const buffer& input() const {
    return input_;
  }

  const buffer& output() const {
    return output_;
  }

private:
  buffer input_;
  buffer::iterator pos_;
  buffer output_;
}

/// A historic query represents an immutable snapshot in time. When started,
/// the query will send its expression to the index. Then it waits for hits and
/// performs candidate checks when receiving them. The query becomes sealed
/// after receiving all hits. Once sealed, the query only responds to cursor
/// requests.
class historic_query {
public:
  // Received from the index after sending it the expression.
  void new_hit(bitmap& xs) {
    timespan runtime = steady_clock::now() - start;
    stats.runtime = runtime;
    auto count = rank(xs);
    if (accountant) {
      if (xs.empty())
        self->send(accountant, "query.hits.first", runtime);
      self->send(accountant, "query.hits.arrived", runtime);
      self->send(accountant, "query.hits.count", count);
    }
    VAST_DEBUG(self_, "got", count, "index hits",
               (count == 0 ? "" : ("in ["s + to_string(select(xs, 1)) + ','
                                   + to_string(select(xs, -1) + 1) + ')')));
    if (count > 0) {
      hits |= xs;
      unprocessed |= xs;
      VAST_DEBUG(self, "forwards hits to archive");
      // FIXME: restrict according to configured limit.
      self->request(archive, infinite, std::move(xs)).then(
        [this](std::vector<event>& candidates) {
          VAST_DEBUG(self, "got batch of", candidates.size(), "events");
          bitmap mask;
          for (auto& candidate : candidates) {
            auto& checker = checkers[candidate.type()];
            // Construct a candidate checker if we don't have one for this type.
            if (is<vast::none>(checker)) {
              auto x = tailor(expr, candidate.type());
              if (!x) {
                VAST_ERROR(self, "failed to tailor expression:",
                           self->system().render(x.error()));
                ship_results();
                self->send_exit(self, exit_reason::normal);
                return;
              }
              checker = std::move(*x);
              VAST_DEBUG(self, "tailored AST to", candidate.type() << ':', checker);
            }
            // Perform candidate check and keep event as result on success.
            if (visit(event_evaluator{candidate}, checker))
              results.push_back(std::move(candidate));
            else
              VAST_DEBUG(self, "ignores false positive:", candidate);
            mask.append_bits(false, candidate.id() - mask.size());
            mask.append_bit(true);
          }
          stats.processed += candidates.size();
          unprocessed -= mask;
          ship_results();
          request_more_hits();
          if (stats.received == stats.expected)
            shutdown();
        }
      );
    }
    // Figure out if we're done.
    ++stats.received;
    self->send(sink, id, stats);
    if (stats.received < stats.expected) {
      VAST_DEBUG(self, "received", stats.received << '/'
                                   << stats.expected, "bitmaps");
      request_more_hits();
    } else {
      VAST_DEBUG(self, "received all", stats.expected,
                 "bitmap(s) in", runtime);
      if (accountant)
        self->send(accountant, "query.hits.runtime", runtime);
      shutdown();
    }
  }

  // Received from the archive after sending it IDs of potential hits.

  void ship_results() {
    if (results.empty() || stats.requested == 0)
      return;
    VAST_DEBUG(self, "relays", results.size(), "events");
    message msg;
    if (results.size() <= stats.requested) {
      stats.requested -= results.size();
      stats.shipped += results.size();
      msg = make_message(std::move(results));
    } else {
      std::vector<event> remainder;
      remainder.reserve(results.size() - stats.requested);
      auto begin = results.begin() + stats.requested;
      auto end = results.end();
      std::move(begin, end, std::back_inserter(remainder));
      results.resize(stats.requested);
      msg = make_message(std::move(results));
      results = std::move(remainder);
      stats.shipped += stats.requested;
      stats.requested = 0;
    }
    self->send(sink, msg);
  }

  void request_more_hits() {
    auto waiting_for_hits = stats.received == stats.scheduled;
    auto need_more_results = stats.requested > 0;
    auto have_no_inflight_requests = any<1>(unprocessed);
    // If we're (1) no longer waiting for index hits, (2) still need more
    // results, and (3) have no inflight requests to the archive, we ask
    // the index for more hits.
    if (waiting_for_hits && need_more_results && have_no_inflight_requests) {
      auto remaining = stats.expected - stats.received;
      // TODO: Figure out right amount of partitions to ask for.
      auto n = std::min(remaining, size_t{2});
      VAST_DEBUG(self, "asks index to process", n, "more partitions");
      self->send(index, id, n);
    }
  }

  void shutdown() {
    if (rank(unprocessed) > 0 || !results.empty())
      return;
    timespan runtime = steady_clock::now() - start;
    stats.runtime = runtime;
    VAST_DEBUG(self, "completed in", runtime);
    self->send(sink, id, stats);
    if (accountant) {
      auto num_hits = rank(hits);
      auto processed = stats.processed;
      auto shipped = stats.shipped;
      auto num_results = shipped + results.size();
      auto selectivity = double(num_results) / num_hits;
      self->send(accountant, "exporter.hits", num_hits);
      self->send(accountant, "exporter.processed", processed);
      self->send(accountant, "exporter.results", num_results);
      self->send(accountant, "exporter.shipped", shipped);
      self->send(accountant, "exporter.selectivity", selectivity);
      self->send(accountant, "exporter.runtime", runtime);
    }
    self->send_exit(self, exit_reason::normal);
  }

  void init() {
    VAST_INFO(self, "executes query", expr);
    start = steady_clock::now();
    self->request(index, infinite, expr).then(
      [=](const uuid& lookup, size_t partitions, size_t scheduled) {
        VAST_DEBUG(self, "got lookup handle", lookup << ", scheduled",
                   scheduled << '/' << partitions, "partitions");
        id = lookup;
        if (partitions > 0) {
          stats.expected = partitions;
          stats.scheduled = scheduled;
        } else {
          shutdown();
        }
        self->become(
          [=](extract_atom) {
            if (stats.requested == max_events) {
              VAST_WARNING(self, "ignores extract request, already getting all");
              return;
            }
            stats.requested = max_events;
            ship_results();
            request_more_hits();
          });
      },
      [=](const error&) {
        VAST_DEBUG(self, "failed to lookup query at index:",
                   self->system().render(e));
      }
    );
    execute();
  }

  // The query only responds to `extract` and `get` atoms after sealing it.
  void seal() {
    self->set_default_handler(print_and_drop);
    self->become(
      [this](system::get_atom) {
        return hits;
      });
  }

  // Starts running the query by triggering messages to the index.
  void execute() {
    VAST_INFO(self, "executes query", expr);
    self->unbecome();
    start = steady_clock::now();
    self->request(index, infinite, expr).then(
      [=](const uuid& lookup_id, size_t partitions, size_t scheduled) {
        VAST_DEBUG(self, "got lookup handle", lookup << ", scheduled",
                   scheduled << '/' << partitions, "partitions");
        if (partitions == 0) {
          shutdown();
          return;
        }
        id = lookup_id;
        stats.expected = partitions;
        stats.scheduled = scheduled;
        self->become([this](bitmap& xs) { new_hit(xs); });
      },
      [=](const error&) {
        VAST_DEBUG(self, "failed to lookup query at index:",
                   self->system().render(e));
      }
    );
  }

  expression expr;
  archive_type archive;
  caf::actor index;
  caf::actor sink;
  accountant_type accountant;
  bitmap hits;
  bitmap unprocessed;
  std::unordered_map<type, expression> checkers;
  std::vector<event> results;
  std::chrono::steady_clock::time_point start;
  query_statistics stats;
  uuid id;
  event_based_actor* self;

  static inline const char* name = "query";
};

behavior query_actor(stateful_actor<historic_query>* self, expression expr,
                     query_options) {
  self->set_default_handler(skip);
  self->state.self = self;
  self->state.expr = std::move(expr);
  auto fetch_run = [self](system::run_atom) {
    self->state.init();
  };
  auto fetch_sink = [=](system::sink_atom, actor& x) {
    self->state.sink = std::move(x);
    self->become(fetch_run);
  };
  auto fetch_index = [=](system::index_atom, actor& x) {
    self->state.index = std::move(x);
    self->become(fetch_sink);
  };
  auto fetch_archive = [=](system::archive_atom, archive_type& x) {
    self->state.archive = std::move(x);
    self->become(fetch_index);
  };
  return {fetch_archive};
}

TEST(query) {
  auto i = self->spawn(system::index, directory / "index", 1000, 5, 5);
  auto a = self->spawn(system::archive, directory / "archive", 1, 1024);
  MESSAGE("ingesting conn.log");
  self->send(i, bro_conn_log);
  self->send(a, bro_conn_log);
  auto expr = to<expression>("service == \"http\" && :addr == 212.227.96.110");
  REQUIRE(expr);
  MESSAGE("issueing query");
  auto e = self->spawn(query_actor, *expr, historical);
  self->send(e, system::archive_atom::value, a);
  self->send(e, system::index_atom::value, i);
  self->send(e, system::sink_atom::value, self);
  self->send(e, system::run_atom::value);
  self->send(e, system::extract_atom::value);
  MESSAGE("waiting for results");
  std::vector<event> results;
  self->do_receive(
    [&](std::vector<event>& xs) {
      std::move(xs.begin(), xs.end(), std::back_inserter(results));
    },
    error_handler()
  ).until([&] { return results.size() == 28; });
  MESSAGE("sanity checking result correctness");
  CHECK_EQUAL(results.front().id(), 105u);
  CHECK_EQUAL(results.front().type().name(), "bro::conn");
  CHECK_EQUAL(results.back().id(), 8354u);
  self->send_exit(i, exit_reason::user_shutdown);
  self->send_exit(a, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
