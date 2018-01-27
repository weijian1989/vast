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

#include "caf/timestamp.hpp"

#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/ids.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/query.hpp"
#include "vast/system/index.hpp"

#include "vast/detail/event_pipeline.hpp"
#include "vast/detail/event_sink.hpp"
#include "vast/detail/event_source.hpp"
#include "vast/detail/event_stage.hpp"

#define SUITE query_result
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace vast;
using namespace vast::system;
using namespace vast::detail;
using namespace std::chrono;
using namespace std::string_literals;

namespace {

template <class Iterator>
auto mmi(Iterator i) {
  return std::make_move_iterator(i);
}

using batch = std::vector<event>;

class dummy_source : public event_source {
public:
  dummy_source(batch xs) : demand_(0) {
    for (auto& x : xs) {
      auto id = x.id();
      archive_.emplace(id, std::move(id));
    }
  }

  void pull(size_t num) override {
    demand_ += num;
    push();
  }

  size_t query(const ids& xs) override {
    // Filter already selected IDs from xs.
    auto ys = xs - selected_;
    auto result = rank(ys);
    if (result == 0)
      return 0;
    selected_ |= ys;
    /// Shove newly selected items into the buffer.
    for (auto id : select(ys)) {
      auto i = archive_.find(id);
      if (i != archive_.end())
        buf_.emplace_back(i->second);
    }
    // Try push data to the sink.
    push();
    return result;
  }

  size_t available() const noexcept override {
    return buf_.size();
  }

  size_t pending() const noexcept override {
    return 0;
  }

  inline const ids& selected() const noexcept {
    return selected_;
  }

private:
  void push() {
    auto n = std::min(demand_, buf_.size());
    if (n > 0) {
      MESSAGE("push " << n << " more elements from the source to the sink");
      batch tmp;
      if (n == buf_.size()) {
        tmp.swap(buf_);
      } else {
        tmp.insert(tmp.end(), mmi(buf_.end() - n), mmi(buf_.end()));
        buf_.erase(buf_.end() - n, buf_.end());
      }
      demand_ -= n;
      sink_->push(std::move(tmp));
    }
  }

  std::unordered_map<event_id, event> archive_;
  ids selected_;
  batch buf_;
  size_t demand_ = 0;
};

struct statistics {
  caf::timestamp start;   ///< Timestamp of the first query() operation.
  caf::timestamp fin;     ///< Timestamp of the last push() operation.
  size_t expected = 0;    ///< Expected ID sets from INDEX.
  size_t received = 0;    ///< Received ID sets from INDEX.
  size_t scheduled = 0;   ///< Scheduled partitions (ID sets) at INDEX.
  size_t candidates = 0;  ///< Candidates from ARCHIVE.
  size_t shipped = 0;     ///< Shipped results to sink.
  size_t requested = 0;   ///< User-requested pending results to extract.

  bool completed() const {
    return fin.time_since_epoch().count() != 0;
  }

  /// Returns the runtime in nanoseconds when completed, otherwise returns 0.
  caf::timestamp::duration runtime() const {
    if (completed())
      return fin - start;
    return caf::timestamp::duration{0};
  }
};

class accounting_stage : public event_stage {
public:
  void pull(size_t num) override {
    stats_.requested += num;
    source().pull(num);
  }

  void push(batch&& xs) override {
    stats_.shipped += xs.size();
    sink().push(std::move(xs));
  }

  size_t query(const ids& xs) override {
    if (stats_.start.time_since_epoch().count() == 0)
      stats_.start = caf::make_timestamp();
    stats_.received += 1;
    auto result = source().query(xs);
    stats_.candidates += result;
    return result;
  }

  statistics& stats() {
    return stats_;
  }

private:
  statistics stats_;
};

class dummy_sink : public event_sink {
public:
  dummy_sink() {
    // nop
  }

  void push(batch&& xs) override {
    output_.insert(output_.end(), xs.begin(), xs.end());
  }

  const batch& output() const noexcept {
    return output_;
  }

private:
  batch output_;
};

} // namespace <anonymous>

FIXTURE_SCOPE(query_tests, fixtures::actor_system_and_events)

TEST(materialization) {
  MESSAGE("bro conn log has " << bro_conn_log.size() << " elements");
  dummy_source source{bro_conn_log};
  dummy_sink sink;
  CHECK_EQUAL(source.at_end(), true);
  auto pipe = make_event_pipeline(source, sink);
  CHECK_EQUAL(pipe.at_end(), true);
  pipe.add_credit(10u);
  CHECK_EQUAL(sink.output().size(), 0u);
  pipe.query(make_ids({{10, 40}}));
  CHECK_EQUAL(pipe.at_end(), false);
  CAF_CHECK_EQUAL(source.selected(), make_ids({{10, 40}}));
  CHECK_EQUAL(source.available(), 20u);
  CHECK_EQUAL(sink.output().size(), 10u);
  CHECK_EQUAL(pipe.at_end(), false);
  pipe.add_credit(10u);
  CHECK_EQUAL(source.available(), 10u);
  CHECK_EQUAL(sink.output().size(), 20u);
  CHECK_EQUAL(pipe.at_end(), false);
  pipe.add_credit(10u);
  CHECK_EQUAL(source.available(), 0u);
  CHECK_EQUAL(sink.output().size(), 30u);
  CHECK_EQUAL(pipe.at_end(), true);
}

TEST(materialization with statistics) {
  dummy_source source{bro_conn_log};
  accounting_stage stage;
  dummy_sink sink;
  auto pipe = make_event_pipeline(source, stage, sink);
  CAF_CHECK_EQUAL(stage.stats().start.time_since_epoch().count(), 0u);
  MESSAGE("querying [10, 40) should add 30 candidates");
  pipe.query(make_ids({{10, 40}}));
  CAF_CHECK_NOT_EQUAL(stage.stats().start.time_since_epoch().count(), 0u);
  CAF_CHECK_EQUAL(stage.stats().candidates, 30u);
  CAF_CHECK_EQUAL(stage.stats().shipped, 0u);
  CAF_CHECK_EQUAL(stage.stats().requested, 0u);
  MESSAGE("querying [10, 40) again is a nop");
  pipe.query(make_ids({{10, 40}}));
  CAF_CHECK_EQUAL(stage.stats().candidates, 30u);
  CAF_CHECK_EQUAL(stage.stats().shipped, 0u);
  CAF_CHECK_EQUAL(stage.stats().requested, 0u);
  MESSAGE("have the sink consume up to 100 results");
  pipe.add_credit(100u);
  CAF_CHECK_EQUAL(stage.stats().candidates, 30u);
  CAF_CHECK_EQUAL(stage.stats().shipped, 30u);
  CAF_CHECK_EQUAL(stage.stats().requested, 100u);
}

FIXTURE_SCOPE_END()
