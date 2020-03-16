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

#include "vast/system/index.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/cache.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/ids.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/index.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/query_supervisor.hpp"
#include "vast/system/spawn_indexer.hpp"
#include "vast/system/task.hpp"
#include "vast/table_slice.hpp"

#include <caf/all.hpp>
#include <caf/detail/unordered_flat_map.hpp>

#include <chrono>
#include <deque>
#include <unordered_set>

using namespace std::chrono;

namespace vast::system {

namespace {

auto make_index_stage(index_state* st) {
  using impl = detail::notifying_stream_manager<indexer_stage_driver>;
  auto result = caf::make_counted<impl>(st->self);
  result->continuous(true);
  return result;
}

} // namespace

partition_ptr index_state::partition_factory::operator()(const uuid& id) const {
  // The factory must not get called for the active partition nor for
  // partitions that are currently unpersisted.
  VAST_ASSERT(st_->active == nullptr || id != st_->active->id());
  VAST_ASSERT(std::none_of(st_->unpersisted.begin(), st_->unpersisted.end(),
                           [&](auto& kvp) { return kvp.first->id() == id; }));
  // Load partition from disk.
  VAST_DEBUG(st_->self, "loads partition", id);
  auto result = std::make_unique<partition>(st_, id, st_->max_partition_size);
  if (auto err = result->init())
    VAST_ERROR(st_->self, "unable to load partition state from disk:", id);
  return result;
}

index_state::index_state(caf::stateful_actor<index_state>* self)
  : self(self),
    factory(spawn_indexer),
    lru_partitions(10, partition_lookup{}, partition_factory{this}) {
  // nop
}

index_state::~index_state() {
  VAST_VERBOSE(self, "tearing down");
  flush_to_disk();
}

caf::error index_state::init(const path& dir, size_t max_partition_size,
                             uint32_t in_mem_partitions,
                             uint32_t taste_partitions) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(max_partition_size),
             VAST_ARG(in_mem_partitions), VAST_ARG(taste_partitions));
  put(meta_idx.factory_options(), "max-partition-size", max_partition_size);
  // Set members.
  this->dir = dir;
  this->max_partition_size = max_partition_size;
  this->lru_partitions.size(in_mem_partitions);
  this->taste_partitions = taste_partitions;
  if (auto a = self->system().registry().get(accountant_atom::value)) {
    namespace defs = defaults::system;
    this->accountant = caf::actor_cast<accountant_type>(a);
    self->send(this->accountant, announce_atom::value, "index");
    self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
  }
  // Read persistent state.
  if (auto err = load_from_disk())
    return err;
  // Spin up the stream manager.
  stage = make_index_stage(this);
  return caf::none;
}

caf::error index_state::load_from_disk() {
  VAST_TRACE("");
  // Nothing to load is not an error.
  if (!exists(dir)) {
    VAST_DEBUG(self, "found no directory to load from");
    return caf::none;
  }
  if (auto fname = statistics_filename(); exists(fname)) {
    VAST_VERBOSE(self, "loading statistics from", fname);
    if (auto err = load(&self->system(), fname, stats)) {
      VAST_ERROR(self,
                 "failed to load statistics:", self->system().render(err));
      return err;
    }
    VAST_DEBUG(self, "loaded statistics");
  }
  if (auto fname = meta_index_filename(); exists(fname)) {
    VAST_VERBOSE(self, "loading meta index from", fname);
    if (auto err = load(&self->system(), fname, meta_idx)) {
      VAST_ERROR(self, "failed to load meta index:",
                 self->system().render(err));
      return err;
    }
    VAST_DEBUG(self, "loaded meta index");
  }
  return caf::none;
}

caf::error index_state::flush_meta_index() {
  VAST_VERBOSE(self, "writing meta index to", meta_index_filename());
  return save(&self->system(), meta_index_filename(), meta_idx);
}

caf::error index_state::flush_statistics() {
  VAST_VERBOSE(self, "writing statistics to", statistics_filename());
  return save(&self->system(), statistics_filename(), stats);
}

caf::error index_state::flush_to_disk() {
  VAST_TRACE("");
  auto flush_all = [this]() -> caf::error {
    // Flush meta index to disk.
    if (auto err = flush_meta_index())
      return err;
    // Flush statistics to disk.
    if (auto err = flush_statistics())
      return err;
    // Flush active partition.
    if (active != nullptr)
      if (auto err = active->flush_to_disk())
        return err;
    // Flush all unpersisted partitions. This only writes the meta state of
    // each table_indexer. For actually writing the contents of each INDEXER we
    // need to rely on messaging.
    for (auto& kvp : unpersisted)
      if (auto err = kvp.first->flush_to_disk())
        return err;
    return caf::none;
  };
  if (auto err = flush_all()) {
    VAST_ERROR(self, "failed to flush state:", self->system().render(err));
    return err;
  }
  return caf::none;
}

path index_state::statistics_filename() const {
  return dir / "statistics";
}

path index_state::meta_index_filename() const {
  return dir / "meta";
}

bool index_state::worker_available() {
  return !idle_workers.empty();
}

caf::actor index_state::next_worker() {
  auto result = std::move(idle_workers.back());
  idle_workers.pop_back();
  return result;
}

caf::dictionary<caf::config_value> index_state::status() const {
  using caf::put_dictionary;
  using caf::put_list;
  caf::dictionary<caf::config_value> result;
  // Misc parameters.
  result.emplace("meta-index-filename", meta_index_filename().str());
  // Statistics.
  auto& stats_object = put_dictionary(result, "statistics");
  auto& layout_object = put_dictionary(stats_object, "layouts");
  for (auto& [name, layout_stats] : stats.layouts) {
    auto xs = caf::dictionary<caf::config_value>{};
    xs.emplace("count", layout_stats.count);
    // We cannot use put_dictionary(layout_object, name) here, because this
    // function splits the key at '.', which occurs in every layout name.
    // Hence the fallback to low-level primitives.
    layout_object.insert_or_assign(name, std::move(xs));
  }
  // Resident partitions.
  auto& partitions = put_dictionary(result, "partitions");
  if (active != nullptr)
    partitions.emplace("active", to_string(active->id()));
  auto& cached = put_list(partitions, "cached");
  for (auto& part : lru_partitions.elements())
    cached.emplace_back(to_string(part->id()));
  auto& unpersisted = put_list(partitions, "unpersisted");
  for (auto& kvp : this->unpersisted)
    unpersisted.emplace_back(to_string(kvp.first->id()));
  // General state such as open streams.
  detail::fill_status_map(result, self);
  return result;
}

void index_state::send_report() {
  performance_report r;
  measurement min, max;
  auto min_rate = std::numeric_limits<double>::infinity();
  auto max_rate = -std::numeric_limits<double>::infinity();
  auto append_report = [&](partition& p) {
    for (auto& [layout, ti] : p.table_indexers_) {
      for (size_t i = 0; i < ti.measurements_.size(); ++i) {
#ifdef VAST_MEASUREMENT_MUTEX_WORKAROUND
        ti.measurements_[i].mutex.lock();
        auto tmp = static_cast<measurement>(ti.measurements_[i]);
        ti.measurements_[i].reset();
        ti.measurements_[i].mutex.unlock();
#else
        auto tmp = std::atomic_exchange(&(ti.measurements_[i]), measurement{});
#endif
        if (tmp.events > 0) {
          r.push_back({layout.name() + "." + layout.fields[i].name, tmp});
          double rate = tmp.events * 1'000'000'000.0 / tmp.duration.count();
          if (rate < min_rate) {
            min_rate = rate;
            min = tmp;
          }
          if (rate > max_rate) {
            max_rate = rate;
            max = tmp;
          }
        }
      }
    }
  };
  if (active)
    append_report(*active);
  for (auto& p : unpersisted)
    append_report(*p.first);
  if (min.events > 0) {
    VAST_VERBOSE(self, "handled", min.events, "events at a minimum rate of",
                 static_cast<uint64_t>(min_rate), "events/sec in",
                 to_string(min.duration));
    r.push_back({"index.min", min});
  }
  if (max.events > 0) {
    VAST_DEBUG(self, "handled", max.events, "events at a maximum rate of",
               static_cast<uint64_t>(max_rate), "events/sec in",
               to_string(max.duration));
    r.push_back({"index.max", max});
  }
  if (!r.empty())
    self->send(accountant, std::move(r));
}

void index_state::reset_active_partition() {
  // Persist meta data and the state of all INDEXER actors when the active
  // partition gets replaced becomes full.
  if (active != nullptr) {
    if (auto err = active->flush_to_disk())
      VAST_ERROR(self, "failed to persist active partition");
    // Store this partition as unpersisted to make sure we're not attempting
    // to load it from disk until it is safe to do so.
    if (active_partition_indexers > 0)
      unpersisted.emplace_back(std::move(active), active_partition_indexers);
  }
  // Persist the current version of the meta_index and statistics to preserve
  // the state and be partially robust against crashes.
  if (auto err = flush_meta_index())
    VAST_ERROR(self, "failed to persist the meta index");
  if (auto err = flush_statistics())
    VAST_ERROR(self, "failed to persist the statistics");
  active = make_partition();
  active_partition_indexers = 0;
}

partition_ptr index_state::make_partition() {
  return make_partition(uuid::random());
}

partition_ptr index_state::make_partition(uuid id) {
  VAST_DEBUG(self, "starts a new partition:", id);
  return std::make_unique<partition>(this, std::move(id), max_partition_size);
}

caf::actor
index_state::make_indexer(path dir, type column_type, std::string column,
                          uuid partition_id, atomic_measurement* m) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(column_type), VAST_ARG(column),
             VAST_ARG(index), VAST_ARG(partition_id));
  caf::settings index_opts;
  index_opts["cardinality"] = max_partition_size;
  return factory(self, std::move(dir), std::move(column_type),
                 std::move(index_opts), std::move(column), self, partition_id,
                 m);
}

void index_state::decrement_indexer_count(uuid partition_id) {
  if (partition_id == active->id())
    active_partition_indexers--;
  else {
    auto i = std::find_if(unpersisted.begin(), unpersisted.end(),
                          [&](auto& kvp) {
                            return kvp.first->id() == partition_id;
                          });
    if (i == unpersisted.end())
      VAST_ERROR(self,
                 "received done from unknown indexer:", self->current_sender());
    if (--i->second == 0) {
      VAST_DEBUG(self, "successfully persisted", partition_id);
      unpersisted.erase(i);
    }
  }
}

partition* index_state::find_unpersisted(const uuid& id) {
  auto i = std::find_if(unpersisted.begin(), unpersisted.end(),
                        [&](auto& kvp) { return kvp.first->id() == id; });
  return i != unpersisted.end() ? i->first.get() : nullptr;
}

using pending_query_map = caf::detail::unordered_flat_map<uuid, evaluation_map>;

pending_query_map
index_state::build_query_map(lookup_state& lookup, uint32_t num_partitions) {
  VAST_TRACE(VAST_ARG(lookup), VAST_ARG(num_partitions));
  if (num_partitions == 0 || lookup.partitions.empty())
    return {};
  // Prefer partitions that are already available in RAM.
  std::partition(lookup.partitions.begin(), lookup.partitions.end(),
                 [&](const uuid& candidate) {
                   return (active != nullptr && active->id() == candidate)
                          || find_unpersisted(candidate) != nullptr
                          || lru_partitions.contains(candidate);
                 });
  // Maps partition IDs to the EVALUATOR actors we are going to spawn.
  pending_query_map result;
  // Helper function to spin up EVALUATOR actors for a single partition.
  auto spin_up = [&](const uuid& partition_id) {
    // We need to first check whether the ID is the active partition or one
    // of our unpersistet ones. Only then can we dispatch to our LRU cache.
    partition* part;
    if (active != nullptr && active->id() == partition_id)
      part = active.get();
    else if (auto ptr = find_unpersisted(partition_id); ptr != nullptr)
      part = ptr;
    else
      part = lru_partitions.get_or_add(partition_id).get();
    auto eval = part->eval(lookup.expr);
    if (eval.empty()) {
      VAST_DEBUG(self, "identified partition", partition_id,
                 "as candidate in the meta index, but it didn't produce an "
                 "evaluation map");
      return;
    }
    result.emplace(partition_id, std::move(eval));
  };
  // Loop over the candidate set until we either successfully scheduled
  // num_partitions partitions or run out of candidates.
  {
    auto i = lookup.partitions.begin();
    auto last = lookup.partitions.end();
    for (; i != last && result.size() < num_partitions; ++i)
      spin_up(*i);
    lookup.partitions.erase(lookup.partitions.begin(), i);
  }
  return result;
}

query_map
index_state::launch_evaluators(pending_query_map pqm, expression expr) {
  query_map result;
  for (auto& [id, eval] : pqm) {
    std::vector<caf::actor> xs{self->spawn(evaluator, expr, std::move(eval))};
    result.emplace(id, std::move(xs));
  }
  return result;
}

void index_state::add_flush_listener(caf::actor listener) {
  VAST_DEBUG(self, "adds a new 'flush' subscriber:", listener);
  flush_listeners.emplace_back(std::move(listener));
  detail::notify_listeners_if_clean(*this, *stage);
}

void index_state::notify_flush_listeners() {
  VAST_DEBUG(self, "sends 'flush' messages to", flush_listeners.size(),
             "listeners");
  for (auto& listener : flush_listeners)
    self->send(listener, flush_atom::value);
  flush_listeners.clear();
}

caf::behavior index(caf::stateful_actor<index_state>* self, const path& dir,
                    size_t max_partition_size, size_t in_mem_partitions,
                    size_t taste_partitions, size_t num_workers) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(max_partition_size),
             VAST_ARG(in_mem_partitions), VAST_ARG(taste_partitions),
             VAST_ARG(num_workers));
  VAST_ASSERT(max_partition_size > 0);
  VAST_ASSERT(in_mem_partitions > 0);
  VAST_DEBUG(self, "spawned:", VAST_ARG(max_partition_size),
             VAST_ARG(in_mem_partitions), VAST_ARG(taste_partitions));
  if (auto err = self->state.init(dir, max_partition_size, in_mem_partitions,
                                  taste_partitions)) {
    self->quit(std::move(err));
    return {};
  }
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "received exit from", msg.source,
               "with reason:", msg.reason);
    self->state.send_report();
    self->quit(msg.reason);
  });
  // Launch workers for resolving queries.
  for (size_t i = 0; i < num_workers; ++i)
    self->spawn(query_supervisor, self);
  // We switch between has_worker behavior and the default behavior (which
  // simply waits for a worker).
  self->set_default_handler(caf::skip);
  self->state.has_worker.assign(
    [=](expression& expr) {
      auto respond = [&](auto&&... xs) {
        auto mid = self->current_message_id();
        unsafe_response(self, self->current_sender(), {}, mid.response_id(),
                        std::forward<decltype(xs)>(xs)...);
      };
      // Sanity check.
      if (self->current_sender() == nullptr) {
        VAST_ERROR(self, "got an anonymous query (ignored)");
        respond(caf::sec::invalid_argument);
        return;
      }
      auto& st = self->state;
      auto client = caf::actor_cast<caf::actor>(self->current_sender());
      // Convenience function for dropping out without producing hits. Makes
      // sure that clients always receive a 'done' message.
      auto no_result = [&] {
        respond(uuid::nil(), uint32_t{0}, uint32_t{0});
        self->send(client, done_atom::value);
      };
      // Get all potentially matching partitions.
      auto candidates = st.meta_idx.lookup(expr);
      // Report no result if no candidates are found.
      if (candidates.empty()) {
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        no_result();
        return;
      }
      // Allows the client to query further results after initial taste.
      auto query_id = uuid::random();
      auto lookup = index_state::lookup_state{expr, std::move(candidates)};
      auto pqm = st.build_query_map(lookup, st.taste_partitions);
      if (pqm.empty()) {
        VAST_ASSERT(lookup.partitions.empty());
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        no_result();
        return;
      }
      auto hits = pqm.size() + lookup.partitions.size();
      auto scheduling = std::min(taste_partitions, hits);
      // Notify the client that we don't have more hits.
      if (scheduling == hits)
        query_id = uuid::nil();
      respond(query_id, detail::narrow<uint32_t>(hits),
              detail::narrow<uint32_t>(scheduling));
      auto qm = st.launch_evaluators(pqm, expr);
      VAST_DEBUG(self, "scheduled", qm.size(), "/", hits,
                 "partitions for query", expr);
      if (!lookup.partitions.empty()) {
        [[maybe_unused]] auto result
          = st.pending.emplace(query_id, std::move(lookup));
        VAST_ASSERT(result.second);
      }
      // Delegate to query supervisor (uses up this worker) and report
      // query ID + some stats to the client.
      self->send(st.next_worker(), std::move(expr), std::move(qm), client);
      if (!st.worker_available())
        self->unbecome();
    },
    [=](const uuid& query_id, uint32_t num_partitions) {
      auto& st = self->state;
      // A zero as second argument means the client drops further results.
      if (num_partitions == 0) {
        VAST_DEBUG(self, "dropped remaining results for query ID", query_id);
        st.pending.erase(query_id);
        return;
      }
      // Sanity checks.
      if (self->current_sender() == nullptr) {
        VAST_ERROR(self, "got an anonymous query (ignored)");
        return;
      }
      auto client = caf::actor_cast<caf::actor>(self->current_sender());
      auto iter = st.pending.find(query_id);
      if (iter == st.pending.end()) {
        VAST_WARNING(self, "got a request for unknown query ID", query_id);
        self->send(client, done_atom::value);
        return;
      }
      auto pqm = st.build_query_map(iter->second, num_partitions);
      if (pqm.empty()) {
        VAST_ASSERT(iter->second.partitions.empty());
        st.pending.erase(iter);
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        self->send(client, done_atom::value);
        return;
      }
      auto qm = st.launch_evaluators(pqm, iter->second.expr);
      // Delegate to query supervisor (uses up this worker) and report
      // query ID + some stats to the client.
      VAST_DEBUG(self, "schedules", qm.size(), "more partition(s) for query",
                 iter->first, "with", iter->second.partitions.size(),
                 "remaining");
      self->send(st.next_worker(), iter->second.expr, std::move(qm), client);
      // Cleanup if we exhausted all candidates.
      if (iter->second.partitions.empty())
        st.pending.erase(iter);
    },
    [=](worker_atom, caf::actor& worker) {
      self->state.idle_workers.emplace_back(std::move(worker));
    },
    [=](done_atom, uuid partition_id) {
      self->state.decrement_indexer_count(partition_id);
    },
    [=](caf::stream<table_slice_ptr> in) {
      VAST_DEBUG(self, "got a new source");
      return self->state.stage->add_inbound_path(in);
    },
    [=](status_atom) -> caf::config_value::dictionary {
      return self->state.status();
    },
    [=](telemetry_atom) {
      self->state.send_report();
      namespace defs = defaults::system;
      self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
    },
    [=](subscribe_atom, flush_atom, caf::actor& listener) {
      self->state.add_flush_listener(std::move(listener));
    });
  return {[=](worker_atom, caf::actor& worker) {
            auto& st = self->state;
            st.idle_workers.emplace_back(std::move(worker));
            self->become(caf::keep_behavior, st.has_worker);
          },
          [=](done_atom, uuid partition_id) {
            self->state.decrement_indexer_count(partition_id);
          },
          [=](caf::stream<table_slice_ptr> in) {
            VAST_DEBUG(self, "got a new source");
            return self->state.stage->add_inbound_path(in);
          },
          [=](status_atom) -> caf::config_value::dictionary {
            return self->state.status();
          },
          [=](telemetry_atom) {
            self->state.send_report();
            namespace defs = defaults::system;
            self->delayed_send(self, defs::telemetry_rate,
                               telemetry_atom::value);
          },
          [=](subscribe_atom, flush_atom, caf::actor& listener) {
            self->state.add_flush_listener(std::move(listener));
          }};
}

} // namespace vast::system
