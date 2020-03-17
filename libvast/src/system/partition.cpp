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

#include "vast/system/partition.hpp"

#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/index.hpp"
#include "vast/system/spawn_indexer.hpp"
#include "vast/system/table_indexer.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/local_actor.hpp>
#include <caf/make_counted.hpp>
#include <caf/stateful_actor.hpp>

using namespace std::chrono;
using namespace caf;

namespace vast::system {

partition::partition(index_state* state, uuid id, size_t max_capacity)
  : state_(state),
    id_(std::move(id)),
    capacity_(max_capacity) {
  // If the directory already exists, we must have some state from the past and
  // are pre-loading all INDEXER types we are aware of.
  VAST_ASSERT(state != nullptr);
}

partition::~partition() noexcept {
  flush_to_disk();
}

// -- persistence --------------------------------------------------------------

caf::error partition::init() {
  VAST_TRACE("");
  auto file_path = meta_file();
  if (!exists(file_path))
    return ec::no_such_file;
  if (auto err = load(nullptr, file_path , meta_data_))
    return err;
  VAST_DEBUG(state_->self, "loaded partition", id_, "from disk with",
             meta_data_.types.size(), "layouts");
  return caf::none;
}

caf::error partition::flush_to_disk() {
  if (meta_data_.dirty) {
    // Write all layouts to disk.
    if (auto err = save(nullptr, meta_file(), meta_data_))
      return err;
    meta_data_.dirty = false;
  }
  // Write state for each layout to disk.
  for (auto& kvp : table_indexers_)
    if (auto err = kvp.second.flush_to_disk())
      return err;
  return caf::none;
}

// -- properties ---------------------------------------------------------------

namespace {

using eval_mapping = evaluation_map::mapped_type;

caf::actor fetch_indexer(table_indexer& tbl, const data_extractor& dx,
                         [[maybe_unused]] relational_operator op,
                         [[maybe_unused]] const data& x) {
  VAST_TRACE(VAST_ARG(tbl), VAST_ARG(dx), VAST_ARG(op), VAST_ARG(x));
  // Sanity check.
  if (dx.offset.empty())
    return nullptr;
  auto& r = caf::get<record_type>(dx.type);
  auto k = r.resolve(dx.offset);
  VAST_ASSERT(k);
  auto index = r.flat_index_at(dx.offset);
  if (!index) {
    VAST_DEBUG(tbl.state().self, "got invalid offset for record type", dx.type);
    return nullptr;
  }
  return tbl.indexer_at(*index);
}

caf::actor fetch_indexer(table_indexer& tbl, const attribute_extractor& ex,
                         relational_operator op, const data& x) {
  VAST_TRACE(VAST_ARG(tbl), VAST_ARG(ex), VAST_ARG(op), VAST_ARG(x));
  auto& layout = tbl.layout();
  if (ex.attr == system::type_atom::value) {
    // Doesn't apply if the query name doesn't match our type.
    if (!evaluate(layout.name(), op, x))
      return nullptr;
    // We know the answer immediately: all IDs that are part of the table.
    // However, we still have to "lift" this result into an actor for the
    // EVALUATOR.
    // TODO: Spawning a one-shot actor is quite expensive. Maybe the
    //       table_indexer could instead maintain this actor lazily.
    auto row_ids = tbl.row_ids();
    return tbl.state().self->spawn([row_ids]() -> caf::behavior {
      return [=](const curried_predicate&) { return row_ids; };
    });
  }
  if (ex.attr == system::timestamp_atom::value) {
    if (!caf::holds_alternative<timestamp>(x)) {
      VAST_WARNING(tbl.state().self,
                   "expected a timestamp as time extractor attribute , got:",
                   x);
      return nullptr;
    }
    // Find the column with attribute 'time'.
    auto pred = [](auto& x) {
      return caf::holds_alternative<time_type>(x.type)
             && has_attribute(x.type, "timestamp");
    };
    auto& fs = layout.fields;
    auto i = std::find_if(fs.begin(), fs.end(), pred);
    if (i == fs.end())
      return nullptr;
    // Redirect to "ordinary data lookup".
    auto pos = static_cast<size_t>(std::distance(fs.begin(), i));
    data_extractor dx{layout, vast::offset{pos}};
    return fetch_indexer(tbl, dx, op, x);
  }
  VAST_WARNING(tbl.state().self, "got unsupported attribute:", ex.attr);
  return nullptr;
}

} // namespace

evaluation_map partition::eval(const expression& expr) {
  evaluation_map result;
  // Step #1: use the expression to select matching layouts.
  for (auto layout : layouts()) {
    // Step #2: Split the resolved expression into its predicates and select
    // all matching INDEXER actors per predicate.
    auto resolved = resolve(expr, layout);
    // Skip any layout that we cannot resolve.
    if (resolved.empty())
      continue;
    // Add triples (offset, curried predicate, and INDEXER) to evaluation map.
    evaluation_map::mapped_type triples;
    for (auto& kvp: resolved) {
      auto& pred = kvp.second;
      auto get_indexer_handle = [&](const auto& ext, const data& x) {
        if (auto i = get_or_add(layout))
          return fetch_indexer(i->first, ext, pred.op, x);
        VAST_ERROR(state_->self,
                   "failed to initialize table_indexer for layout", layout,
                   "-> query will not execute on the full data set");
        return caf::actor{};
      };
      auto v = detail::overload(
        [&](const attribute_extractor& ex, const data& x) {
          return get_indexer_handle(ex, x); // clang-format fix
        },
        [&](const data_extractor& dx, const data& x) {
          return get_indexer_handle(dx, x);
        },
        [](const auto&, const auto&) {
          return caf::actor{}; // clang-format fix
        });
      auto hdl = caf::visit(v, pred.lhs, pred.rhs);
      if (hdl != nullptr) {
        triples.emplace_back(kvp.first, curried(pred), std::move(hdl));
      }
    }
    if (!triples.empty())
      result.emplace(layout, std::move(triples));
  }
  return result;
}

std::vector<record_type> partition::layouts() const {
  std::vector<record_type> result;
  auto& ts = meta_data_.types;
  result.reserve(ts.size());
  std::transform(ts.begin(), ts.end(), std::back_inserter(result),
                 [](auto& kvp) { return kvp.second; });
  return result;
}

path partition::base_dir() const {
  return state_->dir / to_string(id_);
}

path partition::meta_file() const {
  return base_dir() / "meta";
}

path partition::column_file(const record_field& field) const {
  return base_dir() / (field.name + "-" + to_string(uhash<xxhash64>{}(field)));
}

caf::expected<std::pair<caf::actor, bool>>
partition::get(const record_field& field) {
  using ret_t = std::pair<caf::actor, bool>;
  auto i = indexers_.find(field);
  if (i != indexers_.end())
    return ret_t{i->second, false};
  auto atmc = &measurements_[field];
  auto indexer = state().make_indexer(column_file(field), field.type,
                                      field.name, id(), atmc);
  if (!indexer)
    return make_error(ec::unspecified, "failed to create column index");
  indexers_.emplace(field, indexer);
  return ret_t{indexer, true};
}

caf::expected<std::pair<table_indexer&, bool>>
partition::get_or_add(const record_type& key) {
  VAST_TRACE(VAST_ARG(key));
  auto i = table_indexers_.find(key);
  if (i != table_indexers_.end())
    return std::pair<table_indexer&, bool>{i->second, false};
  auto digest = to_digest(key);
  add_layout(digest, key);
  auto ti = table_indexer::make(this, key);
  if (!ti)
    return ti.error();
  auto result = table_indexers_.emplace(key, std::move(*ti));
  VAST_ASSERT(result.second == true);
  return std::pair<table_indexer&, bool>{result.first->second, true};
}

} // namespace vast::system

namespace std {

namespace {

using pptr = vast::system::partition_ptr;

} // namespace <anonymous>

size_t hash<pptr>::operator()(const pptr& ptr) const {
  hash<vast::uuid> f;
  return ptr != nullptr ? f(ptr->id()) : 0u;
}

} // namespace std
