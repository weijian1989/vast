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

#include "vast/system/indexer.hpp"

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/all.hpp>

#include <new>

using namespace caf;

namespace vast::system {

indexer_state::indexer_state() {
  // nop
}

indexer_state::~indexer_state() {
  col.~column_index();
}

caf::error indexer_state::init(event_based_actor* self, path filename,
                               type column_type, caf::settings index_opts,
                               std::string column, caf::actor index,
                               uuid partition_id, atomic_measurement* m) {
  this->index = std::move(index);
  this->partition_id = partition_id;
  this->measurement = m;
  new (&col)
    column_index(self->system(), std::move(column_type), std::move(index_opts),
                 std::move(filename), std::move(column));
  return col.init();
}

behavior indexer(stateful_actor<indexer_state>* self, path dir,
                 type column_type, caf::settings index_opts, std::string column,
                 caf::actor index, uuid partition_id, atomic_measurement* m) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(column_type), VAST_ARG(column));
  VAST_DEBUG(self, "operates for column", column, "of type", column_type);
  if (auto err = self->state.init(
        self, std::move(dir) / "fields" / column + "-" + to_digest(column_type),
        std::move(column_type), std::move(index_opts), std::move(column),
        std::move(index), partition_id, m)) {
    self->quit(std::move(err));
    return {};
  }
  auto handle_batch = [=](const std::vector<table_slice_ptr>& xs) {
    auto t = atomic_timer::start(*self->state.measurement);
    auto events = uint64_t{0};
    for (auto& x : xs) {
      events += x->rows();
      self->state.col.add(x);
    }
    t.stop(events);
  };
  return {
    [=](const curried_predicate& pred) {
      VAST_DEBUG(self, "got predicate:", pred);
      return self->state.col.lookup(pred.op, make_view(pred.rhs));
    },
    [=](persist_atom) -> result<void> {
      if (auto err = self->state.col.flush_to_disk(); err != caf::none)
        return err;
      return caf::unit;
    },
    [=](stream<table_slice_ptr> in) {
      self->make_sink(
        in,
        [](unit_t&) {
          // nop
        },
        [=](unit_t&, const std::vector<table_slice_ptr>& xs) {
          handle_batch(xs);
        },
        [=](unit_t&, const error& err) {
          auto& st = self->state;
          if (auto flush_err = st.col.flush_to_disk())
            VAST_WARNING(self, "failed to persist state:",
                         self->system().render(flush_err));
          if (err && err != caf::exit_reason::user_shutdown) {
            VAST_ERROR(self, "got a stream error:", self->system().render(err));
            return;
          }
          self->send(st.index, done_atom::value, st.partition_id);
        });
    },
    [=](const std::vector<table_slice_ptr>& xs) { handle_batch(xs); },
    [=](shutdown_atom) { self->quit(exit_reason::user_shutdown); },
  };
}

} // namespace vast::system
