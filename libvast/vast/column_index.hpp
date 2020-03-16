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

#include "vast/bitmap.hpp"
#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/type.hpp"
#include "vast/value_index.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <memory>

namespace vast {

// -- free functions -----------------------------------------------------------

/// Creates a single colum for a value at column `col`.
/// @relates column_index
caf::expected<column_index_ptr>
make_column_index(caf::actor_system& sys, path filename, type column_type,
                  caf::settings index_opts, std::string column);

// -- class definition ---------------------------------------------------------

/// Indexes a specific aspect of an event, such as meta data (e.g., timestamp)
/// and event data.
class column_index {
public:
  // -- constructors, destructors, and assignment operators --------------------

  column_index(caf::actor_system& sys, type index_type,
               caf::settings index_opts, path filename, std::string column);

  ~column_index();

  // -- persistence ------------------------------------------------------------

  /// Materializes the index from disk if `filename()` exists, constructs a new
  /// one otherwise. Automatically called by the factory functions.
  /// @returns An error if I/O operations fail.
  caf::error init();

  /// Persists the index to disk.
  caf::error flush_to_disk();

  // -- properties -------------------------------------------------------------

  /// Adds an event to the index.
  /// @pre `init()` was called previously.
  void add(const table_slice_ptr& x);

  /// Queries event IDs that fulfill the given predicate.
  /// @pre `init()` was called previously.
  caf::expected<bitmap> lookup(relational_operator op, data_view rhs);

  /// @returns the file name for loading and storing the index.
  const path& filename() const {
    return filename_;
  }

  /// Serializes or deserializes a column index.
  template <class Inspector>
  friend auto inspect(Inspector& f, column_index& x) {
    return f(x.index_type_, x.filename_, x.idx_, f.last_flush_);
  }

  /// @returns the type of this column.
  const type& index_type() const {
    return index_type_;
  }

  /// @returns the value index.
  /// @pre `init()` was called and did not return an error.
  const value_index& idx() const {
    VAST_ASSERT(idx_ != nullptr);
    return *idx_;
  }

  bool has_skip_attribute() const noexcept {
    return has_skip_attribute_;
  }

  /// Returns the position of this column in the layout.
  // size_t position() const noexcept {
  //  return col_;
  //}

  /// Returns whether the column index has unpersisted changes.
  /// @pre `init()` was called and returned no error
  bool dirty() const noexcept;

protected:
  // -- member variables -------------------------------------------------------

  value_index_ptr idx_;
  std::string column_;
  bool has_skip_attribute_;
  type index_type_;
  caf::settings index_opts_;
  path filename_;
  value_index::size_type last_flush_ = 0;
  caf::actor_system& sys_;
};

// -- related types ------------------------------------------------------------

/// @relates column_index
using column_index_ptr = std::unique_ptr<column_index>;

// TODO: should `add_to_index` return a `caf::error` instead?

/*

/// Indexes an event for all columns.
/// @param columns List of column indexes.
/// @param x Event for ingestion.
/// @relates column_index
/// @pre `columns` does not contain null pointers
void add_to_index(column_index::column_index_ptr_vec columns, const event& x);

/// Queries event IDs from all columns that fulfill the given predicate.
/// @param columns List of column indexes.
/// @param event_type Type of events that are ingested by the columns.
/// @param pred Boolean predicate for selecting events.
/// @relates column_index
/// @pre `columns` does not contain null pointers
caf::expected<bitmap> lookup(column_index::column_index_ptr_vec columns,
                             const type& event_type, const predicate& pred);

*/

} // namespace vast
