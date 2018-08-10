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

#include "vast/column_index.hpp"

#include "vast/const_table_slice_handle.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/table_slice.hpp"

namespace vast {

// -- free functions -----------------------------------------------------------

namespace {

/// Tries to initialize `res` and returns it on success, otherwise returns the
/// initialization error.
caf::expected<column_index_ptr> init_res(column_index_ptr res) {
  auto err = res->init();
  if (err)
    return err;
  return res;
}

} // namespace <anonymous>

caf::expected<column_index_ptr> make_type_column_index(path filename) {
  struct impl : column_index {
    impl(path&& fname) : column_index(string_type{}, std::move(fname)) {
      // nop
    }

    void add(const const_table_slice_handle& x) override {
      VAST_TRACE(VAST_ARG(x));
      auto tn = x->layout().name();
      auto offset = x->offset();
      for (table_slice::size_type row = 0; row < x->rows(); ++row)
        idx_->append(make_data_view(tn), offset + row);
    }
  };
  return init_res(std::make_unique<impl>(std::move(filename)));
}

caf::expected<column_index_ptr>
make_column_index(path filename, type column_type, size_t column) {
  struct impl : column_index {
    impl(path&& fname, type&& ctype, size_t col)
      : column_index(std::move(ctype), std::move(fname)),
        col_(col) {
        // nop
    }

    void add(const const_table_slice_handle& x) override {
      VAST_TRACE(VAST_ARG(x));
      auto offset = x->offset();
      for (table_slice::size_type row = 0; row < x->rows(); ++row)
        if (auto element = x->at(row, col_))
          idx_->append(*element, offset + row);
    }

    size_t col_;
  };
  return init_res(std::make_unique<impl>(std::move(filename),
                                         std::move(column_type), column));
}

// -- constructors, destructors, and assignment operators ----------------------

column_index::~column_index() {
  // nop
}

// -- persistence --------------------------------------------------------------

caf::error column_index::init() {
  VAST_TRACE("");
  // Materialize the index when encountering persistent state.
  if (exists(filename_)) {
    detail::value_index_inspect_helper tmp{index_type_, idx_};
    auto result = load(filename_, last_flush_, tmp);
    if (!result) {
      VAST_ERROR("unable to load value index from disk", result.error());
      return std::move(result.error());
    } else {
      VAST_DEBUG("loaded value index with offset", idx_->offset());
    }
    return caf::none;
  }
  // Otherwise construct a new one.
  idx_ = value_index::make(index_type_);
  if (idx_ == nullptr) {
    VAST_ERROR("failed to construct index");
    return make_error(ec::unspecified, "failed to construct index");
  }
  VAST_DEBUG("constructed new value index");
  return caf::none;
}

caf::error column_index::flush_to_disk() {
  VAST_TRACE("");
  // Check whether there's something to write.
  auto offset = idx_->offset();
  if (offset == last_flush_)
    return caf::none;
  // Create parent directory if it doesn't exist.
  auto dir = filename_.parent();
  if (!exists(dir)) {
    auto result = mkdir(dir);
    if (!result)
      return result.error();
  }
  VAST_DEBUG("flush index (" << (offset - last_flush_) << '/' << offset,
             "new/total bits)");
  last_flush_ = offset;
  detail::value_index_inspect_helper tmp{index_type_, idx_};
  auto result = save(filename_, last_flush_, tmp);
  if (!result)
    return result.error();
  return caf::none;
}

// -- properties -------------------------------------------------------------

caf::expected<bitmap> column_index::lookup(const predicate& pred) {
  VAST_TRACE(VAST_ARG(pred));
  VAST_ASSERT(idx_ != nullptr);
  auto result = idx_->lookup(pred.op, make_data_view(caf::get<data>(pred.rhs)));
  VAST_DEBUG(VAST_ARG(result));
  return result;
}

// -- constructors, destructors, and assignment operators ----------------------

column_index::column_index(type index_type, path filename)
  : index_type_(std::move(index_type)),
    filename_(std::move(filename)) {
}

} // namespace vast