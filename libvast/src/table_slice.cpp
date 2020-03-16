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

#include "vast/table_slice.hpp"

#include <unordered_map>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/execution_unit.hpp>
#include <caf/sec.hpp>
#include <caf/serializer.hpp>
#include <caf/sum_type.hpp>

#include "vast/chunk.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/factory.hpp"
#include "vast/format/test.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/value.hpp"
#include "vast/value_index.hpp"
#include <vast/table_slice_builder_factory.hpp>

namespace vast {

namespace {

using size_type = table_slice::size_type;

auto cap (size_type pos, size_type num, size_type last) {
  return num == table_slice::npos ? last : std::min(last, pos + num);
}

} // namespace <anonymous>

table_slice::column_view::column_view(const table_slice& slice, size_t column)
  : slice_(slice), column_(column) {
  // nop
}

data_view table_slice::column_view::operator[](size_t row) const {
  VAST_ASSERT(row < rows());
  return slice_.at(row, column_);
}

table_slice::row_view::row_view(const table_slice& slice, size_t row)
  : slice_(slice), row_(row) {
  // nop
}

data_view table_slice::row_view::operator[](size_t column) const {
  VAST_ASSERT(column < columns());
  return slice_.at(row_, column);
}

table_slice::table_slice(table_slice_header header)
  : header_{std::move(header)} {
  // nop
}

table_slice::~table_slice() {
  // no
}

record_type table_slice::layout(size_type first_column,
                                size_type num_columns) const {
  if (first_column >= columns())
    return {};
  auto col_begin = first_column;
  auto col_end = cap(first_column, num_columns, columns());
  std::vector<record_field> sub_records{layout().fields.begin() + col_begin,
                                        layout().fields.begin() + col_end};
  return record_type{std::move(sub_records)};
}

table_slice::row_view table_slice::row(size_t index) const {
  VAST_ASSERT(index < rows());
  return {*this, index};
}

table_slice::column_view table_slice::column(size_t index) const {
  VAST_ASSERT(index < columns());
  return {*this, index};
}

caf::optional<table_slice::column_view>
table_slice::column(std::string_view name) const {
  auto& fields = header_.layout.fields;
  for (size_t index = 0; index < fields.size(); ++index)
    if (fields[index].name == name)
      return column_view{*this, index};
  return caf::none;
}

caf::error table_slice::load(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::binary_deserializer source{nullptr, data, chunk->size()};
  return deserialize(source);
}

void table_slice::append_column_to_index(std::string_view col,
                                         value_index& idx) const {
  auto val = column(col);
  if (!val)
    return;
  auto xs = *val;
  for (size_type row = 0; row < rows(); ++row)
    idx.append(xs[row], offset() + row);
}

caf::expected<std::vector<table_slice_ptr>>
make_random_table_slices(size_t num_slices, size_t slice_size,
                         record_type layout, id offset, size_t seed) {
  schema sc;
  sc.add(layout);
  // We have no access to the actor system, so we can only pick the default
  // table slice type here. This ignores any user-defined overrides. However,
  // this function is only meant for testing anyways.
  format::test::reader src{defaults::system::table_slice_type, seed,
                           std::numeric_limits<uint64_t>::max()};
  src.schema(std::move(sc));
  std::vector<table_slice_ptr> result;
  auto add_slice = [&](table_slice_ptr ptr) {
    ptr.unshared().offset(offset);
    offset += ptr->rows();
    result.emplace_back(std::move(ptr));
  };
  result.reserve(num_slices);
  if (auto err = src.read(num_slices * slice_size, slice_size, add_slice)
                   .first)
    return err;
  return result;
}

void select(std::vector<table_slice_ptr>& result, const table_slice_ptr& xs,
            const ids& selection) {
  VAST_ASSERT(xs != nullptr);
  auto xs_ids = make_ids({{xs->offset(), xs->offset() + xs->rows()}});
  auto intersection = selection & xs_ids;
  auto intersection_rank = rank(intersection);
  // Do no rows qualify?
  if (intersection_rank == 0)
    return;
  // Do all rows qualify?
  if (rank(xs_ids) == intersection_rank) {
    result.emplace_back(xs);
    return;
  }
  // Start slicing and dicing.
  auto impl = xs->implementation_id();
  auto builder = factory<table_slice_builder>::make(impl, xs->layout());
  if (builder == nullptr) {
    VAST_ERROR(__func__, "failed to get a table slice builder for", impl);
    return;
  }
  id last_offset = xs->offset();
  auto push_slice = [&] {
    if (builder->rows() == 0)
      return;
    auto slice = builder->finish();
    if (slice == nullptr) {
      VAST_WARNING(__func__, "got an empty slice");
      return;
    }
    slice.unshared().offset(last_offset);
    result.emplace_back(std::move(slice));
  };
  auto last_id = last_offset - 1;
  for (auto id : select(intersection)) {
    // Finish last slice when hitting non-consecutive IDs.
    if (last_id + 1 != id) {
      push_slice();
      last_offset = id;
      last_id = id;
    } else {
      ++last_id;
    }
    VAST_ASSERT(id >= xs->offset());
    auto row = id - xs->offset();
    VAST_ASSERT(row < xs->rows());
    for (size_t column = 0; column < xs->columns(); ++column) {
      auto cell_value = xs->at(row, column);
      if (!builder->add(cell_value)) {
        VAST_ERROR(__func__, "failed to add data at column", column, "in row",
                   row, "to the builder:", cell_value);
        return;
      }
    }
  }
  push_slice();
}

std::vector<table_slice_ptr> select(const table_slice_ptr& xs,
                                    const ids& selection) {
  std::vector<table_slice_ptr> result;
  select(result, xs, selection);
  return result;
}

void intrusive_ptr_add_ref(const table_slice* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const table_slice* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

table_slice* intrusive_cow_ptr_unshare(table_slice*& ptr) {
  return caf::default_intrusive_cow_ptr_unshare(ptr);
}

table_slice_ptr truncate(const table_slice_ptr& slice, size_t num_rows) {
  VAST_ASSERT(slice != nullptr);
  VAST_ASSERT(num_rows > 0);
  if (slice->rows() <= num_rows)
    return slice;
  auto selection = make_ids({{slice->offset(), slice->offset() + num_rows}});
  auto xs = select(slice, selection);
  VAST_ASSERT(xs.size() == 1);
  return std::move(xs.back());
}

std::pair<table_slice_ptr, table_slice_ptr> split(const table_slice_ptr& slice,
                                                  size_t partition_point) {
  VAST_ASSERT(slice != nullptr);
  if (partition_point == 0)
    return {nullptr, slice};
  if (partition_point >= slice->rows())
    return {slice, nullptr};
  auto first = slice->offset();
  auto mid = first + partition_point;
  auto last = first + slice->rows();
  // Create first table slice.
  auto xs = select(slice, make_ids({{first, mid}}));
  VAST_ASSERT(xs.size() == 1);
  // Create second table slice.
  select(xs, slice, make_ids({{mid, last}}));
  VAST_ASSERT(xs.size() == 2);
  return {std::move(xs.front()), std::move(xs.back())};
}

bool operator==(const table_slice& x, const table_slice& y) {
  if (&x == &y)
    return true;
  if (x.rows() != y.rows()
      || x.columns() != y.columns()
      || x.layout() != y.layout())
    return false;
  for (size_t row = 0; row < x.rows(); ++row)
    for (size_t col = 0; col < x.columns(); ++col)
      if (x.at(row, col) != y.at(row, col))
        return false;
  return true;
}

caf::error inspect(caf::serializer& sink, table_slice_ptr& ptr) {
  if (!ptr)
    return sink(caf::atom("NULL"));
  return caf::error::eval([&] { return sink(ptr->implementation_id()); },
                          [&] { return sink(ptr->header()); },
                          [&] { return ptr->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, table_slice_ptr& ptr) {
  caf::atom_value id;
  if (auto err = source(id))
    return err;
  if (id == caf::atom("NULL")) {
    ptr.reset();
    return caf::none;
  }
  table_slice_header header;
  if (auto err = source(header))
    return err;
  ptr = factory<table_slice>::make(id, std::move(header));
  if (!ptr)
    return ec::invalid_table_slice_type;
  return ptr.unshared().deserialize(source);
}

} // namespace vast
