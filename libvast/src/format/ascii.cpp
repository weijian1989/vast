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

#include "vast/format/ascii.hpp"

#include <ostream>

#include "vast/error.hpp"
#include "vast/print.hpp"
#include "vast/table_slice.hpp"

namespace vast::format::ascii {

writer::writer(std::unique_ptr<std::ostream> out) : out_(std::move(out)) {
  // nop
}

caf::error writer::write(const table_slice& x) {
  for (size_t row = 0; row < x.rows(); ++row) {
    buf_ += '<';
    print(buf_, x.at(row, 0));
    for (size_t column = 1; column < x.columns(); ++column) {
      buf_ += ", ";
      print(buf_, x.at(row, column));
    }
    buf_ += '>';
    buf_ += '\n';
    *out_ << buf_;
    buf_.clear();
  }
  return caf::none;
}

const char* writer::name() const {
  return "ascii-writer";
}

} // namespace vast::format::ascii
