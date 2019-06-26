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

#include <iosfwd>
#include <memory>
#include <string>

#include "vast/format/writer.hpp"

namespace vast::format::ascii {

class writer : public format::writer {
public:
  writer() = default;

  writer(writer&&) = default;

  explicit writer(std::unique_ptr<std::ostream> out);

  writer& operator=(writer&&) = default;

  caf::error write(const table_slice& x) override;

  const char* name() const override;

private:
  std::string buf_;
  std::unique_ptr<std::ostream> out_;
};

} // namespace vast::format::ascii
