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

#include "fixtures/node.hpp"

#include <caf/all.hpp>

#include "vast/detail/spawn_container_source.hpp"
#include "vast/query_options.hpp"
#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"
#include "vast/uuid.hpp"

#include "vast/system/node.hpp"
#include "vast/system/query_status.hpp"

using namespace vast;

namespace fixtures {

node::node() {
  MESSAGE("spawning node");
  test_node = self->spawn(system::node, "test", directory / "node");
  run();
  MESSAGE("spawning components");
  spawn_component("type-registry");
  spawn_component("consensus");
  spawn_component("archive");
  spawn_component("index");
  spawn_component("importer");
}

node::~node() {
  self->send_exit(test_node, caf::exit_reason::user_shutdown);
}

void node::ingest(const std::string& type) {
  // Get the importer from the node.
  MESSAGE("getting importer from node");
  caf::actor importer;
  auto rh = self->request(test_node, defaults::system::request_timeout,
                          caf::get_atom::value);
  run();
  rh.receive(
    [&](const std::string& id, system::registry& reg) {
      auto er = reg.components[id].equal_range("importer");
      if (er.first == er.second)
        FAIL("no importers available at test node");
      importer = er.first->second.actor;
    },
    error_handler()
  );
  MESSAGE("sending " << type << " logs");
  // Send previously parsed logs directly to the importer (as opposed to
  // going through a source).
  if (type == "zeek" || type == "all") {
    detail::spawn_container_source(sys, zeek_conn_log_slices, importer);
    // TODO: ship DNS and HTTP log slices when available in the events fixture
    // self->send(importer, zeek_dns_log);
    // self->send(importer, zeek_http_log);
  }
  // TODO: ship slices when available in the events fixture
  // if (type == "bgpdump" || type == "all")
  //   self->send(importer, bgpdump_txt);
  // if (type == "random" || type == "all")
  //   self->send(importer, random);
  run();
  MESSAGE("done ingesting logs");
}

std::vector<event> node::query(std::string expr) {
  MESSAGE("spawn an exporter and register ourselves as sink");
  auto exp = spawn_component("exporter", std::move(expr));
  self->monitor(exp);
  self->send(exp, system::sink_atom::value, self);
  self->send(exp, system::run_atom::value);
  self->send(exp, system::extract_atom::value);
  run();
  MESSAGE("fetch results from mailbox");
  std::vector<event> result;
  bool running = true;
  self->receive_while(running)(
    [&](table_slice_ptr slice) {
      MESSAGE("... got " << slice->rows() << " events");
      to_events(result, *slice);
    },
    [&](const uuid&, const system::query_status&) {
      // ignore
    },
    [&](const caf::down_msg& msg) {
      if (msg.reason != caf::exit_reason::normal)
        FAIL("exporter terminated with exit reason: " << to_string(msg.reason));
    },
    // Do a one-pass can over the mailbox without waiting for messages.
    caf::after(std::chrono::seconds(0)) >> [&] { running = false; });
  MESSAGE("got " << result.size() << " events in total");
  return result;
}

} // namespace fixtures
