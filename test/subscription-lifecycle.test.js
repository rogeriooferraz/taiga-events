/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const clientPath = require.resolve("../src/rabbitmq/client");
const subscriptionPath = require.resolve("../src/rabbitmq/subscription");
const events = [];
let startGate;

require.cache[clientPath] = {
  id: clientPath,
  filename: clientPath,
  loaded: true,
  exports: { destroy: async () => { events.push("close channel"); } }
};
require.cache[subscriptionPath] = {
  id: subscriptionPath,
  filename: subscriptionPath,
  loaded: true,
  exports: { Subscription: class {
    start() {
      events.push("start");
      return startGate || Promise.resolve();
    }
    async stop() { events.push("stop"); }
  } }
};
const { SubscriptionManager } = require("../src/rabbitmq/subscription-manager");

test("close waits for an in-flight subscription and is idempotent", async () => {
  events.length = 0;
  let releaseStart;
  startGate = new Promise(resolve => { releaseStart = resolve; });
  const manager = new SubscriptionManager("client", {}, {});
  const adding = manager.add("notifications");
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(events, ["start"]);

  const closing = manager.destroy();
  assert.equal(manager.destroy(), closing);
  assert.equal(await manager.add("another"), false);
  assert.deepEqual(events, ["start"]);

  releaseStart();
  await Promise.all([adding, closing]);
  assert.deepEqual(events, ["start", "close channel"]);
  startGate = null;
});

test("replacement waits for the previous subscription to stop", async () => {
  events.length = 0;
  const manager = new SubscriptionManager("client", {}, {});
  await manager.add("notifications");
  await manager.add("notifications");
  assert.deepEqual(events, ["start", "stop", "start"]);
  await manager.remove("notifications");
  assert.deepEqual(events, ["start", "stop", "start", "stop"]);
  await manager.destroy();
});
