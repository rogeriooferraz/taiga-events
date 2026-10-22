/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const amqpPath = require.resolve("amqplib");
const channels = [];
let connections = 0;
let bindGate;
let closeGate;
require.cache[amqpPath] = {
  id: amqpPath,
  filename: amqpPath,
  loaded: true,
  exports: { connect: async () => {
    connections += 1;
    return {
      on() {},
      createChannel: async () => {
        const channel = {
          consumed: false,
          assertExchange: async () => ({}),
          assertQueue: async () => ({ queue: "temporary" }),
          bindQueue() { return bindGate; },
          consume() {
            this.consumed = true;
            return Promise.resolve({ consumerTag: "tag" });
          },
          cancel: async () => ({}),
          close() { return closeGate; }
        };
        channels.push(channel);
        return channel;
      }
    };
  } }
};
const broker = require("../src/rabbitmq/client");

test("concurrent users share one broker connection", async () => {
  const [first, second] = await Promise.all([
    broker.getConnection(), broker.getConnection()
  ]);
  assert.equal(first, second);
  assert.equal(connections, 1);
});

test("unknown unsubscriptions do not create a broker channel", async () => {
  const count = channels.length;
  assert.equal(await broker.unsubscribe("unknown", "notifications"), false);
  assert.equal(channels.length, count);
});

test("bind completes before consume and destroy waits for channel close", async () => {
  let releaseBind;
  let releaseClose;
  bindGate = new Promise(resolve => { releaseBind = resolve; });
  closeGate = new Promise(resolve => { releaseClose = resolve; });
  const before = channels.length;
  const subscribing = broker.subscribe("client-1", "notifications", () => {});
  await new Promise(resolve => setImmediate(resolve));
  const channel = channels[before];
  assert.equal(channel.consumed, false);
  releaseBind();
  await subscribing;
  assert.equal(channel.consumed, true);
  assert.equal(await broker.unsubscribe("client-1", "notifications"), true);
  assert.equal(await broker.unsubscribe("client-1", "notifications"), false);

  let closed = false;
  const destroying = broker.destroy("client-1").then(() => { closed = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(closed, false);
  releaseClose();
  await destroying;
  assert.equal(closed, true);
});

test("client declarations do not overlap an auto-deleting exchange", async () => {
  let releaseBind;
  bindGate = new Promise(resolve => { releaseBind = resolve; });
  closeGate = Promise.resolve();
  const before = channels.length;
  const first = broker.subscribe("client-2", "notifications", () => {});
  const second = broker.subscribe("client-3", "notifications", () => {});
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(channels.length, before + 1);
  releaseBind();
  await Promise.all([first, second]);
  assert.equal(channels.length, before + 2);
  await Promise.all([broker.destroy("client-2"), broker.destroy("client-3")]);
});
