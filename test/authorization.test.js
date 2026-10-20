/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

const test = require("node:test");
const assert = require("node:assert/strict");
const jwt = require("jsonwebtoken");
const { createServer } = require("node:http");

process.env.SECRET = "test-only-secret";
process.env.TAIGA_BACK_URL = "http://taiga-back:8000/api/v1";
process.env.TAIGA_BACK_HOST = "taiga.example.test";
const { canSubscribe } = require("../src/ws/authorization");
const { Subscription } = require("../src/rabbitmq/subscription");

const token = jwt.sign({ user_id: 42, token_type: "access" }, process.env.SECRET,
                       { expiresIn: "1h" });
const auth = { token, sessionId: "test-session" };

test("limits personal notifications and rejects broker wildcards", async () => {
  assert.equal(await canSubscribe("notifications", auth), true);
  assert.equal(await canSubscribe("live_notifications.42", auth), true);
  assert.equal(await canSubscribe("web_notifications.42", auth), true);
  for (const key of ["live_notifications.43", "web_notifications.43",
                     "changes.project.#", "changes.project.1.*", "tasks"])
    assert.equal(await canSubscribe(key, auth), false);
});

test("consults Taiga project permissions before subscribing", async () => {
  const originalUrl = process.env.TAIGA_BACK_URL;
  let status = 200;
  const server = createServer((request, response) => {
    assert.equal(request.url, "/api/v1/projects/17");
    assert.equal(request.headers.authorization, `Bearer ${token}`);
    assert.equal(request.headers.host, "taiga.example.test");
    response.writeHead(status).end();
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  process.env.TAIGA_BACK_URL =
    `http://127.0.0.1:${server.address().port}/api/v1`;
  try {
    assert.equal(await canSubscribe("changes.project.17.tasks", auth), true);
    assert.equal(await canSubscribe("changes.project.17.tasks.task", auth), true);
    const sent = [];
    let closedCode;
    const socket = {
      readyState: 1,
      send: message => sent.push(message),
      close: code => { closedCode = code; }
    };
    const subscription = new Subscription("test-client", auth, socket,
                                          "changes.project.17.tasks");
    const event = {
      content: Buffer.from(JSON.stringify({ session_id: "other", data: 1 })),
      fields: { routingKey: "changes.project.17.tasks" }
    };
    await subscription.handleMessage(event);
    assert.equal(sent.length, 1);
    status = 403;
    assert.equal(await canSubscribe("changes.project.17.tasks", auth), false);
    await subscription.handleMessage(event);
    assert.equal(closedCode, 4003);
    assert.equal(sent.length, 1);
    process.env.TAIGA_BACK_URL = "http://127.0.0.1:1/api/v1";
    assert.equal(await canSubscribe("changes.project.17.tasks", auth), false);
  } finally {
    process.env.TAIGA_BACK_URL = originalUrl;
    await new Promise(resolve => server.close(resolve));
  }
});
