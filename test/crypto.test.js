/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

const test = require("node:test");
const assert = require("node:assert/strict");
const jwt = require("jsonwebtoken");

process.env.SECRET = "test-only-secret";
const { getUserId, verify } = require("../src/crypto");

test("accepts a current Taiga access token", () => {
  const token = jwt.sign({ user_id: 42, token_type: "access" }, process.env.SECRET,
                         { algorithm: "HS256", expiresIn: "1h" });
  assert.equal(verify(token), true);
  assert.equal(getUserId(token), 42);
});

test("rejects expired, unsigned, wrong algorithm, and missing-expiry tokens", () => {
  const claims = { user_id: 42, token_type: "access" };
  const expired = jwt.sign(claims, process.env.SECRET, { expiresIn: -1 });
  const wrongAlgorithm = jwt.sign(claims, process.env.SECRET,
                                  { algorithm: "HS384", expiresIn: "1h" });
  const noExpiry = jwt.sign(claims, process.env.SECRET);
  for (const token of [expired, wrongAlgorithm, noExpiry, "bad-token"]) {
    assert.equal(verify(token), false);
    assert.equal(getUserId(token), null);
  }
});

test("rejects a refresh token on the event socket", () => {
  const token = jwt.sign({ user_id: 42, token_type: "refresh" }, process.env.SECRET,
                         { expiresIn: "1h" });
  assert.equal(verify(token), false);
});
