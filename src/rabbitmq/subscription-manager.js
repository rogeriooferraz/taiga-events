/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2021-present Kaleidos INC
 */

const { destroy } = require("./client");
const { Subscription } = require("./subscription");

/**
 * Subscription manager
 */
class SubscriptionManager {
  /**
   * @constructor
   * @param client_id   Id of client
   * @param auth        Auth object
   * @param ws          WebSocket object
   */
  constructor(client_id, auth, ws) {
    this.client_id = client_id;
    this.auth = auth;
    this.ws = ws;
    this.subscriptions = {};
    this.operations = Promise.resolve();
    this.closed = false;
    this.closePromise = null;
  }

  enqueue(operation) {
    const result = this.operations.then(operation);
    // A failed operation is reported to its caller but must not block cleanup.
    this.operations = result.catch(() => {});
    return result;
  }

  /**
   * Add new subscription
   * @param routing_key Router Id
   * @param options
   * @return {*}
   */
  add(routing_key, options) {
    if (this.closed) return Promise.resolve(false);
    return this.enqueue(async () => {
      if (this.closed) return false;
      const previous = this.subscriptions[routing_key];
      if (previous) await previous.stop();
      const subscription = new Subscription(
        this.client_id,
        this.auth,
        this.ws,
        routing_key,
        options
      );
      this.subscriptions[routing_key] = subscription;
      return subscription.start();
    });
  }

  /**
   * Remove subscription by route key
   * @param routing_key   oOuter Id
   * @return {boolean}
   */
  remove(routing_key) {
    if (this.closed) return Promise.resolve(false);
    return this.enqueue(async () => {
      if (this.closed) return false;
      const subscription = this.subscriptions[routing_key];
      if (!subscription) return false;
      await subscription.stop();
      delete this.subscriptions[routing_key];
      return true;
    });
  }

  /**
   * Destroy all subscriptions
   * @return {*}
   */
  destroy() {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    this.closePromise = this.enqueue(async () => {
      this.subscriptions = {};
      return destroy(this.client_id);
    });
    return this.closePromise;
  }
}

module.exports = { SubscriptionManager };
