/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2021-present Kaleidos INC
 */

const { subscribe, unsubscribe } = require("./client");
const { logger } = require("../logger");
const { canSubscribe } = require("../ws/authorization");

/**
 * Events subscriptions
 */
class Subscription {
  /**
   * @constructor
   * @param client_id   Id of client
   * @param auth        Auth object
   * @param ws          WS object
   * @param routing_key Router Id
   * @param options     { selfNotification: bool }
   */
  constructor(client_id, auth, ws, routing_key, options) {
    this.client_id = client_id;
    this.auth = auth;
    this.ws = ws;
    this.routing_key = routing_key;
    this.options = options ? options : {};
    this.delivery = Promise.resolve();
  }

  /**
   * Messages handler
   * @param msg Message object
   * @return {void|*}
   */
  async handleMessage(msg) {
    if (!await canSubscribe(this.routing_key, this.auth)) {
      this.ws.close(4003, "subscription authorization expired");
      return;
    }
    if (this.ws.readyState !== 1) return;
    let content;
    try {
      content = JSON.parse(msg.content.toString());
    } catch {
      logger.error("Invalid broker event payload");
      return;
    }
    if (content.session_id === this.auth.sessionId && !this.options.selfNotification) {
      return;
    }
    const clientMsg = content;
    clientMsg.routing_key = msg.fields.routingKey;

    const clientMsgStr = JSON.stringify(clientMsg);
    try {
      return this.ws.send(clientMsgStr);
    } catch (error) {
      return logger.error("WebSocket event delivery failed: %s", error.message);
    }
  }

  /**
   * Start new subscription
   * @return {*}
   */
  start() {
    return subscribe(
      this.client_id,
      this.routing_key,
      msg => {
        this.delivery = this.delivery.then(() => this.handleMessage(msg))
          .catch(() => logger.error("WebSocket event delivery failed"));
      }
    );
  }

  /**
   * Subscription stopping
   * @return {*}
   */
  stop() {
    return unsubscribe(this.client_id, this.routing_key);
  }
}

module.exports = { Subscription };
