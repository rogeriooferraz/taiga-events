/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2021-present Kaleidos INC
 */

const { randomUUID } = require("crypto");

const { SubscriptionManager } = require("../rabbitmq");
const { brokerEvents } = require("../rabbitmq/client");
const { logger } = require("../logger");
const { verify } = require("../crypto");
const { canSubscribe } = require("./authorization");

let clients = {};

brokerEvents.on("disconnect", () => {
  for (const client of Object.values(clients)) {
    if (client.ws.readyState === 1) {
      client.ws.close(1011, "broker connection lost");
    }
  }
});

/**
 * Common WS client
 */
class Client {
  /**
   * @constructor
   * @param ws  WebSocket instance
   */
  constructor(ws, request) {
    this.ws = ws;
    this.remoteAddress = request.socket.remoteAddress;
    this.id = randomUUID();
    this.handleEvents();
  }

  /**
   * Events handler
   * @return {*}
   */
  handleEvents() {
    this.ws.on("message", message => {
      Promise.resolve().then(() => this.handleMessage(message)).catch(error => {
        this.handleError(error);
        if (this.ws.readyState === 1) this.ws.close(1011, "subscription failed");
      });
    });
    return this.ws.on("error", this.handleError.bind(this));
  }

  /**
   * Error handler
   * @param error
   * @return {*}
   */
  handleError(error) {
    logger.error("evt=client_error remote_address=%s error=%s",
                 this.remoteAddress, error.message);
  }

  /**
   * Messages handler
   * @param message
   * @return {null|{sessionId}|{token}|*}
   */
  handleMessage(message) {
    let msg;
    try {
      msg = JSON.parse(message);
    } catch {
      logger.error("Error parsing WebSocket message");
      return null;
    }

    if (msg.cmd === "ping") {
      logger.debug("PING");
      return this.sendPong();
    } else {
      logger.info("WebSocket command received");
      if (msg.cmd === "auth" && msg.data) {
        return this.authUser(msg.data);
      } else if (msg.cmd === "subscribe" && msg.routing_key) {
        return this.addSubscription(msg.routing_key, msg.options);
      } else if (msg.cmd === "unsubscribe" && msg.routing_key) {
        return this.removeSubscription(msg.routing_key);
      }
    }
  }

  /**
   * Auth user
   * @param auth
   * @return {{sessionId}|{token}|*}
   */
  authUser(auth) {
    if (!this.auth && auth.token && auth.sessionId && verify(auth.token)) {
      return (this.auth = auth);
    }
  }

  /**
   * Register client
   * @param routing_key
   * @param options
   * @return {*}
   */
  async addSubscription(routing_key, options) {
    const auth = this.auth;
    if (auth && await canSubscribe(routing_key, auth) &&
        this.auth === auth && this.ws.readyState === 1) {
      if (!this.subscriptionManager) {
        this.subscriptionManager = new SubscriptionManager(
          this.id,
          this.auth,
          this.ws
        );
      }
      return this.subscriptionManager.add(routing_key, options);
    }
  }

  /**
   * Unregister client
   * @param routing_key
   * @return {*}
   */
  removeSubscription(routing_key) {
    if (this.subscriptionManager) {
      return this.subscriptionManager.remove(routing_key);
    }
  }

  /**
   * Sending Pong event to client
   * @return {*}
   */
  sendPong() {
    try {
      return this.ws.send(
        JSON.stringify({
          cmd: "pong"
        })
      );
    } catch (error) {
      return logger.error(error);
    }
  }

  /**
   * Close connections
   */
  close() {
    if (this.subscriptionManager) {
      return this.subscriptionManager.destroy();
    }
  }
}

/**
 * Creating new client with onClose listener
 * @param ws
 * @return {*}
 */
const createClient = (ws, request) => {
  const client = new Client(ws, request);
  clients[client.id] = client;
  logger.info(`ws-connection-open: ${client.id}`);

  return client.ws.on("close", function() {
    Promise.resolve().then(() => client.close()).catch(error => {
      logger.error("rabbitmq-channel-close-failed client_id: %s error: %s",
                   client.id, error.message);
    });
    logger.info(`ws-connection-close: ${client.id}`);
    return delete clients[client.id];
  });
};

module.exports = { createClient };
