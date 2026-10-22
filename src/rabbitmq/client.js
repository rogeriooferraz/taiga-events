/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2021-present Kaleidos INC
 */

const { connect } = require("amqplib");
const { EventEmitter } = require("node:events");
const { logger } = require("../logger");

const amqpUrl = process.env.RABBITMQ_URL;

/**
 * RabbitMQ config object
 * */
const config = {
  exchange: {
    name: "events",
    type: "topic",
    options: {
      durable: false,
      autoDelete: true
    }
  },
  queue: {
    name: "",
    options: {
      autoDelete: true,
      exclusive: true
    }
  },
  channel: {
    noAck: true
  }
};
const brokerEvents = new EventEmitter();

// The exchange is auto-deleted after its last binding is removed. Keep
// declarations, bindings, and channel closes ordered across clients.
let brokerOperation = Promise.resolve();
const serializeBrokerOperation = operation => {
  const result = brokerOperation.then(operation);
  brokerOperation = result.catch(() => {});
  return result;
};

/**
 * Return the connection, creates the connection if it does not exist.
 * @return {Promise}
 */
const getConnection = (() => {
  let connectionPromise = null;
  return () => {
    if (!connectionPromise) {
      connectionPromise = connect(`${amqpUrl}?heartbeat=60`)
        .then(connection => {
          connection.on("error", error => {
            logger.error("rabbitmq-connection-error: %s", error.message);
          });
          connection.on("close", () => {
            logger.warn("rabbitmq-connection-closed");
            connectionPromise = null;
            channels.clear();
            subscriptions.clear();
            brokerEvents.emit("disconnect");
          });
          return connection;
        })
        .catch(error => {
          connectionPromise = null;
          throw error;
        });
    }
    return connectionPromise;
  };
})();

/**
 * Return the user channel
 * @return {{removeClient: (function(*=): PromiseLike<boolean> | Promise<boolean>), get: get}}
 */
const channels = (() => {
  let chs = {};
  let pendingChannels = {};

  /**
   * Remove client
   * @param client_id
   * @return {PromiseLike<boolean> | Promise<boolean>}
   */
  const removeClient = client_id => {
    const client = pendingChannels[client_id] || Promise.resolve(chs[client_id]);
    return client.then(channel => {
      if (!channel) return false;
      delete chs[client_id];
      logger.debug(`rabbitmq-channel-close client_id: ${client_id}`);
      return channel.close().then(() => true);
    });
  };

  const clear = () => {
    chs = {};
    pendingChannels = {};
  };

  /**
   * Get client
   * @param client_id   Id of client
   * @return {Promise}
   */
  const get = client_id => {
    if (pendingChannels[client_id]) {
      return pendingChannels[client_id];
    }
    if (chs[client_id]) return Promise.resolve(chs[client_id]);
    const pending = getConnection()
      .then(connection => {
        logger.debug(`rabbitmq-channel-create client_id: ${client_id}`);
        return connection.createChannel();
      })
      .then(channel => {
        channel.on("error", error => {
          logger.error("rabbitmq-channel-error client_id: %s error: %s",
                       client_id, error.message);
        });
        chs[client_id] = channel;
        return channel;
      });
    pendingChannels[client_id] = pending;
    return pending.finally(() => {
      delete pendingChannels[client_id];
    });
  };
  return {
    removeClient: removeClient,
    get: get,
    clear
  };
})();

/**
 * Return a new queue
 * @return {{create: (function(*=, *, *): Promise)}}
 */
const queues = (() => {
  /**
   * Get exchange
   * @param channel
   * @return {{durable: (boolean|*), internal: boolean, ticket: number, autoDelete: boolean, exchange: *, arguments: null, type: *, passive: boolean, nowait: boolean}}
   */
  const getExchange = channel => {
    return channel.assertExchange(
      config.exchange.name,
      config.exchange.type,
      config.exchange.options
    );
  };

  /**
   * Getting Queue
   * @param channel
   * @return { Promise }
   */
  const getQueue = channel => {
    return channel
      .assertQueue(config.queue.name, config.queue.options)
      .then(qok => {
        return qok.queue;
      });
  };
  return {
    create: (channel, client_id, routing_key) => {
      return getExchange(channel).then(exchange => {
        return getQueue(channel);
      });
    }
  };
})();

const subscriptions = (() => {
  let subs = {};

  /**
   * Binding to Queue
   * @param channel
   * @param queue
   * @param routing_key
   * @param cb
   * @return {Buffer|{noLocal, consumerTag, ticket: number, noAck, exclusive, arguments: *, queue: *, nowait: boolean}}
   */
  const bindAndSubscribe = (channel, queue, routing_key, cb) => {
    return channel.bindQueue(queue, config.exchange.name, routing_key).then(() => {
      return channel.consume(queue, cb, {
        noAck: config.channel.noAck
      });
    });
  };

  /**
   * Register new subscription
   * @param client_id
   * @param routing_key
   * @param consumerTag
   * @return {*}
   */
  const registerSubscription = (client_id, routing_key, consumerTag) => {
    subs[client_id] = subs[client_id] || {};
    return (subs[client_id][routing_key] = consumerTag);
  };

  /**
   * Subscribe to topic
   * @param client_id
   * @param routing_key
   * @param cb
   * @return { Promise }
   */
  const subscribe = (client_id, routing_key, cb) => {
    return serializeBrokerOperation(() => channels.get(client_id).then(channel => {
      return queues
        .create(channel)
        .then(queue => {
          return bindAndSubscribe(channel, queue, routing_key, cb);
        })
        .then(ok => {
          return registerSubscription(client_id, routing_key, ok.consumerTag);
        });
    }));
  };

  /**
   * Unsubscribe from topic
   * @param client_id
   * @param routing_key
   * @return { Promise }
   */
  const unsubscribe = (client_id, routing_key) => {
    return serializeBrokerOperation(() => {
      const consumerTag = subs[client_id] && subs[client_id][routing_key];
      if (consumerTag === void 0) return false;
      return channels.get(client_id).then(channel => channel.cancel(consumerTag))
        .then(() => {
          delete subs[client_id][routing_key];
          return true;
        });
    });
  };

  /**
   * Remove client
   * @param client_id
   * @return {boolean}
   */
  const removeClient = client_id => {
    return delete subs[client_id];
  };

  const clear = () => { subs = {}; };

  return {
    subscribe: subscribe,
    unsubscribe: unsubscribe,
    removeClient: removeClient,
    clear
  };
})();

/**
 * Destroy client connection
 * @param client_id
 * @return {*}
 */
const destroy = client_id => {
  subscriptions.removeClient(client_id);
  return serializeBrokerOperation(() => channels.removeClient(client_id));
};

module.exports = {
  subscribe: subscriptions.subscribe,
  unsubscribe: subscriptions.unsubscribe,
  destroy,
  getConnection,
  brokerEvents
};
