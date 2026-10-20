/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

const { getUserId } = require("../crypto");
const { request } = require("node:http");

const PROJECT_KEY = /^changes\.project\.(\d+)\.[a-z_]+(?:\.[a-z_]+)?$/;

async function canSubscribe(routingKey, auth) {
  if (!auth || typeof routingKey !== "string") return false;
  const userId = getUserId(auth.token);
  if (userId == null) return false;

  if (routingKey === "notifications") return true;
  if (routingKey === `live_notifications.${userId}` ||
      routingKey === `web_notifications.${userId}`) return true;

  const match = PROJECT_KEY.exec(routingKey);
  if (!match) return false;

  const baseUrl = process.env.TAIGA_BACK_URL;
  const host = process.env.TAIGA_BACK_HOST;
  if (!baseUrl || !host) return false;

  return new Promise(resolve => {
    try {
      const req = request(`${baseUrl}/projects/${match[1]}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${auth.token}`, Host: host },
        timeout: 3000
      }, response => {
        response.resume();
        resolve(response.statusCode === 200);
      });
      req.on("timeout", () => req.destroy());
      req.on("error", () => resolve(false));
      req.end();
    } catch {
      resolve(false);
    }
  });
}

module.exports = { canSubscribe };
