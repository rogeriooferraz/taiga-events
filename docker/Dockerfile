# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2021-present Kaleidos INC

FROM node:24-alpine
LABEL maintainer="support@taiga.io"

WORKDIR /taiga-events
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY . /taiga-events

# grab su-exec for easy step-down from root
# https://github.com/ncopa/su-exec

RUN set -eux; \
    apk upgrade --no-cache; \
    apk add --no-cache \
       gettext \
       su-exec; \
    chmod +x docker/entrypoint.sh; \
    addgroup -g 99 -S taiga; \
    adduser -u 99 -S -D -G taiga -H -h /taiga-events -s /bin/sh taiga; \
    chown -R taiga:taiga /taiga-events; \
    rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx

EXPOSE 8888
ENTRYPOINT ["./docker/entrypoint.sh"]
