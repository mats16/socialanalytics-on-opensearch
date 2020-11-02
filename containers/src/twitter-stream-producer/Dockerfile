FROM node:10.19.0-alpine as builder

WORKDIR /home/node

COPY ["src/package.json", "./"]

RUN npm install

FROM node:10.19.0-alpine

USER node

WORKDIR /home/node

COPY --from=builder /home/node/node_modules/ ./node_modules

COPY src/ ./

ENV TWITTER_TOPICS='*' \
    TWITTER_LANGUAGES=en,ja \
    TWITTER_FILTER_LEVEL=none \
    DESTINATION=stdout

CMD [ "node", "./twitter_stream_producer_app.js" ]