FROM node:10.17.0-alpine as builder

WORKDIR /home/node

COPY ["source/ec2_twitter_reader/package.json", "./"]

RUN npm install

FROM node:10.17.0-alpine

USER node

WORKDIR /home/node

COPY --from=builder /home/node/node_modules/ ./node_modules

COPY source/ec2_twitter_reader/ ./

ENV TWITTER_FILTER_LEVEL=none \
    DESTINATION=stdout

CMD [ "node", "./twitter_stream_producer_app.js" ]