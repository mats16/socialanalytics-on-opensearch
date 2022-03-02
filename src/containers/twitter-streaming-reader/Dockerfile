FROM --platform=linux/arm64 public.ecr.aws/docker/library/node:16-alpine as builder

WORKDIR /home/node

COPY ["package.json", "./"]

RUN npm install

COPY ["tsconfig.json", "./"]

COPY src src

RUN npm run build

FROM --platform=linux/arm64 public.ecr.aws/docker/library/node:16-alpine

RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

USER node

WORKDIR /home/node

COPY --from=builder /home/node/node_modules/ ./node_modules
COPY --from=builder /home/node/lib/ ./lib

CMD [ "node", "./lib/app.js" ]