FROM public.ecr.aws/docker/library/node:18-alpine as builder

USER node
WORKDIR /home/node

COPY ["package.json", "package-lock.json", "./"]
RUN npm install

COPY tsconfig.json ./
COPY src src

RUN npm run build

FROM public.ecr.aws/docker/library/node:18-alpine

RUN apk add --no-cache tini

USER node
WORKDIR /home/node

COPY ["package.json", "package-lock.json", "./"]
RUN npm install --production --cache /tmp/empty-cache && rm -rf /tmp/empty-cache

COPY --from=builder /home/node/lib/ ./

ENTRYPOINT ["/sbin/tini", "--"]
CMD [ "node", "--require", "./tracing.js", "./app.js" ]