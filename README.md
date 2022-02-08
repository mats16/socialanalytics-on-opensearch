# Social Analytics on OpenSearch

![full-arch-diagram.png](docs/architecture-diagrams/full-arch-diagram.png)

## Preparation before deployment

### Create service-linked IAM Role for Amazon ECS

If you use AWS ECS first time, you need to create service-linked roles.

```bash
aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com
```

## How to Buid & Deploy

```bash
export TWITTER_BEARER_TOKEN=xxxxxxxxxxxxxxxxxxxx
npx projen
npj projen deploy
```
