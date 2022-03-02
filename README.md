# Social Analytics on OpenSearch

![full-arch-diagram.png](docs/architecture-diagrams/full-arch-diagram.png)

## Tenets

- Use modern technology
- Real-time processing
- Low cost
- Prefer the original  data schema from twitter api, minimal modifications.
- Easy to deploy

## Contributions

```bash
git clone https://github.com/mats16/socialanalytics-on-opensearch.git
cd socialanalytics-on-opensearch
yarn
npx projen
npx projen build
```

### How to deploy

```bash
export TWITTER_BEARER_TOKEN=xxxxxxxxxxxxxxxxxxxx
npx projen deploy
```

#### Create service-linked IAM Role for Amazon ECS before first deployment

If you use AWS ECS first time, you need to create service-linked roles.

```bash
aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com
```
