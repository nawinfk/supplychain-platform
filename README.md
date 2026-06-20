# supplychain-platform

Production-style reference platform for ingesting AIS, port weather, and disruption news into Kafka, persisting operational data in PostgreSQL, and publishing derived supply-chain risk events. Local validation needs only Python 3.12 and Docker; real Kafka credentials are not required for tests or AIS mock mode.

## Architecture

| Component | Input | Output |
| --- | --- | --- |
| `ais-producer` | AISStream WebSocket or built-in mock mode | `supplychain.raw.ais.v1` |
| `weather-producer` | Open-Meteo for five ports every 5 minutes | `supplychain.raw.weather.v1` |
| `news-producer` | GDELT disruption search with retry/backoff | `supplychain.raw.news.v1` |
| `vessel-consumer-worker` | Raw AIS topic | PostgreSQL vessels and positions; invalid data to `supplychain.dlq.v1` |
| `risk-worker` | AIS, weather, and news topics | PostgreSQL risk events and `supplychain.processed.risk-events.v1` |
| `api-service` | PostgreSQL | REST API and OpenAPI docs at `/docs` |

The vessel worker uses manual Kafka offset commits. A valid record is committed only after its database transaction succeeds. Invalid input is committed only after synchronous DLQ delivery succeeds. Processing is at-least-once; downstream consumers should use event identifiers for deduplication.

## 1. Local setup

Prerequisites: Python 3.12 and Docker with Compose.

```bash
cp .env.example .env
make setup
make db-up
make db-init
make test
make run-api
curl http://localhost:8000/health
```

`db-up` initializes a fresh PostgreSQL volume automatically; `db-init` is an idempotent manual schema application. Start the API and mock producer in separate terminals:

```bash
make run-ais-producer
make run-api
```

The API is at `http://localhost:8000`; OpenAPI UI is at `http://localhost:8000/docs`. The mock AIS producer uses a local dry-run publisher, so it does not contact AISStream or Kafka. Consumers require a real Kafka broker and fail immediately with a clear configuration error while dummy credentials are active.

Run the complete local check with `make local-validate`. Database lifecycle helpers are `make db-down`, `make db-reset`, `make db-logs`, and `make db-shell`.

## 2. Confluent Cloud topics

Create these topics before starting services:

```text
supplychain.raw.ais.v1
supplychain.raw.weather.v1
supplychain.raw.news.v1
supplychain.processed.risk-events.v1
supplychain.dlq.v1
```

With the Confluent CLI authenticated and an environment/cluster selected:

```bash
for topic in supplychain.raw.ais.v1 supplychain.raw.weather.v1 supplychain.raw.news.v1 supplychain.processed.risk-events.v1 supplychain.dlq.v1; do
  confluent kafka topic create "$topic" --partitions 3
done
```

Create a scoped Kafka API key with read/write access to these topics and consumer groups. Real Kafka/EKS deployment requires:

- Confluent bootstrap server (`KAFKA_BOOTSTRAP_SERVERS`)
- Confluent API key (`KAFKA_API_KEY`, or legacy `KAFKA_SASL_USERNAME`)
- Confluent API secret (`KAFKA_API_SECRET`, or legacy `KAFKA_SASL_PASSWORD`)
- PostgreSQL SQLAlchemy URL (`DATABASE_URL`)
- AISStream API key (`AISSTREAM_API_KEY`), optional when mock mode is enabled

## 3. PostgreSQL setup

Local Compose runs PostgreSQL only and applies `database/schema.sql` on a fresh volume:

```bash
make db-up
docker compose ps
make db-init
docker compose exec postgres psql -U supplychain -d supplychain -c '\dt'
```

`make db-init` is idempotent. Kubernetes expects an externally managed PostgreSQL instance; set its SQLAlchemy URL in the Secret. Apply the schema to that database before deploying workers.

## 4. Kubernetes secrets

Never commit the rendered Secret. Create it directly from your shell or use an external secret manager:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl -n supplychain create secret generic supplychain-secrets \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS='<bootstrap-server>' \
  --from-literal=KAFKA_SASL_USERNAME='<api-key>' \
  --from-literal=KAFKA_SASL_PASSWORD='<api-secret>' \
  --from-literal=DATABASE_URL='<postgresql+psycopg-url>' \
  --from-literal=AISSTREAM_API_KEY='<aisstream-key>' \
  --from-literal=OTEL_EXPORTER_OTLP_ENDPOINT=''
```

`deploy/k8s/secret-template.yaml` documents required keys and is intentionally excluded from Kustomize.

## 5. Docker build

Build all local images:

```bash
make docker-build
```

Build one image explicitly:

```bash
docker build -f services/api-service/Dockerfile -t supplychain/api-service:local .
```

For Kubernetes, build and push the six images to `ghcr.io/nawinfk/supplychain-platform/<service>:<immutable-tag>`, then replace `:latest` in the manifests (or use a Kustomize image override).

## 6. Argo CD deployment

Commit the manifests and image references to `main`, create the Kubernetes Secret out-of-band, then apply the Application:

```bash
kubectl apply -f deploy/argocd/application.yaml
argocd app get supplychain-platform
argocd app sync supplychain-platform
```

Automated sync, pruning, and self-healing are enabled. The application deploys `deploy/k8s` into namespace `supplychain`.

## 7. Logs and runtime checks

```bash
kubectl -n supplychain get pods
kubectl -n supplychain logs deployment/ais-producer --tail=100 -f
kubectl -n supplychain logs deployment/vessel-consumer-worker --tail=100 -f
kubectl -n supplychain logs deployment/risk-worker --tail=100 -f
kubectl -n supplychain port-forward service/api-service 8000:80
curl http://localhost:8000/health
curl http://localhost:8000/ready
curl http://localhost:8000/metrics-summary
```

## 8. Troubleshooting

Kafka:

- Confirm all five topics exist and the API key ACLs cover topic reads/writes plus consumer groups.
- Verify the bootstrap host includes port `9092`, `SASL_SSL` is selected, and key/secret whitespace was not copied.
- Check consumer lag with `confluent kafka consumer group lag describe supplychain.vessel-consumer.v1`.
- Inspect `supplychain.dlq.v1` for validation failures; its envelope includes source topic, partition, offset, payload, and error.

Database:

- Run `curl /ready`; HTTP 503 means PostgreSQL is unavailable to the API.
- Validate the URL uses the `postgresql+psycopg2://` driver and that network policy/firewalls allow port 5432.
- Apply `database/schema.sql` and confirm all six tables with `\dt`.
- If records replay after a crash, that is expected at-least-once behavior. Check `consumer_offsets_audit` and make downstream writes idempotent where business requirements demand it.

Observability logging includes correlation and OpenTelemetry trace IDs when a span exists. `OTEL_EXPORTER_OTLP_ENDPOINT` is reserved for wiring an SDK/exporter in the target environment; no collector is bundled.

## Quality commands

```bash
make test
make lint
make fmt
```

No real credentials belong in this repository. `.env` is ignored; `.env.example` and the Kubernetes Secret template contain placeholders only.
