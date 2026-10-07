
.PHONY: generate build test ci start stop purge explain sqlc seed outbox-status outbox-requeue dlq-status

generate:
	go generate ./...
	go mod tidy

build:
	go build ./...

test:
	cd .dagger && go run . ..

ci:
	docker run --rm -i \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v "$$(pwd):$$(pwd)" -w "$$(pwd)" \
		$$(DOCKER_BUILDKIT=1 docker build -f .dagger/Dockerfile --label test -q .dagger)
	docker rmi -f $$(docker images -q --filter=label=test) || true

start:
	docker compose up --build

debug:
	PROFILE_DOCKERFILE_TARGET=debugger docker compose up --build

stop:
	docker compose down

purge:
	docker compose -f docker-compose.yml down --volumes
sqlc:
	sqlc generate -f ./internal/postgres/sqlc.yaml

# Isi data contoh ke Postgres di docker compose (idempotent, aman diulang).
seed:
	docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"' < ./internal/postgres/seed.sql
# ---------- Operasional outbox & dead-letter queue ----------
PSQL = docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

# Jumlah event outbox per status + daftar event failed beserta error terakhir.
outbox-status:
	@echo "SELECT status, count(*) AS total, max(attempts) AS max_attempts, min(created_at) AS oldest FROM outbox_events GROUP BY status ORDER BY status;" | $(PSQL)
	@echo "SELECT id, aggregate_id, event_type, attempts, left(last_error, 80) AS last_error FROM outbox_events WHERE status = 'failed' ORDER BY created_at LIMIT 20;" | $(PSQL)

# Kirim ulang event failed (sama dengan query RequeueFailedOutboxEvents).
outbox-requeue:
	@echo "UPDATE outbox_events SET status = 'pending', attempts = 0, last_error = NULL, available_at = now() WHERE status = 'failed';" | $(PSQL)

# Isi queue utama & dead-letter queue di RabbitMQ.
dlq-status:
	@docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_unacknowledged consumers

explain:
	@go mod tidy
	@go run github.com/sqlc-dev/sqlc/cmd/sqlc explain -f ./internal/postgres/sqlc.yaml
	@go mod edit -dropreplace github.com/sqlc-dev/sqlc
	@go mod tidy

publish-test:
	docker run --rm --network host eclipse-mosquitto:2 mosquitto_pub -h host.docker.internal -p 11883 -q 1 -t /fleet/vehicle/B1234XYZ/location -m '{"vehicle_id":"B1234XYZ","latitude":-6.1754,"longitude":106.8272,"timestamp":1715003456}' && echo "publish OK"; docker compose logs --since 1m mosquitto | tail -3