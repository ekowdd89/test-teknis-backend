
.PHONY: generate build test ci start stop purge explain sqlc seed outbox-status outbox-requeue dlq-status publish-test trace-flow rabbit-trace-start rabbit-trace-log rabbit-trace-stop publish-rabbit

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
	docker compose up -d --build

debug:
	PROFILE_DOCKERFILE_TARGET=debugger docker compose up -d --build

stop:
	docker compose down

purge:
	docker compose -f docker-compose.yml down --volumes --remove-orphans
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

PSQL_VALUE = docker compose exec -T postgres sh -c 'psql -tA -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'
# VEHICLE hanya dipakai bila diisi di command line (default VEHICLE milik publish-test diabaikan).
TRACE_VEHICLE = $(if $(filter command line environment,$(origin VEHICLE)),$(VEHICLE),)

# Telusuri satu event geofence dari MQTT sampai worker (read-only):
#   make trace-flow                    # event geofence terbaru
#   make trace-flow VEHICLE=B5678ABC   # event terbaru kendaraan tertentu
trace-flow:
	@V="$(TRACE_VEHICLE)"; \
	if ! echo "$$V" | grep -Eq '^[A-Za-z0-9-]{0,20}$$'; then echo "VEHICLE tidak valid: $$V"; exit 1; fi; \
	if [ -n "$$V" ]; then WHERE="WHERE vehicle_id = '$$V'"; fi; \
	ROW=$$(echo "SELECT message_id || '|' || vehicle_id || '|' || timestamp FROM geofence_events $$WHERE ORDER BY received_at DESC LIMIT 1;" | $(PSQL_VALUE)); \
	if [ -z "$$ROW" ]; then \
		if [ -z "$$V" ]; then echo "Belum ada event geofence."; exit 0; fi; \
		echo "Kendaraan $$V belum punya event geofence. Lokasi terakhirnya:"; \
		echo "SELECT vehicle_id, latitude, longitude, timestamp, created_at FROM vehicle_loctions WHERE vehicle_id = '$$V' ORDER BY timestamp DESC LIMIT 3;" | $(PSQL); \
		echo "Lokasi yang tidak berada dalam radius geofence hanya disimpan di PostgreSQL dan TIDAK dikirim ke RabbitMQ."; \
		exit 0; \
	fi; \
	MID=$${ROW%%|*}; REST=$${ROW#*|}; VID=$${REST%%|*}; TS=$${REST#*|}; \
	echo "Event: message_id=$$MID vehicle=$$VID timestamp=$$TS"; \
	echo; echo "[1] MQTT -> server ingest -> vehicle_loctions"; \
	echo "SELECT vehicle_id, latitude, longitude, timestamp, created_at FROM vehicle_loctions WHERE vehicle_id = '$$VID' AND timestamp = $$TS;" | $(PSQL); \
	echo "[2] Cek geofence -> vehicle_geofence_presence (kosong = kendaraan sudah keluar lagi)"; \
	echo "SELECT vehicle_id, geofence_id, entered_ts, entered_at FROM vehicle_geofence_presence WHERE vehicle_id = '$$VID';" | $(PSQL); \
	echo "[3] outbox_events (transaksi yang sama dengan [1]) dan [4] relay server -> RabbitMQ (published_at)"; \
	echo "SELECT id, exchange, routing_key, headers, status, attempts, created_at, published_at FROM outbox_events WHERE id::text = '$$MID';" | $(PSQL); \
	echo "[5] RabbitMQ -> worker -> inbox_messages + geofence_events"; \
	echo "SELECT consumer, message_id, processed_at FROM inbox_messages WHERE message_id = '$$MID';" | $(PSQL); \
	echo "SELECT vehicle_id, geofence_id, event_type, latitude, longitude, timestamp, received_at FROM geofence_events WHERE message_id = '$$MID';" | $(PSQL); \
	echo "Latensi"; \
	echo "SELECT o.published_at - o.created_at AS outbox_ke_rabbitmq, i.processed_at - o.published_at AS rabbitmq_ke_worker, i.processed_at - o.created_at AS total FROM outbox_events o JOIN inbox_messages i ON i.message_id = o.id::text WHERE o.id::text = '$$MID';" | $(PSQL); \
	echo "Log server & worker"; \
	LOGS=$$(docker compose logs --no-log-prefix server worker 2>/dev/null | grep -E "$$MID|\"vehicle_id\":\"$$VID\",\"geofence_id\":\"[^\"]*\",\"timestamp\":$$TS" | sort | cut -c1-220); \
	if [ -n "$$LOGS" ]; then echo "$$LOGS"; else echo "(tidak ada log: event dari make seed, atau log container sudah terhapus)"; fi

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

# Kirim satu lokasi uji ke MQTT lalu tampilkan hasilnya dari REST API.
# Timestamp selalu "sekarang" (vehicle_id + timestamp yang sama dianggap duplikat).
# Default TEST-01 di Monas: kendaraan yang tidak digerakkan publisher mock.
#   make publish-test
#   make publish-test VEHICLE=B4321ABC LAT=-8.1754 LON=109.8272
VEHICLE ?= B9012DEF
LAT ?= -6.241013333333333
LON ?= 106.80153333333332
API ?= http://localhost:$${HTTP_PORT:-8000}

publish-test:
	@TS=$$(date +%s); \
	docker compose exec -T mosquitto mosquitto_pub -q 1 \
		-t /fleet/vehicle/$(VEHICLE)/location \
		-m "{\"vehicle_id\":\"$(VEHICLE)\",\"latitude\":$(LAT),\"longitude\":$(LON),\"timestamp\":$$TS}" \
	&& echo "publish OK: $(VEHICLE) lat=$(LAT) lon=$(LON) timestamp=$$TS"
	@sleep 2
	@echo "--- GET /vehicles/$(VEHICLE)/location"
	@curl -s $(API)/vehicles/$(VEHICLE)/location; echo
	@echo "--- GET /vehicles/$(VEHICLE)/geofence-events"
	@curl -s $(API)/vehicles/$(VEHICLE)/geofence-events; echo

# ---------- Memantau pesan di RabbitMQ (dev) ----------
# Hanya event geofence_entry (kendaraan BARU masuk radius) yang dikirim ke RabbitMQ;
# lokasi biasa tidak. Queue geofence_alerts tampak kosong karena worker langsung ack.
RABBIT_API ?= http://localhost:$${RABBITMQ_UI_HOST_PORT:-15673}/api
RABBIT_USER ?= $${RABBITMQ_USER:-fleet}
RABBIT_PASS ?= $${RABBITMQ_PASSWORD:-fleet}
RABBIT_CURL = curl -s -u "$(RABBIT_USER):$(RABBIT_PASS)" -H 'content-type: application/json'
RABBIT_TRACE ?= fleet-events

# Mulai mencatat setiap pesan yang di-publish ke exchange fleet.events
# (butuh plugin rabbitmq_tracing, aktif lewat docker-compose.override.yaml).
# Kredensial tracer dikirim per-trace: default plugin login sebagai "guest" yang tidak ada.
rabbit-trace-start:
	@$(RABBIT_CURL) -o /dev/null -w "create trace $(RABBIT_TRACE): HTTP %{http_code}\n" -X PUT $(RABBIT_API)/traces/%2F/$(RABBIT_TRACE) \
		-d "{\"format\":\"json\",\"pattern\":\"publish.fleet.events\",\"tracer_connection_username\":\"$(RABBIT_USER)\",\"tracer_connection_password\":\"$(RABBIT_PASS)\"}"
	@echo "Lihat juga di UI: Admin -> Tracing. Baca log: make rabbit-trace-log"

# Tampilkan pesan yang tercatat trace (satu baris per pesan).
rabbit-trace-log:
	@$(RABBIT_CURL) -f $(RABBIT_API)/trace-files/$(RABBIT_TRACE).log \
		| jq -c '{time: .timestamp, exchange, routing_key: .routing_keys[0], routed_queues, message_id: .properties.message_id, geofence_id: .properties.headers.geofence_id, payload: (.payload | @base64d | fromjson)}' \
		|| echo "Trace belum dimulai atau belum ada pesan (jalankan: make rabbit-trace-start, lalu kirim kendaraan BARU ke geofence)."

# Hentikan trace dan hapus file log-nya.
rabbit-trace-stop:
	@$(RABBIT_CURL) -o /dev/null -w "delete trace: HTTP %{http_code}\n" -X DELETE $(RABBIT_API)/traces/%2F/$(RABBIT_TRACE)
	@$(RABBIT_CURL) -o /dev/null -w "delete log:   HTTP %{http_code}\n" -X DELETE $(RABBIT_API)/trace-files/$(RABBIT_TRACE).log

# Demo pesan yang TERTAHAN di queue geofence_alerts: worker dihentikan sebentar,
# kendaraan baru dikirim ke Monas, isi queue ditampilkan, lalu worker dinyalakan lagi.
# Worker selalu dinyalakan kembali walau ada langkah yang gagal (trap EXIT).
publish-rabbit:
	@trap 'docker compose start worker >/dev/null 2>&1' EXIT; \
	V=RB-$$(date +%H%M%S); \
	docker compose stop worker >/dev/null 2>&1 && echo "1) worker dihentikan"; \
	$(MAKE) -s publish-test VEHICLE=$$V >/dev/null && echo "2) lokasi $$V dikirim ke MQTT (titik Monas)"; \
	sleep 2; \
	echo "3) queue (name / messages / consumers):"; \
	docker compose exec -T rabbitmq rabbitmqctl list_queues -q name messages consumers | grep -E '^geofence_alerts\s'; \
	echo "4) isi pesan di geofence_alerts (dibaca lalu dikembalikan ke queue):"; \
	$(RABBIT_CURL) -X POST $(RABBIT_API)/queues/%2F/geofence_alerts/get \
		-d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}' \
		| jq '.[] | {routing_key, message_id: .properties.message_id, headers: .properties.headers, payload: (.payload | fromjson)}'; \
	docker compose start worker >/dev/null 2>&1 && echo "5) worker dinyalakan lagi"; \
	sleep 4; \
	docker compose exec -T rabbitmq rabbitmqctl list_queues -q name messages consumers | grep -E '^geofence_alerts\s'; \
	docker compose logs --no-log-prefix --since 10s worker | grep "geofence alert received" | jq -c '{msg, message_id, vehicle_id, geofence_id}'

