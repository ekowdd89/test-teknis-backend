-- name: InsertInboxMessage :execrows
-- Langkah pertama worker di dalam transaksi.
-- 0 baris = pesan sudah pernah diproses -> commit & ack tanpa memproses ulang.
INSERT INTO inbox_messages (consumer, message_id, event_type)
VALUES (@consumer, @message_id, @event_type)
ON CONFLICT (consumer, message_id) DO NOTHING;

-- name: DeleteProcessedInboxMessages :execrows
-- Housekeeping. Retensi harus lebih lama dari kemungkinan redelivery.
DELETE FROM inbox_messages
WHERE processed_at < @before::timestamptz;

-- name: InsertOutboxEvent :execrows
-- Dipanggil dalam transaksi ingest. dedup_key contoh:
-- 'geofence_entry:B1234XYZ:monas:1715003456'
INSERT INTO outbox_events (
    aggregate_type, aggregate_id, event_type,
    exchange, routing_key, headers, payload, dedup_key
) VALUES (
    @aggregate_type, @aggregate_id, @event_type,
    @exchange, @routing_key, @headers, @payload, @dedup_key
)
ON CONFLICT (dedup_key) DO NOTHING;

-- name: ClaimPendingOutboxEvents :many
-- Dijalankan relay di dalam transaksi. SKIP LOCKED membuat beberapa
-- instance relay aman berjalan paralel tanpa mengirim event yang sama.
SELECT id, aggregate_type, aggregate_id, event_type, exchange, routing_key, headers, payload, attempts, created_at
FROM outbox_events
WHERE status = 'pending' AND available_at <= now()
ORDER BY created_at, id
LIMIT @batch_size::int
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxEventPublished :exec
UPDATE outbox_events
SET status       = 'published',
    attempts     = attempts + 1,
    published_at = now(),
    last_error   = NULL
WHERE id = @id;

-- name: MarkOutboxEventFailed :exec
-- Retry dengan backoff; setelah max_attempts status menjadi 'failed'.
-- Di klausa SET, "attempts" selalu bernilai lama (sebelum update).
UPDATE outbox_events
SET attempts     = attempts + 1,
    last_error   = sqlc.arg(last_error)::text,
    status       = CASE
                       WHEN attempts + 1 >= sqlc.arg(max_attempts)::int THEN 'failed'
                       ELSE 'pending'
                   END,
    available_at = now() + (sqlc.arg(backoff_seconds)::int * INTERVAL '1 second')
WHERE id = sqlc.arg(id);

-- name: RequeueFailedOutboxEvents :execrows
-- Operasional: kirim ulang event yang gagal permanen setelah masalah diperbaiki.
UPDATE outbox_events
SET status       = 'pending',
    attempts     = 0,
    last_error   = NULL,
    available_at = now()
WHERE status = 'failed';

-- name: DeletePublishedOutboxEvents :execrows
DELETE FROM outbox_events
WHERE status = 'published'
    AND published_at < @before::timestamptz;

-- name: CountOutboxEventsByStatus :many
SELECT status, count(*)::bigint AS total
FROM outbox_events
GROUP BY status
ORDER BY status;


-- name: EnterGeofence :execrows
-- 1 baris = kendaraan baru masuk -> tulis outbox event geofence_entry.
-- 0 baris = sudah di dalam sejak sebelumnya -> tidak ada event.
INSERT INTO vehicle_geofence_presence (vehicle_id, geofence_id, entered_ts)
VALUES (@vehicle_id, @geofence_id, @entered_ts)
ON CONFLICT (vehicle_id, geofence_id) DO NOTHING;

-- name: ExitGeofence :execrows
-- 1 baris = kendaraan baru keluar (bisa dipakai untuk event geofence_exit).
DELETE FROM vehicle_geofence_presence
WHERE vehicle_id = @vehicle_id AND geofence_id = @geofence_id;

-- name: ListVehicleGeofencePresence :many
SELECT geofence_id, entered_ts, entered_at
FROM vehicle_geofence_presence
WHERE vehicle_id = @vehicle_id
ORDER BY geofence_id;

-- name: InsertGeofenceEvent :execrows
-- Dipanggil worker dalam transaksi yang sama dengan InsertInboxMessage.
INSERT INTO geofence_events (
    message_id, vehicle_id, geofence_id, event_type, latitude, longitude, timestamp
) VALUES (
    @message_id, @vehicle_id, @geofence_id, @event_type, @latitude, @longitude, @timestamp
)
ON CONFLICT (message_id) DO NOTHING;

-- name: ListGeofenceEventsByVehicle :many
SELECT id, message_id, vehicle_id, geofence_id, event_type, latitude, longitude, timestamp, received_at
FROM geofence_events
WHERE vehicle_id = @vehicle_id
ORDER BY timestamp DESC
LIMIT @max_rows::int;


-- name: InsertVehicleLocation :execrows
-- 0 baris = duplikat (pesan MQTT QoS 1 terkirim ulang) -> lewati geofence.
INSERT INTO vehicle_loctions (vehicle_id, latitude, longitude, timestamp)
VALUES (@vehicle_id, @latitude, @longitude, @timestamp)
ON CONFLICT (vehicle_id, timestamp) DO NOTHING;

-- name: GetLatestVehicleLocation :one
SELECT vehicle_id, latitude, longitude, timestamp
FROM vehicle_loctions
WHERE vehicle_id = @vehicle_id
ORDER BY timestamp DESC
LIMIT 1;

-- name: ListVehicleLocationHistory :many
SELECT vehicle_id, latitude, longitude, timestamp
FROM vehicle_loctions
WHERE vehicle_id = @vehicle_id
  AND timestamp BETWEEN @start_ts::bigint AND @end_ts::bigint
ORDER BY timestamp ASC
LIMIT @max_rows::int;