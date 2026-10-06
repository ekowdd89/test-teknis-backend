-- =====================================================================
-- Data contoh untuk development / demo.
-- Jalankan: make seed
--
-- Idempotent: aman dijalankan berulang (semua insert ON CONFLICT DO NOTHING
-- dan ID dibuat deterministik dari dedup_key).
--
-- Isi:
--   B1234XYZ  Bundaran HI -> Monas (masuk 2 geofence)
--   B5678ABC  melintasi Monas dari selatan ke utara (masuk 1 geofence)
--   B9012DEF  menuju Blok M dan berhenti di dalamnya (masuk 1 geofence,
--             masih tercatat di vehicle_geofence_presence)
-- Satu titik lokasi tiap 10 detik, timestamp di sekitar 1715003456 agar
-- contoh di README / OpenAPI (start=1715000000&end=1715009999) langsung berisi data.
-- =====================================================================

BEGIN;

-- Titik geofence & radius disamakan dengan GEOFENCE_POINTS dan
-- GEOFENCE_RADIUS_METERS di docker-compose.yml.
CREATE TEMP TABLE seed_geofences (geofence_id, latitude, longitude, radius_m) ON COMMIT DROP AS
VALUES
    ('bundaran-hi', -6.1950::double precision, 106.8230::double precision, 50::double precision),
    ('monas',       -6.1754,                   106.8272,                   50),
    ('blok-m',      -6.2443,                   106.8000,                   50);

-- Rute garis lurus dari (from) ke (to) sebanyak n titik, mulai ts0.
CREATE TEMP TABLE seed_routes (vehicle_id, from_lat, from_lon, to_lat, to_lon, n, ts0) ON COMMIT DROP AS
VALUES
    -- Segaris dengan Bundaran HI -> Monas, sedikit diperpanjang di kedua ujung.
    ('B1234XYZ', -6.19892::double precision, 106.82216::double precision, -6.17207::double precision, 106.82791::double precision, 120, 1715003000::bigint),
    ('B5678ABC', -6.18540,                   106.82720,                   -6.16540,                   106.82720,                   90,  1715003100),
    -- Berhenti ~10 m dari pusat Blok M.
    ('B9012DEF', -6.23430,                   106.80000,                   -6.24421,                   106.80000,                   50,  1715003200);

-- ---------- 1. Lokasi kendaraan ----------
INSERT INTO vehicle_loctions (vehicle_id, latitude, longitude, timestamp)
SELECT r.vehicle_id,
       round((r.from_lat + (r.to_lat - r.from_lat) * i / (r.n - 1))::numeric, 6)::double precision,
       round((r.from_lon + (r.to_lon - r.from_lon) * i / (r.n - 1))::numeric, 6)::double precision,
       r.ts0 + i * 10
FROM seed_routes r
CROSS JOIN LATERAL generate_series(0, r.n - 1) AS i
ON CONFLICT (vehicle_id, timestamp) DO NOTHING;

-- ---------- 2. Deteksi masuk geofence (haversine) ----------
-- Event entry = titik pertama di dalam radius untuk tiap (kendaraan, geofence).
CREATE TEMP TABLE seed_entries ON COMMIT DROP AS
WITH distances AS (
    SELECT l.vehicle_id, g.geofence_id, l.latitude, l.longitude, l.timestamp,
           2 * 6371000 * asin(sqrt(
               power(sin(radians(l.latitude - g.latitude) / 2), 2) +
               cos(radians(g.latitude)) * cos(radians(l.latitude)) *
               power(sin(radians(l.longitude - g.longitude) / 2), 2)
           )) AS distance_m,
           g.radius_m
    FROM vehicle_loctions l
    JOIN seed_routes r ON r.vehicle_id = l.vehicle_id
    CROSS JOIN seed_geofences g
)
SELECT DISTINCT ON (vehicle_id, geofence_id)
       vehicle_id, geofence_id, latitude, longitude, timestamp,
       format('geofence_entry:%s:%s:%s', vehicle_id, geofence_id, timestamp) AS dedup_key,
       -- UUID deterministik agar rerun menghasilkan ID yang sama.
       md5(format('geofence_entry:%s:%s:%s', vehicle_id, geofence_id, timestamp))::uuid AS event_id
FROM distances
WHERE distance_m <= radius_m
ORDER BY vehicle_id, geofence_id, timestamp;

-- ---------- 3. Outbox (sudah terkirim ke RabbitMQ) ----------
-- Payload mengikuti schema GeofenceAlertMessage di api/openapi-spec.yaml.
INSERT INTO outbox_events (
    id, aggregate_type, aggregate_id, event_type, exchange, routing_key,
    headers, payload, dedup_key, status, attempts, published_at
)
SELECT e.event_id, 'vehicle', e.vehicle_id, 'geofence_entry', 'fleet.events', 'geofence.entry',
       jsonb_build_object('geofence_id', e.geofence_id),
       json_build_object(
           'vehicle_id', e.vehicle_id,
           'event', 'geofence_entry',
           'location', json_build_object('latitude', e.latitude, 'longitude', e.longitude),
           'timestamp', e.timestamp
       ),
       e.dedup_key, 'published', 1, now()
FROM seed_entries e
ON CONFLICT (dedup_key) DO NOTHING;

-- ---------- 4. Inbox worker (message_id = outbox_events.id) ----------
INSERT INTO inbox_messages (consumer, message_id, event_type)
SELECT 'geofence-alert-worker', e.event_id::text, 'geofence_entry'
FROM seed_entries e
ON CONFLICT (consumer, message_id) DO NOTHING;

-- ---------- 5. Hasil proses worker ----------
INSERT INTO geofence_events (message_id, vehicle_id, geofence_id, event_type, latitude, longitude, timestamp)
SELECT e.event_id::text, e.vehicle_id, e.geofence_id, 'geofence_entry', e.latitude, e.longitude, e.timestamp
FROM seed_entries e
ON CONFLICT (message_id) DO NOTHING;

-- ---------- 6. Kendaraan yang masih di dalam geofence ----------
-- Hanya bila titik terakhir kendaraan masih di dalam radius.
INSERT INTO vehicle_geofence_presence (vehicle_id, geofence_id, entered_ts)
SELECT e.vehicle_id, e.geofence_id, e.timestamp
FROM seed_entries e
JOIN LATERAL (
    SELECT latitude, longitude
    FROM vehicle_loctions
    WHERE vehicle_id = e.vehicle_id
    ORDER BY timestamp DESC
    LIMIT 1
) last ON true
JOIN seed_geofences g ON g.geofence_id = e.geofence_id
WHERE 2 * 6371000 * asin(sqrt(
          power(sin(radians(last.latitude - g.latitude) / 2), 2) +
          cos(radians(g.latitude)) * cos(radians(last.latitude)) *
          power(sin(radians(last.longitude - g.longitude) / 2), 2)
      )) <= g.radius_m
ON CONFLICT (vehicle_id, geofence_id) DO NOTHING;

COMMIT;

-- Ringkasan
SELECT 'vehicle_loctions' AS "table", count(*) AS total FROM vehicle_loctions
UNION ALL SELECT 'geofence_events', count(*) FROM geofence_events
UNION ALL SELECT 'outbox_events', count(*) FROM outbox_events
UNION ALL SELECT 'inbox_messages', count(*) FROM inbox_messages
UNION ALL SELECT 'vehicle_geofence_presence', count(*) FROM vehicle_geofence_presence;
