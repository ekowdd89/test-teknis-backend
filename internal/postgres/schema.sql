-- =====================================================================
-- 1. Lokasi kendaraan
-- Nama tabel mengikuti spesifikasi tes apa adanya (termasuk typo "loctions").
-- =====================================================================
CREATE TABLE vehicle_loctions (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vehicle_id  VARCHAR(20)      NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    timestamp   BIGINT           NOT NULL, -- unix epoch (detik), sesuai payload
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),

    CONSTRAINT chk_vehicle_loctions_latitude  CHECK (latitude  BETWEEN -90  AND 90),
    CONSTRAINT chk_vehicle_loctions_longitude CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT chk_vehicle_loctions_timestamp CHECK (timestamp > 0),

    -- Idempotensi ingest MQTT (QoS 1 bisa mengirim duplikat).
    -- Index ini juga melayani query lokasi terakhir & riwayat.
    CONSTRAINT uq_vehicle_loctions_vehicle_ts UNIQUE (vehicle_id, timestamp)
);

-- =====================================================================
-- 2. Status kendaraan di dalam geofence
-- Satu baris = kendaraan sedang berada di dalam geofence tersebut.
--   masuk  -> INSERT ... ON CONFLICT DO NOTHING (1 row = event entry)
--   keluar -> DELETE (1 row = event exit)
-- Primary key menjamin event entry tidak ganda walau ada concurrency.
-- =====================================================================
CREATE TABLE vehicle_geofence_presence (
    vehicle_id  VARCHAR(20) NOT NULL,
    geofence_id VARCHAR(64) NOT NULL,
    entered_ts  BIGINT      NOT NULL, -- timestamp lokasi saat masuk
    entered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (vehicle_id, geofence_id)
);

-- =====================================================================
-- 3. Transactional Outbox (sisi server / producer)
-- Event ditulis dalam transaksi yang sama dengan data lokasi,
-- lalu relay mem-publish ke RabbitMQ. Tidak ada event yang hilang
-- walau RabbitMQ sedang down saat lokasi diterima.
-- =====================================================================
CREATE TABLE outbox_events (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(), -- dipakai sebagai AMQP message_id
    aggregate_type VARCHAR(50)  NOT NULL,                  -- 'vehicle'
    aggregate_id   VARCHAR(64)  NOT NULL,                  -- vehicle_id
    event_type     VARCHAR(100) NOT NULL,                  -- 'geofence_entry'
    exchange       VARCHAR(255) NOT NULL,                  -- 'fleet.events'
    routing_key    VARCHAR(255) NOT NULL,                  -- 'geofence.entry'
    headers        JSONB        NOT NULL DEFAULT '{}',     -- metadata, mis. geofence_id
    payload        JSON         NOT NULL,                  -- JSON (bukan JSONB) agar byte & urutan key persis
    dedup_key      VARCHAR(255) NOT NULL,                  -- mencegah event ganda dari producer
    status         VARCHAR(20)  NOT NULL DEFAULT 'pending',
    attempts       INT          NOT NULL DEFAULT 0,
    last_error     TEXT,
    available_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),    -- untuk retry dengan backoff
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,

    CONSTRAINT uq_outbox_events_dedup_key UNIQUE (dedup_key),
    CONSTRAINT chk_outbox_events_status CHECK (status IN ('pending', 'published', 'failed'))
);

-- Relay hanya memindai event pending yang sudah waktunya dikirim.
CREATE INDEX idx_outbox_events_pending
    ON outbox_events (available_at, created_at)
    WHERE status = 'pending';

-- Housekeeping event yang sudah terkirim.
CREATE INDEX idx_outbox_events_published_at
    ON outbox_events (published_at)
    WHERE status = 'published';

-- =====================================================================
-- 4. Inbox (sisi worker / consumer)
-- RabbitMQ menjamin at-least-once; inbox membuat efeknya exactly-once.
-- Insert inbox + efek bisnis + commit dalam satu transaksi, baru ack.
-- Bila message_id sudah ada -> duplikat -> langsung ack tanpa proses.
-- =====================================================================
CREATE TABLE inbox_messages (
    consumer     VARCHAR(100) NOT NULL, -- 'geofence-alert-worker'
    message_id   VARCHAR(255) NOT NULL, -- AMQP message_id (= outbox_events.id)
    event_type   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),

    PRIMARY KEY (consumer, message_id)
);

CREATE INDEX idx_inbox_messages_processed_at ON inbox_messages (processed_at);

-- =====================================================================
-- 5. Hasil pemrosesan worker: log event geofence
-- =====================================================================
CREATE TABLE geofence_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id  VARCHAR(255)     NOT NULL,
    vehicle_id  VARCHAR(20)      NOT NULL,
    geofence_id VARCHAR(64)      NOT NULL,
    event_type  VARCHAR(100)     NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    timestamp   BIGINT           NOT NULL,
    received_at TIMESTAMPTZ      NOT NULL DEFAULT now(),

    CONSTRAINT uq_geofence_events_message_id UNIQUE (message_id)
);

CREATE INDEX idx_geofence_events_vehicle_ts
    ON geofence_events (vehicle_id, timestamp DESC);
