-- Rollback schema.sql (urutan kebalikan dari pembuatan tabel).
-- Tidak dibaca sqlc; jalankan manual bila perlu reset database.
DROP TABLE IF EXISTS geofence_events;
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS vehicle_geofence_presence;
DROP TABLE IF EXISTS vehicle_loctions;
