# Fleet Management Backend

Backend sistem manajemen armada (studi kasus Transjakarta). Lokasi kendaraan
diterima lewat **MQTT**, disimpan ke **PostgreSQL**, dicek terhadap **geofence**
(radius 50 m), lalu event geofence dikirim ke **RabbitMQ** dan diproses oleh
**worker**. Data dapat diakses lewat **REST API** (Gin + OpenAPI).

---

## Daftar Isi

1. [Arsitektur](#arsitektur)
2. [Jenis Arsitektur & Alasan](#jenis-arsitektur--alasan)
3. [Alur Data](#alur-data)
4. [Arsitektur Kode & Design Pattern](#arsitektur-kode--design-pattern)
5. [Struktur Direktori](#struktur-direktori)
6. [Teknologi & Library](#teknologi--library)
7. [Skema Database](#skema-database)
8. [sqlc (Query Type-Safe)](#sqlc-query-type-safe)
9. [Penggunaan Library Internal](#penggunaan-library-internal)
10. [REST API](#rest-api)
11. [Konfigurasi (Environment Variable)](#konfigurasi-environment-variable)
12. [Menjalankan Aplikasi](#menjalankan-aplikasi)
13. [Perintah Makefile](#perintah-makefile)
14. [Alur startup server](#alur-startup-server)
15. [Status Implementasi & Catatan](#status-implementasi--catatan)

---

## Arsitektur

```mermaid
flowchart TB
    vehicle["Kendaraan / publisher"]
    client["Client"]

    subgraph edge["Edge"]
        caddy["Caddy :80"]
        swagger["Swagger UI<br/>/docs"]
    end

    subgraph broker["Message Broker"]
        mqtt[("Mosquitto<br/>MQTT :1883")]
        rabbit[("RabbitMQ :5672 / :15672<br/>exchange: fleet.events<br/>queue: geofence_alerts")]
    end

    subgraph app["Aplikasi"]
        server["server<br/>- MQTT ingest<br/>- cek geofence (50 m)<br/>- outbox relay<br/>- REST API (Gin :8080)"]
        worker["worker"]
    end

    db[("PostgreSQL :5432")]

    vehicle -- "publish QoS 1<br/>/fleet/vehicle/{vehicle_id}/location" --> mqtt
    mqtt -- "subscribe" --> server
    server <-- "transaksi: lokasi + presence + outbox" --> db
    server -- "publish geofence.entry<br/>(publisher confirms)" --> rabbit
    rabbit -- "consume" --> worker
    worker -- "transaksi: inbox + geofence_events" --> db

    client --> caddy
    caddy -- "REST" --> server
    caddy -- "/docs" --> swagger
```

Satu image Docker berisi tiga binary: `server`, `worker`, dan `publisher`
(mock pengirim lokasi untuk demo). Semua service dijalankan lewat Docker Compose.

### Pola keandalan pesan

| Pola | Lokasi | Tujuan |
| --- | --- | --- |
| **Idempotent ingest** | `vehicle_loctions` `UNIQUE (vehicle_id, timestamp)` | MQTT QoS 1 bisa mengirim pesan duplikat; insert kedua menghasilkan 0 baris dan diabaikan. |
| **Presence table** | `vehicle_geofence_presence` PK `(vehicle_id, geofence_id)` | Event `geofence_entry` hanya dibuat **sekali** saat kendaraan baru masuk, aman walau ada concurrency. |
| **Transactional Outbox** | `outbox_events` | Event ditulis dalam transaksi yang sama dengan data lokasi. Bila RabbitMQ down, event tidak hilang; relay akan mengirim ulang. |
| **`FOR UPDATE SKIP LOCKED`** | `ClaimPendingOutboxEvents` | Beberapa instance relay bisa berjalan paralel tanpa mengirim event yang sama. |
| **Retry + backoff** | `MarkOutboxEventFailed` | Event gagal dicoba ulang dengan jeda; setelah `max_attempts` statusnya `failed`. |
| **Inbox (idempotent consumer)** | `inbox_messages` PK `(consumer, message_id)` | RabbitMQ menjamin *at-least-once*; inbox membuat efek bisnis menjadi *exactly-once*. |

---

## Jenis Arsitektur & Alasan

Sistem ini bukan satu "jenis arsitektur" tunggal. Ia menggabungkan beberapa gaya, masing-masing
dipilih untuk masalah tertentu. Ringkasnya:

| Lingkup | Jenis arsitektur | Wujud di proyek ini |
| --- | --- | --- |
| Sistem (antar proses) | **Event-Driven Architecture** | MQTT untuk data masuk, RabbitMQ untuk event `geofence_entry` |
| Deployment | **Modular monolith, multi-proses** | Satu codebase & satu image, dijalankan sebagai `server`, `worker`, `publisher` |
| Kode (dalam proses) | **Layered + Ports & Adapters (ringan)** | `cmd` → `internal/cmd` → `httpserver`/`fleet` → `postgres` → `pkg` |
| Data | **Pemisahan jalur tulis & baca (CQRS ringan)** | Tulis lewat MQTT/worker, baca lewat REST |
| Edge | **Reverse proxy + contract-first API** | Caddy di depan Gin; API setup dari OpenAPI |

### 1. Event-Driven Architecture (EDA)

**Bentuknya:** komponen tidak saling memanggil langsung. Kendaraan *mengirim event lokasi*
ke broker MQTT, dan server bereaksi. Saat kendaraan masuk geofence, server *menerbitkan event*
`geofence_entry` ke RabbitMQ, lalu worker bereaksi. Tidak ada komponen yang menunggu
balasan dari komponen lain.

**Alasan:**
- **Sesuai sifat datanya.** Lokasi kendaraan adalah aliran kejadian yang terus-menerus,
  bukan request yang menunggu jawaban. Kendaraan tidak perlu tahu siapa yang memproses datanya.
- **Decoupling.** Publisher, server, dan worker bisa di-deploy, di-restart, dan diskalakan
  sendiri-sendiri. Worker mati tidak menghentikan ingest. Pesan menunggu di queue.
- **Ketahanan terhadap gangguan.** MQTT QoS 1 dan sesi persisten menahan pesan saat server
  restart. Antrean RabbitMQ yang *durable* menahan event saat worker mati.
- **Mudah menambah konsumen baru.** Misalnya notifikasi atau analitik cukup membuat queue
  baru yang di-*bind* ke exchange `fleet.events`, tanpa mengubah server.

**Kenapa dua broker (MQTT *dan* RabbitMQ)?**

| | MQTT (Mosquitto) | RabbitMQ (AMQP) |
| --- | --- | --- |
| Peran | Jalur **masuk** dari perangkat | Jalur **event internal** antar layanan |
| Alasan dipilih | Protokol ringan untuk perangkat IoT/kendaraan dengan jaringan tidak stabil; topik wildcard per kendaraan | Routing (exchange/binding), *publisher confirm*, ack per pesan, dead-letter queue, competing consumers |
| Yang tidak dimiliki pihak lain | — | MQTT tidak punya nack, DLQ, maupun routing berbasis exchange |

**Trade-off:** sistem bersifat *eventually consistent*. `geofence_events` baru terisi sekitar
0,5 detik setelah lokasi masuk. Alurnya juga lebih sulit dilacak daripada pemanggilan langsung,
karena itu disediakan `make trace-flow`.

### 2. Modular monolith yang dijalankan sebagai beberapa proses

**Bentuknya:** satu repository, satu `go.mod`, dan satu image Docker berisi tiga binary.
`server` menangani ingest MQTT, outbox relay, dan REST. `worker` mengonsumsi RabbitMQ.
`publisher` adalah mock. Ketiganya memakai package `internal/` yang sama dan **satu database**.

**Alasan:**
- **Skala masalahnya kecil.** Satu domain (armada & geofence), tiga tabel inti, satu tim.
  Microservices penuh hanya menambah biaya operasional (banyak repo, banyak database,
  kontrak antar layanan) tanpa manfaat sepadan.
- **Tetap bisa diskalakan per peran.** Karena `server` dan `worker` adalah proses terpisah,
  beban consume bisa ditambah dengan menjalankan lebih banyak `worker` tanpa menggandakan REST API.
- **Konsistensi kode.** Model pesan (`fleet.GeofenceAlertMessage`) dan topologi RabbitMQ
  (`fleet.RabbitTopology`) dipakai bersama oleh server dan worker, jadi keduanya tidak mungkin
  tidak sinkron.
- **Jalan menuju pemisahan tetap terbuka.** Batasnya sudah jelas lewat event. Bila kelak
  perlu dipisah, worker bisa menjadi layanan sendiri dengan database sendiri tanpa mengubah
  kontrak pesannya.

**Kenapa bukan microservices:** `server` dan `worker` **berbagi satu database PostgreSQL**.
Dalam microservices yang ketat, tiap layanan memiliki datanya sendiri. Ini pilihan sadar demi
kesederhanaan, dan tercatat sebagai batasan.

### 3. Layered + Ports & Adapters (ringan) di dalam kode

**Bentuknya:** dependensi hanya mengarah ke bawah (lihat
[Lapisan dan arah dependensi](#lapisan-dan-arah-dependensi)). Logika bisnis ada di
`internal/fleet`. Cara data masuk dan keluar dipasang sebagai **adapter**: HTTP (`httpserver`),
MQTT handler (`Ingestor.MQTTHandler`), consumer RabbitMQ (`Consumer.Handle`). Semuanya
dirakit di satu **composition root** (`internal/cmd`).

**Alasan:**
- **Logika bisnis tidak bergantung pada transport.** Aturan geofence dan outbox tidak tahu
  apakah datanya datang dari MQTT, test, atau sumber lain di masa depan.
- **Mudah dites.** Handler HTTP dites dengan `sqlc.Querier` palsu tanpa database. Relay hanya
  bergantung pada interface kecil `fleet.Publisher` (2 method).
- **Mudah dipahami developer baru.** Setiap folder punya satu tanggung jawab yang jelas.

**Kenapa disebut "ringan":** `internal/fleet` masih memakai `postgres.Postgres` dan query
sqlc secara langsung, tanpa interface repository di antaranya. Hexagonal/Clean Architecture
yang ketat akan menambah lapisan abstraksi itu. Untuk ukuran proyek ini, abstraksi tersebut
menambah kode tanpa manfaat nyata. sqlc sudah memberi `Querier` sebagai interface bila diperlukan.

### 4. Pemisahan jalur Write dan Read (CQRS ringan)

**Bentuknya:**
- **Jalur tulis:** MQTT → transaksi ingest → outbox → RabbitMQ → worker → `geofence_events`.
- **Jalur baca:** REST API hanya `SELECT` dari `vehicle_loctions` dan `geofence_events`.
  REST tidak pernah menulis.

`geofence_events` berperan sebagai *read model* yang dibangun dari event oleh worker.

**Alasan:**
- **Beban Write yang tinggi** (setiap kendaraan mengirim lokasi tiap beberapa detik) tidak
  bersaing dengan logika API. API cukup query sederhana dengan index yang tepat.
- **Read model bisa dibangun ulang dari event**, dan konsumen lain bisa membangun read model
  berbeda dari event yang sama.

**Kenapa "ringan":** kedua jalur masih memakai database yang sama. CQRS penuh biasanya memisahkan
penyimpanan tulis dan baca.

### 5. Reverse proxy & contract-first API di edge

**Bentuknya:** Caddy menerima semua request di port 8000, meneruskan REST ke Gin dan `/docs`
ke Swagger UI. Kontrak API ditulis dulu di `api/openapi-spec.yaml`, lalu kode server
dibangkitkan dengan `oapi-codegen`.

**Alasan:**
- **Satu pintu masuk.** Kompresi, logging akses, dan (kelak) TLS dikelola di satu tempat,
  bukan di setiap layanan.
- **Spesifikasi menjadi sumber kebenaran.** Dokumentasi Swagger dan implementasi tidak bisa
  berbeda. Endpoint baru di spec yang belum diimplementasikan menyebabkan gagal compile.

### Pola keandalan yang menopang arsitektur ini

EDA dengan broker berarti pesan bisa terkirim ulang atau hilang di tengah jalan. Karena itu
arsitektur ini **membutuhkan** pola pendukung berikut (detail di
[Katalog pattern lainnya](#katalog-pattern-lainnya)):

| Masalah EDA | Pola yang dipakai |
| --- | --- |
| Lokasi tersimpan tapi event hilang saat RabbitMQ mati | **Transactional Outbox** |
| Pesan terkirim lebih dari sekali (at-least-once) | **Idempotent Receiver** (`UNIQUE`) & **Inbox** |
| Pesan rusak diulang terus-menerus | **Dead Letter Queue** |
| Beberapa instance relay mengirim event yang sama | **Competing Consumers** (`FOR UPDATE SKIP LOCKED`) |

### Kapan arsitektur ini perlu diubah

| Kondisi | Perubahan yang disarankan |
| --- | --- |
| Ribuan kendaraan, polling outbox 1 detik jadi bottleneck | Ganti polling dengan *Change Data Capture* (mis. Debezium) atau `LISTEN/NOTIFY` |
| Worker butuh skala dan siklus rilis sendiri | Pisahkan worker menjadi layanan dengan database sendiri (menuju microservices) |
| Query riwayat lokasi sangat besar | Pindahkan `vehicle_loctions` ke time-series DB (mis. TimescaleDB), atau partisi tabel per waktu |
| Banyak konsumen event yang berbeda | Tambah queue baru yang di-*bind* ke `fleet.events`; server tidak perlu diubah |

---

## Alur Data

1. **Ingest (server)** — menerima pesan MQTT `/fleet/vehicle/{id}/location`, lalu dalam **satu transaksi**:
   1. `InsertVehicleLocation` → jika 0 baris (duplikat), berhenti.
   2. Hitung jarak ke setiap titik geofence (`GEOFENCE_POINTS`, radius `GEOFENCE_RADIUS_METERS`).
   3. Di dalam radius → `EnterGeofence`; jika 1 baris (baru masuk) → `InsertOutboxEvent`.
      Di luar radius → `ExitGeofence`.
   4. Commit.
2. **Outbox relay (server)** — secara periodik, dalam transaksi:
   `ClaimPendingOutboxEvents` → publish ke RabbitMQ (`message_id` = `outbox_events.id`) →
   `MarkOutboxEventPublished` atau `MarkOutboxEventFailed`.
3. **Worker** — consume queue `geofence_alerts`, dalam satu transaksi:
   `InsertInboxMessage` → jika 0 baris (sudah diproses) langsung ack;
   jika 1 baris → `InsertGeofenceEvent` → commit → ack.
4. **REST API** — membaca `vehicle_loctions` dan `geofence_events`.
5. **Housekeeping** — `DeletePublishedOutboxEvents`, `DeleteProcessedInboxMessages`,
   `RequeueFailedOutboxEvents` (operasional).

---

## Arsitektur Kode & Design Pattern

Bagian ini menjelaskan **bagaimana kode disusun**, **bagaimana sistem bekerja dari
ujung ke ujung**, dan **pola desain** yang dipakai beserta alasannya.

### Lapisan dan arah dependensi

```mermaid
flowchart TB
    subgraph entry["cmd/ — entry point"]
        mainServer["server/main.go"]
        mainWorker["worker/main.go"]
        mainPub["publisher/main.go"]
    end

    subgraph root["internal/cmd — composition root"]
        common["common.go<br/>env · logger · init koneksi · closers"]
    end

    subgraph transport["Transport (adapter masuk)"]
        http["internal/httpserver<br/>Gin + OpenAPI strict server"]
    end

    subgraph domain["Logika bisnis"]
        fleet["internal/fleet<br/>ingest · relay · consumer · geofence"]
    end

    subgraph data["Akses data"]
        pg["internal/postgres<br/>pool · retry · WithTx"]
        sqlc["internal/postgres/sqlc<br/>query type-safe (generate)"]
    end

    subgraph infra["pkg/ — library generik"]
        mqtt["pkg/mqtt"]
        rabbit["pkg/rabbitmq"]
        env["pkg/env"]
    end

    entry --> root
    root --> http & fleet & pg & mqtt & rabbit & env
    http --> sqlc
    fleet --> pg & sqlc & mqtt & rabbit
    pg --> sqlc
```

| Lapisan | Tanggung jawab | Tidak boleh tahu tentang |
| --- | --- | --- |
| `cmd/*` | Menangkap sinyal OS (`SIGINT`/`SIGTERM`), memanggil `New` lalu `Run` | Apa pun selain `internal/cmd` |
| `internal/cmd` | **Implementasi** semua komponen: baca env, buat koneksi, sambungkan dependensi, urutan shutdown | Aturan bisnis (geofence, outbox) |
| `internal/httpserver` | Validasi request HTTP, mapping ke query, format respons/error sesuai OpenAPI | MQTT, RabbitMQ |
| `internal/fleet` | Aturan bisnis: validasi payload, deteksi geofence, transactional outbox, idempotensi worker | HTTP, Gin, env var |
| `internal/postgres` | Koneksi, retry saat startup, helper transaksi; query di-generate sqlc | Aturan bisnis |
| `pkg/*` | Library generik (reconnect, re-subscribe, topologi, parse env) | Apa pun di `internal/` |

Aturannya: **dependensi hanya mengarah ke bawah**. `pkg/` bisa dipakai proyek lain apa
adanya; `internal/fleet` bisa dites tanpa HTTP; `internal/httpserver` dites dengan
`sqlc.Querier` palsu tanpa database.

### Cara kerja end-to-end

```mermaid
sequenceDiagram
    autonumber
    participant P as publisher / kendaraan
    participant M as Mosquitto
    participant S as server (Ingestor)
    participant DB as PostgreSQL
    participant R as server (Relay)
    participant Q as RabbitMQ
    participant W as worker (Consumer)
    participant C as Client REST

    P->>M: PUBLISH /fleet/vehicle/{id}/location (QoS 1)
    M->>S: deliver
    rect rgba(127,127,127,0.12)
    note over S,DB: satu transaksi (Unit of Work)
    S->>DB: InsertVehicleLocation (0 baris = duplikat → selesai)
    S->>DB: EnterGeofence / ExitGeofence per titik
    S->>DB: InsertOutboxEvent (hanya saat baru masuk)
    end
    loop tiap OUTBOX_POLL_INTERVAL
        R->>DB: ClaimPendingOutboxEvents (FOR UPDATE SKIP LOCKED)
        R->>Q: publish fleet.events / geofence.entry (message_id = outbox id)
        Q-->>R: publisher confirm (ack)
        R->>DB: MarkOutboxEventPublished
    end
    Q->>W: deliver dari queue geofence_alerts
    rect rgba(127,127,127,0.12)
    note over W,DB: satu transaksi
    W->>DB: InsertInboxMessage (0 baris = duplikat → ack saja)
    W->>DB: InsertGeofenceEvent
    end
    W-->>Q: ack
    C->>S: GET /vehicles/{id}/location | /history | /geofence-events
    S->>DB: query (sqlc)
    S-->>C: JSON
```

Poin penting cara kerjanya:

1. **Ingest dan outbox satu transaksi.** Lokasi, status geofence, dan event ditulis
   bersama; tidak mungkin lokasi tersimpan tetapi event-nya hilang (atau sebaliknya).
2. **Server tidak pernah publish langsung saat menerima MQTT.** Publish dilakukan
   relay secara terpisah, sehingga RabbitMQ yang down tidak memperlambat atau
   menggagalkan ingest.
3. **Pengiriman at-least-once, efek exactly-once.** MQTT QoS 1 dan RabbitMQ sama-sama
   bisa mengirim ulang; unique constraint di `vehicle_loctions` dan tabel inbox
   membuat pengulangan tidak berdampak.
4. **Event hanya saat masuk.** Tabel `vehicle_geofence_presence` menyimpan status
   "sedang di dalam", sehingga lokasi berikutnya di dalam radius tidak memicu event lagi.

### Functional Options (`OptFunc`)

Pola konstruktor utama di proyek ini. Dipakai di `postgres`, `httpserver`,
`pkg/mqtt`, `pkg/rabbitmq`, `fleet.NewIngestor`/`NewRelay`, dan `internal/cmd`.

**Bentuknya** ([internal/postgres/postgres.go](internal/postgres/postgres.go), diringkas):

```go
type OptFunc func(*Postgres) error

func WithDbUri(uri string) OptFunc {
    return func(p *Postgres) error {
        p.dbUri = uri
        return nil
    }
}

func New(opts ...OptFunc) (pg *Postgres, err error) {
    pg = &Postgres{                       // 1. default yang masuk akal
        dbUri:        os.Getenv("DB_CONNECTION"),
        retryDelay:   2 * time.Second,
        maxOpenConns: 5,
        logger:       slog.Default(),
    }
    for _, opt := range opts {            // 2. caller hanya menimpa yang perlu
        if err = opt(pg); err != nil {
            return
        }
    }
    pg.db, err = sql.Open("pgx", pg.dbUri) // 3. objek siap pakai
    return
}
```

**Pemakaian** — perhatikan tidak ada parameter yang wajib diurutkan atau diisi nol:

```go
pg, err := postgres.New(
    postgres.WithDbUri(cfg.DBConnection),
    postgres.WithLogger(logger.With("logger-name", "postgres")),
)
```

**Mengapa memakai pola ini:**

| Alasan | Contoh nyata di kode |
| --- | --- |
| **Default yang aman + fallback env.** Pemanggil cukup menyebut yang berbeda dari default. | `postgres.New()` tanpa opsi tetap jalan dengan `DB_CONNECTION`; `httpserver` default `HTTP_ADDR` atau `:8080`. |
| **Validasi per opsi.** Nilai salah ditolak di tempat ia diberikan, dengan pesan jelas. | `rabbitmq.WithPrefetch(0)` → `"prefetch must be > 0"`; `fleet.WithRelayInterval(0)` ditolak. |
| **Dependensi wajib dicek di `New`.** Objek tidak pernah ada dalam keadaan setengah jadi. | `httpserver.New` tanpa `WithQuerier` → error `querier is required`. |
| **Menambah opsi tanpa merusak pemanggil lama** (backward compatible). | `WithShutdownTimeout` ditambahkan belakangan; semua pemanggil `httpserver.New` lama tetap terkompilasi. |
| **Mudah dites** — dependensi bisa diganti dari luar. | `httpserver.WithListener` (port acak di test), `WithQuerier(&fakeQuerier{})`, `mqtt.WithClient(mockPaho)`. |
| **Terbaca di call site.** Nama opsi menjelaskan dirinya. | `fleet.WithIngestGeofences(gs, 50)` lebih jelas dari argumen ke-3 sebuah fungsi. |
| **Konsisten antar paket.** Developer baru cukup belajar satu pola. | Semua `New(...)` di proyek ini berbentuk sama. |

**Dibanding alternatif:**

| Alternatif | Kekurangan untuk kasus ini |
| --- | --- |
| Parameter panjang `New(uri, logger, retry, maxConns, ...)` | Urutan mudah tertukar; setiap parameter baru merusak semua pemanggil. |
| Struct konfigurasi `New(Config{...})` | Tidak bisa membedakan "tidak diisi" dengan nilai nol (`Prefetch: 0`); validasi menumpuk di satu tempat. |
| Builder `NewBuilder().WithX().Build()` | Lebih banyak kode (tipe builder terpisah) tanpa manfaat tambahan di Go. |

**Variasi yang dipakai:**
- `OptFunc` **mengembalikan `error`** agar validasi bisa langsung menolak nilai salah.
- Di `internal/cmd`, opsi bekerja pada `*Common` sehingga `cmd.WithVersion`,
  `cmd.WithEnvPrefix`, `cmd.WithoutDotEnv` berlaku sama untuk server, worker, dan publisher.
- **Tidak dipakai** untuk objek sederhana: `fleet.NewConsumer(pg, name, logger)` cukup
  tiga parameter, dan struct data seperti `fleet.Geofence` cukup literal struct.

**Menambah opsi baru** — misalnya ukuran pool koneksi:

```go
func WithMaxOpenConns(n int) OptFunc {
    return func(p *Postgres) error {
        if n <= 0 {
            return errors.New("postgres: max open conns must be > 0")
        }
        p.maxOpenConns = n
        return nil
    }
}
```

Pemanggil lama tidak perlu diubah; yang butuh cukup menambah `postgres.WithMaxOpenConns(20)`.

### Katalog pattern lainnya

| Pattern | Di mana | Masalah yang diselesaikan |
| --- | --- | --- |
| **Composition Root** (DI manual) | [internal/cmd](internal/cmd/) | Satu-satunya tempat yang tahu cara merakit semua komponen; paket lain hanya menerima dependensi lewat opsi. Tanpa framework DI. |
| **Contract-first API** | [api/openapi-spec.yaml](api/openapi-spec.yaml) → `oapi-codegen` | Spesifikasi menjadi sumber kebenaran; handler wajib mengikuti `StrictServerInterface`, respons yang tidak ada di spec tidak bisa dikembalikan. |
| **Query object type-safe** | sqlc → [internal/postgres/sqlc](internal/postgres/sqlc/) | SQL ditulis sebagai SQL biasa, salah nama kolom/tipe ketahuan saat generate, bukan saat runtime. |
| **Interface untuk test double** | `sqlc.Querier`, `fleet.Publisher` | Handler HTTP dites dengan `fakeQuerier`; relay hanya butuh 2 method (`PublishMessage`, `IsConnected`), bukan seluruh klien RabbitMQ. |
| **Unit of Work** | `postgres.WithTx` | Commit bila sukses, rollback bila error **atau panic**; pemanggil tidak perlu menulis ulang logika itu. |
| **Transactional Outbox** | [fleet/ingest.go](internal/fleet/ingest.go) + [fleet/relay.go](internal/fleet/relay.go) | Data dan event konsisten; event tidak hilang walau broker down. |
| **Idempotent Receiver** | `UNIQUE (vehicle_id, timestamp)` | Pesan MQTT QoS 1 yang terkirim ulang tidak menggandakan data/event. |
| **Inbox (Idempotent Consumer)** | [fleet/consumer.go](internal/fleet/consumer.go) | RabbitMQ at-least-once → efek exactly-once di worker. |
| **Competing Consumers** | `FOR UPDATE SKIP LOCKED` | Beberapa instance relay bisa jalan paralel tanpa mengirim event yang sama. |
| **Retry + Exponential Backoff** | `MarkOutboxEventFailed`, `Relay.backoffSeconds` | Gangguan sementara dicoba ulang dengan jeda 1, 2, 4, … detik (maks 5 menit). |
| **Dead Letter Queue / Poison Message** | `rabbitmq.Permanent`, [fleet/topology.go](internal/fleet/topology.go) | Pesan rusak tidak di-requeue terus-menerus; dipindah ke `geofence_alerts.dlq` untuk dianalisis. |
| **Error classification** | `fleet.ErrInvalidMessage` + `errors.Is` | Membedakan error permanen (buang/DLQ) dari error sementara (retry). |
| **State table untuk edge-trigger** | `vehicle_geofence_presence` | Event dipicu saat **transisi** masuk, bukan setiap lokasi di dalam radius. |
| **Supervisor / auto-reconnect** | `pkg/rabbitmq` (`watch`), `pkg/mqtt` (re-subscribe di `onConnect`) | Koneksi putus dipulihkan otomatis tanpa restart proses. |
| **Graceful shutdown** | `signal.NotifyContext`, `errgroup`, closers LIFO | Request/pesan yang sedang diproses diselesaikan; resource ditutup dengan urutan terbalik dari pembuatannya. |
| **Health check aggregation** | `httpserver.WithHealthCheck` | `/healthz` mengecek semua dependensi paralel dengan timeout; `503` bila ada yang down. |
| **12-Factor config** | `pkg/env` + struct tag `env:"..."` | Semua konfigurasi dari env var dengan default; `.env` opsional untuk lokal. |
| **Redaksi data sensitif** | `slog.LogValuer` di `internal/cmd` | Konfigurasi bisa di-log saat startup tanpa membocorkan password. |
| **Compile-time interface check** | `var _ openapi.StrictServerInterface = &openapiServerImplementation{}` | Lupa mengimplementasikan endpoint baru → gagal compile, bukan error saat runtime. |

**Cuplikan kunci** — urutan shutdown (LIFO) di [internal/cmd/common.go](internal/cmd/common.go), diringkas:

```go
// Dibuat: postgres → rabbitmq → mqtt → http
// Ditutup: mqtt → rabbitmq → postgres  (berhenti menerima dulu, database terakhir)
func (c *Common) close(ctx context.Context, err error) error {
    for i := len(c.closers) - 1; i >= 0; i-- {
        err = errors.Join(err, c.closers[i](ctx))
    }
    return err
}
```

**Cuplikan kunci** — klasifikasi error di worker ([internal/fleet/consumer.go](internal/fleet/consumer.go)):

```go
alert, geofenceID, err := ParseAlert(msg)
if err != nil {
    return rabbitmq.Permanent(err) // rusak → nack tanpa requeue → DLQ
}
// error database dikembalikan apa adanya → nack + requeue setelah jeda
```

---

## Struktur Direktori

```text
.
├── api/
│   └── openapi-spec.yaml          # Kontrak REST API (OpenAPI 3.0.3), sumber codegen & Swagger UI
├── cmd/                           # Entry point: hanya signal handling → internal/cmd
│   ├── server/main.go             # REST API + MQTT ingest + outbox relay
│   ├── worker/main.go             # Consumer RabbitMQ geofence_alerts
│   └── publisher/main.go          # Mock pengirim lokasi (dev/demo)
├── internal/
│   ├── cmd/                       # Composition root tiap binary
│   │   ├── common.go              # Env, logger, init Postgres/RabbitMQ/MQTT, closers
│   │   ├── cmd.go                 # Server
│   │   ├── worker.go              # Worker
│   │   └── publisher.go           # Publisher
│   ├── fleet/                     # Logika bisnis (tanpa HTTP)
│   │   ├── geofence.go            # Parse GEOFENCE_POINTS + jarak haversine
│   │   ├── message.go             # Payload MQTT & pesan RabbitMQ + validasi
│   │   ├── ingest.go              # MQTT → lokasi + geofence + outbox (1 transaksi)
│   │   ├── relay.go               # Outbox → RabbitMQ (SKIP LOCKED, retry + backoff)
│   │   ├── consumer.go            # RabbitMQ → inbox + geofence_events (idempotent)
│   │   ├── topology.go            # Exchange, queue, binding, dead-letter queue
│   │   └── simulator.go           # Rute kendaraan palsu untuk publisher
│   ├── httpserver/                # Layer HTTP (Gin) + implementasi StrictServerInterface
│   │   ├── oapi-codegen.yml       # Konfigurasi oapi-codegen
│   │   ├── oapi-codegen.go        # //go:generate + struct implementasi
│   │   ├── fleet.go               # Handler endpoint: validasi + query sqlc + health check
│   │   ├── fleet_test.go          # Test end-to-end via httptest + fake Querier
│   │   ├── httpserver.go          # New(opts...), router Gin, middleware, graceful shutdown
│   │   └── openapi/openapi.gen.go # HASIL GENERATE — jangan diedit
│   └── postgres/                  # Layer database
│       ├── postgres.go            # Koneksi, retry connect, helper transaksi
│       ├── schema.sql             # DDL (dibaca sqlc & initdb Postgres)
│       ├── schema.down.sql        # Rollback DDL (manual)
│       ├── seed.sql               # Data contoh untuk dev/demo (make seed)
│       ├── query.sql              # Query bernama untuk sqlc
│       ├── sqlc.yaml              # Konfigurasi sqlc
│       ├── sqlc.go                # //go:generate sqlc
│       └── sqlc/                  # HASIL GENERATE — jangan diedit
├── pkg/
│   ├── env/                       # Loader env var ke struct (+ .env)
│   ├── mqtt/                      # Wrapper Paho MQTT (auto-reconnect, re-subscribe)
│   └── rabbitmq/                  # Wrapper AMQP (reconnect, topology, confirms)
├── deployments/
│   ├── mosquitto.conf             # Konfigurasi broker MQTT
│   └── rabbitmq/enabled_plugins   # + rabbitmq_tracing (hanya di-mount di override/dev)
├── .dagger/                       # Pipeline test/CI berbasis Dagger
├── Caddyfile                      # Reverse proxy (REST, /docs)
├── Dockerfile                     # Multi-stage build (distroless, non-root)
├── docker-compose.yml             # Stack "production"
├── docker-compose.override.yaml   # Tambahan untuk dev (port host, debug log, publisher)
└── makefile
```

Konvensi: `internal/` berisi kode khusus aplikasi ini, `pkg/` berisi library
generik yang bisa dipakai ulang. Semua komponen memakai pola **functional
options** (`New(opts ...OptFunc)`) dengan default dari environment variable.

---

## Teknologi & Library

| Kategori | Library / Tool | Versi | Kegunaan |
| --- | --- | --- | --- |
| Bahasa | Go | 1.23 | — |
| HTTP | [`gin-gonic/gin`](https://github.com/gin-gonic/gin) | v1.11.0 | Router & server REST |
| OpenAPI | [`oapi-codegen`](https://github.com/oapi-codegen/oapi-codegen) | v2.4.1 | Generate model, server Gin, dan *strict server interface* dari `api/openapi-spec.yaml` |
| OpenAPI | `oapi-codegen/runtime`, `getkin/kin-openapi` | v1.2.0 / v0.135.0 | Runtime binding & embedded spec |
| Database | PostgreSQL | 17 | Penyimpanan utama |
| Database | [`jackc/pgx/v5`](https://github.com/jackc/pgx) (`stdlib`) | v5.7.6 | Driver Postgres lewat `database/sql` |
| Database | [`sqlc`](https://sqlc.dev) | v1.30.0 | Generate kode Go type-safe dari SQL |
| Database | `google/uuid` | v1.6.0 | Tipe kolom `uuid` (`outbox_events.id`) |
| MQTT | [`eclipse/paho.mqtt.golang`](https://github.com/eclipse/paho.mqtt.golang) | v1.5.0 | Client MQTT |
| MQTT | Eclipse Mosquitto | 2 | Broker MQTT |
| AMQP | [`rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) | v1.15.0 | Client RabbitMQ |
| AMQP | RabbitMQ | 4 (management) | Message broker |
| Config | [`caarlos0/env/v10`](https://github.com/caarlos0/env) | v10.0.0 | Parse env var ke struct |
| Config | [`joho/godotenv`](https://github.com/joho/godotenv) | v1.5.1 | Membaca file `.env` |
| Logging | `log/slog` (stdlib) | — | Structured logging |
| Edge | Caddy | 2 | Reverse proxy & gzip |
| Docs | Swagger UI | v5.18.2 | Dokumentasi API di `/docs` |
| CI | Dagger | — | `make test` / `make ci` |

---

## Skema Database

Didefinisikan di [internal/postgres/schema.sql](internal/postgres/schema.sql).

| Tabel | Isi | Kunci penting |
| --- | --- | --- |
| `vehicle_loctions` | Lokasi kendaraan (nama tabel mengikuti spesifikasi tes, termasuk typo) | `UNIQUE (vehicle_id, timestamp)`, `CHECK` lat/lon/timestamp |
| `vehicle_geofence_presence` | Kendaraan yang sedang berada di dalam geofence | `PRIMARY KEY (vehicle_id, geofence_id)` |
| `outbox_events` | Event yang menunggu dikirim ke RabbitMQ | `UNIQUE (dedup_key)`, status `pending`/`published`/`failed`, partial index untuk `pending` |
| `inbox_messages` | Pesan yang sudah diproses worker | `PRIMARY KEY (consumer, message_id)` |
| `geofence_events` | Hasil pemrosesan worker | `UNIQUE (message_id)`, index `(vehicle_id, timestamp DESC)` |

Catatan:
- `timestamp` disimpan sebagai `BIGINT` unix epoch (detik), sama dengan payload MQTT.
- `outbox_events.payload` bertipe `JSON` (bukan `JSONB`) agar byte & urutan key persis seperti yang ditulis.
- Schema dijalankan otomatis oleh Postgres saat volume pertama kali dibuat
  (`/docker-entrypoint-initdb.d/001_schema.sql`).
- Untuk reset manual: `psql "$DB_CONNECTION" -f internal/postgres/schema.down.sql`.

---

## sqlc (Query Type-Safe)

Query ditulis sebagai SQL biasa di [internal/postgres/query.sql](internal/postgres/query.sql),
lalu sqlc menghasilkan fungsi Go di `internal/postgres/sqlc/`.

### Konfigurasi ([sqlc.yaml](internal/postgres/sqlc.yaml))

| Opsi | Nilai | Alasan |
| --- | --- | --- |
| `sql_package` | `database/sql` | Sesuai `postgres.go` yang memakai `*sql.DB` / `*sql.Tx` (driver `pgx/v5/stdlib`) |
| `package` / `out` | `sqlc` / `./sqlc` | Kode hasil generate terpisah dari kode tulisan tangan |
| `emit_interface` | `true` | Menghasilkan interface `Querier` — mudah di-mock untuk unit test |
| `emit_json_tags` | `true` | Struct bisa langsung di-marshal ke JSON |
| `emit_empty_slices` | `true` | Query `:many` tanpa hasil mengembalikan `[]` bukan `nil` (JSON `[]`, bukan `null`) |
| `rename` | `vehicle_loction: VehicleLocation` | Nama struct Go yang benar walau nama tabel typo |
| `overrides` | `uuid` → `uuid.UUID`, `json`/`jsonb` → `json.RawMessage` | Tipe Go yang lebih nyaman dipakai |

### Generate ulang

```bash
make sqlc                      # memakai binary sqlc di PATH
# atau
go generate ./internal/postgres/...   # memakai go run sqlc@v1.30.0 (tanpa install)
```

> File di `internal/postgres/sqlc/` adalah hasil generate — **jangan diedit manual**.
> Ubah `schema.sql` / `query.sql`, lalu generate ulang.

### Konvensi penulisan query

- `-- name: NamaQuery :one | :many | :exec | :execrows`
- `:execrows` dipakai untuk operasi idempotent: jumlah baris yang terpengaruh
  menentukan alur (mis. `0` = duplikat).
- Parameter bernama: `@nama` atau `sqlc.arg(nama)`; tambahkan cast (`@before::timestamptz`)
  agar tipe Go-nya tepat.
- Komentar di atas query ikut menjadi doc comment di Go.

### Daftar query

| Query | Tipe | Fungsi |
| --- | --- | --- |
| `InsertVehicleLocation` | `:execrows` | Simpan lokasi, 0 = duplikat |
| `GetLatestVehicleLocation` | `:one` | Lokasi terakhir kendaraan |
| `ListVehicleLocationHistory` | `:many` | Riwayat lokasi `start..end` (inklusif) |
| `EnterGeofence` | `:execrows` | 1 = kendaraan baru masuk geofence |
| `ExitGeofence` | `:execrows` | 1 = kendaraan baru keluar geofence |
| `ListVehicleGeofencePresence` | `:many` | Geofence tempat kendaraan sedang berada |
| `InsertOutboxEvent` | `:execrows` | Tulis event ke outbox (dedup lewat `dedup_key`) |
| `ClaimPendingOutboxEvents` | `:many` | Ambil batch event pending (`SKIP LOCKED`) |
| `MarkOutboxEventPublished` | `:exec` | Tandai event terkirim |
| `MarkOutboxEventFailed` | `:exec` | Catat error + backoff, `failed` setelah `max_attempts` |
| `RequeueFailedOutboxEvents` | `:execrows` | Kirim ulang semua event `failed` |
| `DeletePublishedOutboxEvents` | `:execrows` | Housekeeping outbox |
| `CountOutboxEventsByStatus` | `:many` | Monitoring jumlah event per status |
| `InsertInboxMessage` | `:execrows` | 0 = pesan sudah pernah diproses |
| `DeleteProcessedInboxMessages` | `:execrows` | Housekeeping inbox |
| `InsertGeofenceEvent` | `:execrows` | Simpan hasil proses worker |
| `ListGeofenceEventsByVehicle` | `:many` | Event geofence per kendaraan (terbaru dulu) |

### Contoh: ingest lokasi dalam satu transaksi

```go
import (
    "github.com/ekowdd89/test-teknis-backend/internal/postgres"
    "github.com/ekowdd89/test-teknis-backend/internal/postgres/sqlc"
)

pg, err := postgres.New(postgres.WithDbUri(cfg.DBConnection), postgres.WithLogger(logger))
if err != nil { return err }
if err := pg.Connect(ctx); err != nil { return err } // retry sampai DB siap / ctx habis
defer pg.Close()

queries := sqlc.New(pg.Db())

err = pg.WithTx(ctx, func(tx *sql.Tx) error {
    q := queries.WithTx(tx)

    n, err := q.InsertVehicleLocation(ctx, sqlc.InsertVehicleLocationParams{
        VehicleID: loc.VehicleID,
        Latitude:  loc.Latitude,
        Longitude: loc.Longitude,
        Timestamp: loc.Timestamp,
    })
    if err != nil || n == 0 {
        return err // n == 0: duplikat, commit tanpa efek
    }

    entered, err := q.EnterGeofence(ctx, sqlc.EnterGeofenceParams{
        VehicleID: loc.VehicleID, GeofenceID: "monas", EnteredTs: loc.Timestamp,
    })
    if err != nil || entered == 0 {
        return err
    }

    _, err = q.InsertOutboxEvent(ctx, sqlc.InsertOutboxEventParams{
        AggregateType: "vehicle",
        AggregateID:   loc.VehicleID,
        EventType:     "geofence_entry",
        Exchange:      "fleet.events",
        RoutingKey:    "geofence.entry",
        Headers:       json.RawMessage(`{"geofence_id":"monas"}`),
        Payload:       payload,
        DedupKey:      fmt.Sprintf("geofence_entry:%s:monas:%d", loc.VehicleID, loc.Timestamp),
    })
    return err
})
```

`WithTx` otomatis **rollback** bila fungsi mengembalikan error atau panic,
dan **commit** bila sukses.

---

## Penggunaan Library Internal

### `internal/postgres`

| Fungsi | Keterangan |
| --- | --- |
| `New(opts...)` | Membuka pool `database/sql` (driver `pgx`). Default URI dari `DB_CONNECTION`; pool: 5 open, 5 idle, lifetime 5 menit. |
| `WithDbUri(uri)`, `WithLogger(l)` | Opsi. |
| `Connect(ctx)` | Ping berulang (timeout 3 detik, jeda 2 detik) sampai berhasil atau `ctx` selesai. |
| `WithTx(ctx, fn)` | Helper transaksi (commit/rollback otomatis). |
| `Db()` | `*sql.DB` untuk `sqlc.New(...)`. |
| `Ping(ctx)`, `Close()` | Health check & shutdown. |

### `pkg/env`

```go
type Config struct {
    DBConnection string `env:"DB_CONNECTION,required"`
    LogLevel     string `env:"LOG_LEVEL" envDefault:"info"`
}

var cfg Config
err := env.Load(&cfg, env.Options{DotEnv: true}) // DotEnv: baca .env bila ada
```

### `pkg/mqtt`

Wrapper Paho dengan auto-reconnect dan **re-subscribe otomatis** setelah reconnect.

```go
client, err := mqtt.New(
    mqtt.WithBrokerId("tcp://mosquitto:1883"),
    mqtt.WithClientId("fleet-server"),
    mqtt.WithCleanSession(false), // simpan sesi agar pesan QoS 1 tidak hilang saat reconnect
    mqtt.WithLogger(logger),
)
if err := client.Connect(ctx); err != nil { ... }

err = client.Subscribe(ctx, "/fleet/vehicle/+/location", 1, func(topic string, payload []byte) {
    // parse payload & ingest
})

// Publisher (mock)
err = client.PublishJSON(ctx, "/fleet/vehicle/B1234XYZ/location", 1, false, loc)
```

Opsi lain: `WithCredential(user, pass)`, `WithKeepAlive(d)` (default 30 detik),
`WithTimeout(d)` (default 10 detik), `WithClient(paho.Client)` (untuk test).

### `pkg/rabbitmq`

Wrapper AMQP dengan reconnect otomatis, deklarasi topologi saat connect,
publisher confirms, dan prefetch.

```go
rb, err := rabbitmq.New(
    rabbitmq.WithURI("amqp://fleet:fleet@rabbitmq:5672/"),
    rabbitmq.WithExchange(rabbitmq.Exchange{Name: "fleet.events", Kind: "topic", Durable: true}),
    rabbitmq.WithQueue(rabbitmq.Queue{Name: "geofence_alerts", Durable: true}),
    rabbitmq.WithBinding(rabbitmq.Binding{
        Queue: "geofence_alerts", Exchange: "fleet.events", RoutingKey: "geofence.entry",
    }),
    rabbitmq.WithPrefetch(10),
    rabbitmq.WithPublisherConfirms(true),
)
if err := rb.Connect(ctx); err != nil { ... }
defer rb.Close()

// Producer (outbox relay)
err = rb.Publish(ctx, "fleet.events", "geofence.entry", "application/json", body)

// Consumer (worker)
err = rb.Consume(ctx, "geofence_alerts", func(ctx context.Context, msg rabbitmq.Message) error {
    // msg.MessageId dipakai sebagai kunci inbox
    if invalid {
        return rabbitmq.Permanent(err) // tidak di-requeue
    }
    return nil
})
```

Default: reconnect 2 detik, publish timeout 2 detik, prefetch 10, confirms aktif.

### `internal/httpserver`

Handler mengimplementasikan `openapi.StrictServerInterface` hasil `oapi-codegen`.
Alurnya: ubah `api/openapi-spec.yaml` → `go generate ./internal/httpserver/...` →
implementasikan method baru di `fleet.go`. Compiler akan error bila ada method
yang belum diimplementasikan (`var _ openapi.StrictServerInterface = ...`).

```go
hs, err := httpserver.New(
    httpserver.WithAddr(cfg.HTTPAddr),                 // default HTTP_ADDR atau ":8080"
    httpserver.WithQuerier(sqlc.New(pg.Db())),
    httpserver.WithHealthCheck("postgres", pg.Ping),   // muncul di GET /healthz
    httpserver.WithHealthCheck("rabbitmq", func(context.Context) error {
        if !rb.IsConnected() { return rabbitmq.ErrNotConnected }
        return nil
    }),
    httpserver.WithLogger(logger),
)
if err != nil { return err }

// Berjalan sampai ctx dibatalkan (mis. SIGTERM), lalu graceful shutdown
// (batas waktu WithShutdownTimeout, default 10 detik).
if err := hs.Serve(ctx); err != nil { return err }
```

Perilaku:

| Hal | Perilaku |
| --- | --- |
| Validasi | `vehicle_id` 1–20 karakter `[A-Za-z0-9-]`; `start`/`end` ≥ 0 dan `start <= end`; `limit` history 1–10000 (default 1000), event 1–1000 (default 100) |
| Format error | Semua error (termasuk parameter salah format, route tidak ada, panic) memakai schema `Error` `{code, message}` |
| Kode error | `INVALID_ARGUMENT` (400), `NOT_FOUND` (404), `INTERNAL` (500), `METHOD_NOT_ALLOWED` (405) |
| Error database | Detail tidak dikirim ke client, hanya dicatat di log |
| Data kosong | `history` & `geofence-events` mengembalikan `[]`, bukan `null` |
| `/healthz` | Semua check dijalankan paralel dengan timeout 2 detik per check; `200 ok` bila semua `up`, `503 degraded` bila ada yang `down` |
| Timeout server | read header 5 detik, read 10 detik, write 30 detik, idle 60 detik |
| Gin mode | `release` secara default; set `GIN_MODE=debug` untuk log route Gin |

---

## REST API

Spesifikasi lengkap: [api/openapi-spec.yaml](api/openapi-spec.yaml).
Swagger UI tersedia di `http://localhost:8000/docs` (lewat Caddy).

| Method | Path | Keterangan |
| --- | --- | --- |
| `GET` | `/vehicles/{vehicle_id}/location` | Lokasi terakhir kendaraan (404 bila belum ada) |
| `GET` | `/vehicles/{vehicle_id}/history?start=&end=&limit=` | Riwayat lokasi `start <= timestamp <= end`, urut naik, `limit` default 1000 (maks 10000) |
| `GET` | `/vehicles/{vehicle_id}/geofence-events?limit=` | Event geofence hasil worker, `limit` default 100 (maks 1000) |
| `GET` | `/healthz` | Status service & dependensinya |

Contoh:

```bash
curl http://localhost:8000/vehicles/B1234XYZ/location
curl "http://localhost:8000/vehicles/B1234XYZ/history?start=1715000000&end=1715009999"
```

Payload MQTT (`/fleet/vehicle/{vehicle_id}/location`):

```json
{
  "vehicle_id": "B1234XYZ",
  "latitude": -6.2088,
  "longitude": 106.8456,
  "timestamp": 1715003456
}
```

---

## Konfigurasi (Environment Variable)

| Variabel | Dipakai oleh | Default (compose) | Keterangan |
| --- | --- | --- | --- |
| `DB_CONNECTION` | server, worker | `postgres://fleet:fleet@postgres:5432/fleet?sslmode=disable` | URI PostgreSQL |
| `LOG_LEVEL` | semua | `info` (`debug` di override) | Level log slog |
| `MQTT_BROKER_URI` | server, publisher | `tcp://mosquitto:1883` | Broker MQTT |
| `MQTT_CLIENT_ID` | server, publisher | `fleet-server` / `fleet-publisher` | Harus unik per koneksi |
| `MQTT_TOPIC` | server | `/fleet/vehicle/+/location` | Topik subscribe |
| `MQTT_USERNAME`, `MQTT_PASSWORD` | `pkg/mqtt` | — | Opsional |
| `RABBITMQ_URI` | server, worker | `amqp://fleet:fleet@rabbitmq:5672/` | URI RabbitMQ |
| `RABBITMQ_EXCHANGE` | server, worker | `fleet.events` | Exchange event |
| `RABBITMQ_QUEUE` | worker | `geofence_alerts` | Queue worker |
| `RABBITMQ_ROUTING_KEY` | server, worker | `geofence.entry` | Routing key |
| `RABBITMQ_PREFETCH` | worker | `10` | Jumlah pesan yang diproses paralel |
| `RABBITMQ_CONNECT_TIMEOUT` | server, worker | `30s` | Batas waktu menunggu RabbitMQ siap saat startup |
| `MQTT_CONNECT_TIMEOUT` | server, publisher | `30s` | Batas waktu menunggu broker MQTT siap saat startup |
| `HTTP_ADDR` | server | `:8080` | Alamat REST |
| `DB_CONNECT_TIMEOUT` | server, worker | `30s` | Batas waktu menunggu Postgres siap saat startup |
| `SHUTDOWN_TIMEOUT` | server | `10s` | Batas waktu graceful shutdown HTTP |
| `GEOFENCE_RADIUS_METERS` | server | `50` | Radius geofence |
| `GEOFENCE_POINTS` | server, publisher | `bundaran-hi:-6.1950:106.8230,monas:-6.1754:106.8272,blok-m:-6.2443:106.8000` | Format `nama:lat:lon`, dipisah koma |
| `OUTBOX_POLL_INTERVAL` | server | `1s` | Interval relay membaca outbox |
| `OUTBOX_BATCH_SIZE` | server | `100` | Jumlah event per batch relay |
| `OUTBOX_MAX_ATTEMPTS` | server | `10` | Setelah ini event berstatus `failed` dan tidak dicoba ulang otomatis (lihat `make outbox-requeue`) |
| `OUTBOX_STATS_INTERVAL` | server | `1m` | Interval log jumlah event per status; `WARN` bila ada yang `failed` |
| `WORKER_CONSUMER_NAME` | worker | `geofence-alert-worker` | Nama consumer di tabel inbox |
| `PUBLISH_INTERVAL` | publisher | `2s` | Interval kirim lokasi (minimal `1s`) |
| `PUBLISH_STEPS_PER_LEG` | publisher | `30` | Jumlah titik antar dua geofence |
| `VEHICLE_IDS` | publisher | `B1234XYZ,B5678ABC,B9012DEF` | Kendaraan mock |
| `POSTGRES_USER/PASSWORD/DB` | compose | `fleet` | Kredensial Postgres |
| `RABBITMQ_USER/PASSWORD` | compose | `fleet` | Kredensial RabbitMQ |
| `HTTP_PORT` | compose | `8000` | Port Caddy di host |
| `SERVER_HOST_PORT` | override | `8080` | Port REST langsung di host |
| `POSTGRES_HOST_PORT` | override | `15432` | Port PostgreSQL di host |
| `RABBITMQ_HOST_PORT` | override | `5673` | Port AMQP di host |
| `RABBITMQ_UI_HOST_PORT` | override | `15673` | Port RabbitMQ Management di host |
| `MQTT_HOST_PORT` | override | `11883` | Port MQTT di host |
| `DELVE_HOST_PORT` | override | `2345` | Port Delve di host (`make debug`) |

Untuk development lokal (tanpa Docker), isi `.env`:

```env
DB_CONNECTION=postgres://fleet:fleet@localhost:15432/fleet?sslmode=disable
```

---

## Menjalankan Aplikasi

### Prasyarat

- Docker + Docker Compose v2
- Go 1.23 (untuk development lokal)
- sqlc v1.30.0 (opsional; bisa lewat `go generate`)

### Docker Compose

```bash
make start          # docker compose up --build (otomatis + docker-compose.override.yaml)
make stop           # docker compose down
make purge          # down + hapus volume (database ikut terhapus)

# Mode "production" tanpa override (tanpa port debug & publisher):
docker compose -f docker-compose.yml up -d --build

# Hot rebuild saat kode berubah:
docker compose watch
```

| URL | Service |
| --- | --- |
| `http://localhost:8000` | REST API lewat Caddy |
| `http://localhost:8000/docs` | Swagger UI |
| `http://localhost:8080` | REST langsung (dev) |
| `http://localhost:15673` | RabbitMQ Management (`fleet` / `fleet`) |
| `localhost:11883` | MQTT (dev) |
| `localhost:15432` | PostgreSQL (dev, `fleet` / `fleet`) |

Port host sengaja digeser dari port standar (80, 5432, 5672, 15672, 1883) agar tidak
bentrok dengan service yang terpasang lokal. Semua bisa diubah lewat env var
(lihat tabel [Konfigurasi](#konfigurasi-environment-variable)), mis. `HTTP_PORT=80 make start`.

### Data contoh (seeder)

Database baru masih kosong. Isi data contoh dengan:

```bash
make start   # di terminal lain, atau docker compose up -d
make seed
```

[internal/postgres/seed.sql](internal/postgres/seed.sql) membuat rute garis lurus
(satu titik tiap 10 detik, timestamp sekitar `1715003456`) yang melintasi titik geofence
dari `GEOFENCE_POINTS`. Event dihitung dengan rumus haversine (radius 50 m), lalu
ditulis lengkap: `outbox_events` (status `published`) → `inbox_messages` → `geofence_events`
dengan `message_id` yang sama.

| Kendaraan | Rute | Event geofence |
| --- | --- | --- |
| `B1234XYZ` | Bundaran HI → Monas (120 titik) | `bundaran-hi`, `monas` |
| `B5678ABC` | Melintasi Monas dari selatan ke utara (90 titik) | `monas` |
| `B9012DEF` | Menuju Blok M dan berhenti di dalamnya (50 titik) | `blok-m` (masih ada di `vehicle_geofence_presence`) |

Seeder aman dijalankan berulang: ID dibuat deterministik dan semua insert memakai
`ON CONFLICT DO NOTHING`. Untuk mengosongkan database: `make purge && make start`.

```bash
curl "http://localhost:8000/vehicles/B1234XYZ/geofence-events?limit=10"
curl "http://localhost:8000/vehicles/B1234XYZ/history?start=1715000000&end=1715009999&limit=5"
```

Kirim lokasi manual (titik di Monas → memicu event geofence untuk kendaraan baru):

```bash
# Dengan mosquitto_pub di host (brew install mosquitto)
mosquitto_pub -h localhost -p 11883 -q 1 \
  -t /fleet/vehicle/B4321ABC/location \
  -m "{\"vehicle_id\":\"B4321ABC\",\"latitude\":-6.1754,\"longitude\":106.8272,\"timestamp\":$(date +%s)}"

# Tanpa install: pakai mosquitto_pub di dalam container
docker compose exec mosquitto mosquitto_pub -q 1 \
  -t /fleet/vehicle/B4321ABC/location \
  -m "{\"vehicle_id\":\"B4321ABC\",\"latitude\":-6.1754,\"longitude\":106.8272,\"timestamp\":$(date +%s)}"

curl localhost:8000/vehicles/B4321ABC/geofence-events
```

`-t` (topik) dan `-m` (pesan) wajib; `vehicle_id` di payload harus sama dengan topik.

Atau cukup `make publish-test` (default `TEST-01` di Monas, timestamp sekarang), yang
langsung menampilkan hasil `/location` dan `/geofence-events`:

```bash
make publish-test
make publish-test VEHICLE=B4321ABC LAT=-8.1754 LON=109.8272
```

> **Data tidak masuk?** Periksa hal berikut:
> - **Timestamp harus baru.** Pasangan `vehicle_id + timestamp` yang sudah tersimpan
>   dianggap duplikat (pengaman QoS 1) dan diabaikan, dengan log
>   `duplicate location ignored`. Pakai `$(date +%s)`, bukan angka tetap.
> - **Hindari `B1234XYZ`/`B5678ABC`/`B9012DEF` selama publisher mock berjalan.**
>   `/location` mengembalikan timestamp terbaru, jadi data manual dengan timestamp lama
>   akan tertutup data publisher.
> - **MQTT tidak memberi balasan aplikasi.** `mosquitto_pub` hanya menerima ack dari broker.
>   Cek hasilnya lewat API atau `docker compose logs server`.

Memantau alurnya:

```bash
docker compose logs -f server worker | grep -E "geofence entry|published|alert received"
```

RabbitMQ Management (`http://localhost:15673`, `fleet`/`fleet`) menampilkan koneksi
server & worker, queue `geofence_alerts` (1 consumer), dan `geofence_alerts.dlq`.

### Memantau dari RabbitMQ Management

**Penting:** tidak setiap lokasi MQTT masuk RabbitMQ. Yang di-publish ke exchange
`fleet.events` hanya event **`geofence_entry`**, yaitu saat kendaraan *baru masuk* radius
geofence. Lokasi yang jauh dari geofence (mis. `-8.1754, 109.8272`) hanya tersimpan di
PostgreSQL. Selain itu, queue `geofence_alerts` hampir selalu tampak **kosong** karena worker
langsung meng-ack setiap pesan dalam hitungan milidetik.

Buka `http://localhost:15673` (`fleet` / `fleet`):

| Menu | Yang terlihat |
| --- | --- |
| **Overview** | Grafik *Message rates*: publish, deliver, dan ack naik bersamaan setiap ada event |
| **Connections** | 2 koneksi: server (2 channel: publish + deklarasi) dan worker (1 channel consume) |
| **Exchanges → `fleet.events`** | Binding ke `geofence_alerts` dengan routing key `geofence.entry`, rate *publish in/out* |
| **Queues → `geofence_alerts`** | 1 consumer, *Message rates* (publish → deliver → ack); *Get messages* hanya berisi saat worker dihentikan |
| **Queues → `geofence_alerts.dlq`** | Pesan yang ditolak permanen oleh worker |
| **Admin → Tracing** | Trace `fleet-events` beserta file log setiap pesan yang di-publish (setelah `make rabbit-trace-start`) |

**Melihat isi setiap pesan tanpa menghentikan worker** (plugin `rabbitmq_tracing`, aktif
hanya di dev lewat `docker-compose.override.yaml`):

```bash
make rabbit-trace-start                       # mulai mencatat publish ke fleet.events
make publish-test VEHICLE=B$(date +%H%M%S)    # kendaraan BARU ke Monas → memicu event
make rabbit-trace-log                         # satu baris JSON per pesan
make rabbit-trace-stop                        # hentikan & hapus log
```

Contoh keluaran `make rabbit-trace-log`:

```json
{"time":"2026-10-07T04:51:53.184+00:00","exchange":"fleet.events","routing_key":"geofence.entry","routed_queues":["geofence_alerts"],"message_id":"aa396077-…","geofence_id":"monas","payload":{"vehicle_id":"TR-115152","event":"geofence_entry","location":{"latitude":-6.1754,"longitude":106.8272},"timestamp":1791348712}}
```

`routed_queues` membuktikan pesan diteruskan ke `geofence_alerts`. Trace juga mencatat
event dari publisher mock.

**Melihat pesan tertahan di queue** (cocok untuk demo):

```bash
make publish-rabbit
```

Langkahnya:
1. Worker dihentikan.
2. Kendaraan baru dikirim ke Monas.
3. Queue menampilkan `geofence_alerts 1 0` (1 pesan, 0 consumer), lalu isi pesannya
   dibaca dan dikembalikan ke queue.
4. Worker dinyalakan lagi, queue kembali `0 1`, dan log worker mencatat `geofence alert received`.

Worker selalu dinyalakan kembali walau ada langkah yang gagal.

> Plugin tracing login sebagai `guest` secara default, sedangkan user itu tidak ada di stack
> ini. Karena itu `make rabbit-trace-start` mengirim kredensial per-trace
> (`tracer_connection_username/password`). Bila membuat trace manual di UI, isi kolom
> *Tracer connection username/password* dengan `fleet` / `fleet`.

### Menelusuri satu event dari MQTT sampai worker

Alurnya berjalan otomatis. Yang perlu diingat: **server (outbox relay) yang publish ke
RabbitMQ, worker hanya consume**. Tidak ada komponen yang publish balik dari worker.

| # | Dari → ke | Cara komunikasi | Data ditulis ke |
| --- | --- | --- | --- |
| 1 | publisher/kendaraan → Mosquitto → **server** | MQTT pub/sub QoS 1, topik `/fleet/vehicle/{id}/location`; server subscribe `/fleet/vehicle/+/location` dengan sesi persisten | `vehicle_loctions` (semua lokasi) |
| 2 | server (cek jarak haversine) | di dalam proses, transaksi yang sama | `vehicle_geofence_presence` (masuk → insert, keluar → delete) |
| 3 | server | transaksi yang sama, **hanya saat baru masuk** geofence | `outbox_events` status `pending` |
| 4 | **server (relay)** → RabbitMQ | AMQP publish ke exchange `fleet.events`, routing key `geofence.entry`, menunggu *publisher confirm*; `message_id` = `outbox_events.id` | status outbox → `published` |
| 5 | RabbitMQ → **worker** | AMQP consume queue `geofence_alerts`, ack setelah commit | `inbox_messages` + `geofence_events` |
| 6 | client → server | REST (lewat Caddy) | — (membaca `vehicle_loctions` & `geofence_events`) |

Telusuri satu event lengkap dengan satu perintah (read-only):

```bash
make trace-flow                    # event geofence terbaru
make trace-flow VEHICLE=B5678ABC   # event terbaru kendaraan tertentu
```

Keluarannya menampilkan isi kelima tabel untuk event itu, log server/worker berurutan
waktu, dan latensinya. Contoh nyata dari publisher mock:

```text
Event: message_id=2a841039-… vehicle=B5678ABC timestamp=1791349372
[1] vehicle_loctions            created_at   05:02:52.504
[3] outbox_events  published    created_at   05:02:52.504   published_at 05:02:53.044
[5] inbox_messages              processed_at 05:02:53.060
Latensi: outbox_ke_rabbitmq 0.54s | rabbitmq_ke_worker 0.016s | total 0.56s
Log: geofence entry detected → outbox event published → geofence alert received
```

`created_at` lokasi dan outbox **sama persis** karena keduanya ditulis dalam satu transaksi.
Jeda sekitar 0,5 detik berasal dari interval polling relay (`OUTBOX_POLL_INTERVAL=1s`).
Bila kendaraan belum punya event, `make trace-flow VEHICLE=...` menampilkan lokasi
terakhirnya dan menjelaskan bahwa lokasi di luar geofence tidak dikirim ke RabbitMQ.

### Lokal (tanpa Docker untuk aplikasi)

```bash
docker compose up -d postgres rabbitmq mosquitto
make generate       # go generate ./... + go mod tidy
go run ./cmd/server
```

---

## Perintah Makefile

| Target | Perintah | Keterangan |
| --- | --- | --- |
| `make generate` | `go generate ./... && go mod tidy` | Generate oapi-codegen + sqlc |
| `make sqlc` | `sqlc generate -f ./internal/postgres/sqlc.yaml` | Generate sqlc saja |
| `make seed` | `psql < internal/postgres/seed.sql` di container `postgres` | Isi data contoh (idempotent) |
| `make publish-test` | `mosquitto_pub` + `curl` | Kirim 1 lokasi uji (`VEHICLE`, `LAT`, `LON`, timestamp sekarang) lalu tampilkan hasil API |
| `make rabbit-trace-start` / `rabbit-trace-log` / `rabbit-trace-stop` | Management API + plugin `rabbitmq_tracing` | Catat & tampilkan setiap pesan yang di-publish ke `fleet.events` (dev) |
| `make publish-rabbit` | `docker compose stop/start worker` + Management API | Demo pesan tertahan di `geofence_alerts`, lalu dikonsumsi worker |
| `make trace-flow [VEHICLE=...]` | `psql` + `docker compose logs` (read-only) | Telusuri satu event: lokasi → presence → outbox → RabbitMQ → inbox → geofence_events, plus latensi |
| `make outbox-status` | `psql` | Jumlah event outbox per status + daftar event `failed` dan error terakhirnya |
| `make outbox-requeue` | `psql` (= `RequeueFailedOutboxEvents`) | Kembalikan event `failed` ke `pending` dengan `attempts = 0` |
| `make dlq-status` | `rabbitmqctl list_queues` | Isi `geofence_alerts` dan `geofence_alerts.dlq` |
| `make build` | `go build ./...` | Compile semua package |
| `make test` | `cd .dagger && go run . ..` | Test via Dagger (Postgres, RabbitMQ, Mosquitto di container) |
| `make ci` | Dagger di dalam container | Menjalankan pipeline CI |
| `make start` / `debug` | `docker compose up --build` | `debug` memakai target `debugger` (Delve di port 2345) |
| `make stop` / `purge` | `docker compose down [--volumes]` | — |

---

## Alur startup server

`cmd/*/main.go` hanya menangani sinyal (`SIGINT`/`SIGTERM`), lalu menyerahkan
semuanya ke [internal/cmd](internal/cmd/). Ketiga binary memakai pola yang sama:
`New(ctx)` menyiapkan dan menghubungkan semua dependensi, `Run(ctx)` berjalan
sampai ada sinyal, lalu semua resource ditutup **dengan urutan terbalik** dari pembuatannya.
Bila satu langkah `New` gagal, resource yang sudah dibuat langsung ditutup dan proses keluar dengan kode 1.

| Binary | Urutan `New` | `Run` |
| --- | --- | --- |
| `server` | logger → Postgres → RabbitMQ (+ topologi) → outbox relay → MQTT (subscribe lalu connect) → HTTP | HTTP server + outbox relay paralel (`errgroup`) |
| `worker` | logger → Postgres → RabbitMQ (+ topologi) → consumer | `Consume(geofence_alerts)`; pesan yang sedang diproses diselesaikan sebelum keluar |
| `publisher` | logger → MQTT | Kirim lokasi semua kendaraan tiap `PUBLISH_INTERVAL` |

Semua koneksi memakai retry saat startup (batas `*_CONNECT_TIMEOUT`) dan reconnect otomatis
setelahnya. Status koneksi terlihat di `GET /healthz`:

```json
{"checks":{"mqtt":"up","postgres":"up","rabbitmq":"up"},"status":"ok"}
```

Di server, `RABBITMQ_URI` dan `MQTT_BROKER_URI` boleh kosong (mis. `go run ./cmd/server`
hanya dengan `.env` berisi `DB_CONNECTION`): fitur terkait dinonaktifkan dengan log `WARN`
dan REST API tetap berjalan.

Opsi `New`: `WithEnvPrefix("FLEET_")` (semua env var diberi prefix),
`WithoutDotEnv()` (jangan baca `.env`), `WithVersion(v)` (dari `-ldflags -X main.version=...`).

### Penanganan error pesan

| Kondisi | Perilaku |
| --- | --- |
| Lokasi MQTT duplikat (QoS 1 kirim ulang) | `UNIQUE (vehicle_id, timestamp)` → diabaikan, geofence tidak diproses ulang |
| Payload MQTT tidak valid / `vehicle_id` beda dengan topik | Dibuang dengan log `WARN` (MQTT tidak punya nack) |
| Database error saat ingest | Dicoba ulang 3× (jeda 1 detik) |
| RabbitMQ down | Event tetap `pending` di outbox (jatah `attempts` tidak terpakai), terkirim otomatis setelah reconnect |
| Publish gagal saat RabbitMQ terhubung | `attempts + 1`, backoff 1, 2, 4, … detik (maks 5 menit); `failed` setelah `OUTBOX_MAX_ATTEMPTS` dengan log `ERROR` |
| Event outbox `failed` | Tidak dicoba ulang otomatis. Terlihat di log `WARN` tiap `OUTBOX_STATS_INTERVAL` dan `make outbox-status`; kirim ulang dengan `make outbox-requeue` setelah penyebabnya diperbaiki |
| Pesan RabbitMQ duplikat | Inbox `(consumer, message_id)` → di-ack tanpa diproses ulang |
| Pesan RabbitMQ rusak (JSON salah, tanpa `geofence_id`/`message_id`) | Ditolak permanen → masuk `geofence_alerts.dlq` |
| Database tidak tersedia di worker (koneksi putus, timeout) | Nack + requeue setelah jeda 2 detik, terus sampai database pulih |
| Data ditolak database di worker (SQLSTATE kelas `22`/`23`, mis. nilai terlalu panjang) | Ditolak permanen → `geofence_alerts.dlq` (mengulang tidak akan berhasil) |

---

## Status Implementasi & Catatan


- **Sudah berjalan end-to-end:** MQTT ingest → PostgreSQL (lokasi, geofence, outbox) →
  outbox relay → RabbitMQ → worker (inbox, `geofence_events`) → REST API.
  `make start` menjalankan semuanya termasuk mock `publisher`, sehingga event geofence
  baru muncul tiap kendaraan tiba di sebuah geofence (±1 menit sekali).
- Hanya event **masuk** geofence (`geofence_entry`) yang dikirim ke RabbitMQ; keluar
  geofence hanya memperbarui `vehicle_geofence_presence`.
- Pesan MQTT yang datang **tidak berurutan** (timestamp lebih lama tiba belakangan) tetap
  diproses apa adanya untuk status geofence.
- Schema dibuat oleh initdb Postgres **hanya saat volume pertama kali dibuat**.
  Setelah mengubah `schema.sql`, jalankan `make purge` lalu `make start`.
- `make debug` menjalankan binary di bawah Delve (port `2345`), contohnya untuk
  VS Code: *attach* ke `localhost:2345` dengan mode `remote`.
