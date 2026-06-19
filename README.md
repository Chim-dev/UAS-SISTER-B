# Pub-Sub Log Aggregator Terdistribusi

Sistem **pub-sub log aggregator** multi-service yang berjalan penuh di atas
**Docker Compose**. Sistem menjamin **idempotency** (event yang sama tidak
diproses ulang), **deduplication** kuat berbasis *unique constraint*, serta
**transaksi & kontrol konkurensi** yang mencegah *race condition* dan
*lost-update*. Seluruh layanan berkomunikasi hanya di jaringan internal Compose
(tanpa akses ke layanan eksternal publik), dan data disimpan pada *named volumes*
sehingga tetap aman walau container dihapus/di-recreate.

Stack: **Python 3.11 + FastAPI** (aggregator & publisher), **Redis 7** (broker /
message queue via Redis Streams), **PostgreSQL 16** (dedup store transaksional).

---

## 1. Arsitektur

```
                 HTTP POST /publish
   publisher  ─────────────────────────►  aggregator (FastAPI)
 (simulator,                                  │  1) validasi skema event
  ber-duplikat)                               │  2) XADD ke Redis Stream
                                              │  3) received += n (transaksi)
                                              ▼
                                        broker (Redis Streams)
                                        key: events:stream
                                        group: aggregator
                                              │  XREADGROUP '>'
                       ┌──────────────────────┼──────────────────────┐
                       ▼                       ▼                      ▼
                  worker-0                worker-1   ...          worker-3
                       └──────────────────────┬──────────────────────┘
                                              ▼
                       INSERT ... ON CONFLICT (topic,event_id) DO NOTHING
                       di dalam SATU transaksi  +  XACK setelah commit
                                              ▼
                                    storage (PostgreSQL 16)
                                    processed_events  (dedup store)
                                    stats, audit_log
                                    volume: pg_data  (persisten)

   Client  ── GET /events?topic=...  ──►  aggregator ──► query processed_events
   Client  ── GET /stats            ──►  aggregator ──► query stats + topics
```

**Alur singkat:** publisher mengirim event (sebagian sengaja duplikat) ke
`aggregator`. Aggregator memvalidasi lalu menaruh event ke **Redis Stream**
(broker). Beberapa **consumer worker internal** (default 4) membaca stream lewat
*consumer group* dan memproses tiap event secara **idempotent** di dalam satu
transaksi Postgres. Karena dedup dijamin oleh *primary key* `(topic, event_id)`
+ `ON CONFLICT DO NOTHING`, walau banyak worker memproses event yang sama
bersamaan, hanya satu yang tersimpan; sisanya tercatat sebagai duplikat.

### Service Compose

| Service      | Image                | Port keluar | Volume        | Fungsi |
|--------------|----------------------|-------------|---------------|--------|
| `aggregator` | build `./aggregator` | `8080`      | –             | API publish/akses + consumer internal |
| `publisher`  | build `./publisher`  | – (profile `load`) | –      | Generator event + duplikat, ukur metrik |
| `broker`     | `redis:7-alpine`     | tidak ada   | `broker_data` | Message queue (Redis Streams, AOF on) |
| `storage`    | `postgres:16-alpine` | tidak ada   | `pg_data`     | Dedup store + stats + audit log |

Hanya `aggregator` yang meng-expose port (8080) — itu pun semata untuk demo
lokal. `broker` dan `storage` **tidak** punya port keluar; keduanya hanya bisa
diakses dari dalam network `internal` (isolasi jaringan).

---

## 2. Menjalankan

### Prasyarat
- Docker Desktop / Docker Engine + plugin Compose v2.

### Build & jalankan
```bash
docker compose up --build -d
```
Tunggu sampai aggregator sehat (otomatis menunggu Postgres & Redis `healthy`),
lalu akses: `http://localhost:8080`.

Cek kesiapan:
```bash
curl http://localhost:8080/health/ready      # {"status":"ready"}
curl http://localhost:8080/stats
```

### Menjalankan beban (publisher, 20.000 event, 30% duplikat)
Publisher sengaja **tidak** ikut `up` biasa agar beban terkontrol. Jalankan
on-demand lewat profile `load`:
```bash
docker compose --profile load run --rm publisher
```
Atur beban via environment (lihat tabel di bawah), contoh 50.000 event:
```bash
docker compose --profile load run --rm -e TOTAL_EVENTS=50000 publisher
```

### Menghentikan
```bash
docker compose down            # hentikan, volume TETAP ada (data persisten)
docker compose down -v         # hentikan + HAPUS volume (data ikut terhapus)
```

---

## 3. Endpoint API

### `POST /publish`
Menerima **satu** event atau **batch** (array event). Validasi skema bersifat
atomik: bila ada satu item tidak valid, seluruh request ditolak `422` dan tidak
ada yang ter-enqueue.

Event JSON minimal:
```json
{
  "topic": "svc-a",
  "event_id": "svc-a-9f3c...",
  "timestamp": "2026-06-18T10:00:00+00:00",
  "source": "publisher",
  "payload": { "level": "INFO", "msg": "hello" }
}
```
Contoh:
```bash
# single
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"topic":"svc-a","event_id":"e1","timestamp":"2026-06-18T10:00:00Z","source":"curl","payload":{}}'

# batch
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '[{"topic":"svc-a","event_id":"e2","timestamp":"2026-06-18T10:00:01Z","source":"curl","payload":{}}]'
```
Respons: `{"accepted": <n>, "message": "enqueued"}`.

### `GET /events?topic=...&limit=...&offset=...`
Daftar event **unik** yang sudah diproses, terurut berdasarkan `seq` (counter
monotonik urutan ingest). Parameter `topic` opsional.

### `GET /stats`
```json
{
  "received": 22409,
  "unique_processed": 15657,
  "duplicate_dropped": 6752,
  "topics": [{"topic": "svc-a", "count": 4636}, ...],
  "distinct_topics": 16,
  "uptime_seconds": 428.03,
  "pending_in_stream": 0
}
```
Invarian: untuk event valid, `received == unique_processed + duplicate_dropped`
setelah antrian habis (`pending_in_stream == 0`).

### `GET /health/live` & `GET /health/ready`
Liveness dan readiness (readiness mengecek koneksi Postgres + Redis). Dipakai
oleh healthcheck Compose.

---

## 4. Konfigurasi (environment)

### aggregator
| Variable | Default | Keterangan |
|---|---|---|
| `DATABASE_URL` | `postgres://user:pass@storage:5432/db` | DSN Postgres |
| `BROKER_URL` | `redis://broker:6379` | URL Redis |
| `CONSUMER_WORKERS` | `4` | Jumlah worker consumer paralel |
| `STREAM_KEY` | `events:stream` | Nama Redis Stream |
| `CONSUMER_GROUP` | `aggregator` | Nama consumer group |
| `READ_COUNT` | `128` | Pesan per XREADGROUP |
| `AUTOCLAIM_IDLE_MS` | `30000` | Ambang idle reclaim pesan worker mati |

### publisher
| Variable | Default | Keterangan |
|---|---|---|
| `TARGET_URL` | `http://aggregator:8080/publish` | Endpoint tujuan |
| `TOTAL_EVENTS` | `20000` | Total event dikirim |
| `DUPLICATE_RATE` | `0.3` | Proporsi event duplikat |
| `BATCH_SIZE` | `500` | Event per request |
| `CONCURRENCY` | `8` | Request batch paralel |
| `TOPICS` | `svc-a,svc-b,svc-c` | Daftar topic |

---

## 5. Idempotency, Dedup & Konkurensi (inti penilaian)

- **Dedup store persisten:** tabel `processed_events` dengan `PRIMARY KEY
  (topic, event_id)` di Postgres (volume `pg_data`).
- **Idempotent write:** pemrosesan memakai
  `INSERT ... ON CONFLICT (topic, event_id) DO NOTHING RETURNING seq` di dalam
  satu transaksi. `RETURNING` mengembalikan baris hanya saat insert benar-benar
  terjadi → event baru; bila konflik → duplikat. Operasi ini atomik di level DB.
- **Bebas race / double-process:** walau 4 worker memproses event sama
  bersamaan, *unique constraint* menjamin hanya satu insert berhasil. Dibuktikan
  oleh `tests/test_concurrency.py`.
- **Bebas lost-update:** counter `received/unique_processed/duplicate_dropped`
  diupdate dengan `UPDATE ... SET col = col + 1` (row-level lock) di dalam
  transaksi yang sama → konsisten di bawah beban.
- **Isolation level:** `READ COMMITTED` (default Postgres). Cukup karena
  korektness dedup dijamin *unique constraint*, bukan serialization. Trade-off
  dibahas di `report.md`.
- **At-least-once + crash recovery:** Redis Streams + consumer group; worker
  `XACK` hanya setelah commit DB. Pesan yang belum di-ACK tetap di *Pending
  Entries List* dan di-`XAUTOCLAIM` oleh worker lain bila worker mati.
  Redelivery aman karena pemrosesan idempotent.

---

## 6. Tests

21 test berjalan default + 1 test persistensi opt-in.

```bash
# 1) pastikan stack hidup
docker compose up --build -d

# 2) siapkan environment test
python -m venv .venv
.venv/Scripts/python -m pip install -r tests/requirements.txt   # Windows
# source .venv/bin/activate && pip install -r tests/requirements.txt  # Linux/Mac

# 3) jalankan
.venv/Scripts/python -m pytest        # Windows
# pytest                              # Linux/Mac (setelah activate)
```
Atur target via `BASE_URL` (default `http://localhost:8080`).

Cakupan test:
| File | Fokus |
|---|---|
| `test_models.py` | Validasi skema event (unit, tanpa service) |
| `test_api.py` | Health, publish single/batch, filter topic, batch atomic 422 |
| `test_dedup.py` | Idempotency, dedup, counter duplikat |
| `test_concurrency.py` | Tidak ada double-process, stats konsisten di bawah beban |
| `test_stats_ordering.py` | Bentuk `/stats`, `seq` monotonik, stress 2.000 event |
| `test_persistence.py` | Dedup tahan recreate container (opt-in) |

Test persistensi me-recreate container (volume dipertahankan):
```bash
RUN_RESTART_TEST=1 .venv/Scripts/python -m pytest tests/test_persistence.py
```

---

## 7. Persistensi & bukti

Data tersimpan di *named volumes* `pg_data` (Postgres) dan `broker_data`
(Redis AOF). Bukti manual:
```bash
docker compose up --build -d
docker compose --profile load run --rm publisher      # isi data
curl http://localhost:8080/stats                      # catat unique_processed

docker compose up -d --force-recreate storage aggregator
curl http://localhost:8080/stats                      # angka tetap (data persisten)
```
Lokasi data dikelola Docker sebagai named volume; inspeksi via
`docker volume inspect sister_uas_pg_data`.

---

## 8. Observability

- **Logging terstruktur** di aggregator (`uvicorn` + logger `aggregator.*`).
  Duplikat dicatat eksplisit: `DUPLICATE dropped topic=... event_id=...`.
- **Audit log** di tabel `audit_log` (outcome `processed`/`duplicate` + nama
  worker) untuk telusur.
- **Metrik** lewat `GET /stats`. Publisher mencetak throughput & latency.
- Lihat log: `docker compose logs -f aggregator`.

---

## 9. Struktur proyek

```
sister_uas/
├── docker-compose.yml
├── README.md
├── report.md
├── pytest.ini
├── aggregator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── config.py      # konfigurasi dari environment
│       ├── models.py      # skema event & response (Pydantic)
│       ├── db.py          # pool, skema, transaksi & dedup
│       ├── broker.py      # helper Redis Streams
│       ├── consumer.py    # worker consumer paralel
│       └── main.py        # FastAPI: endpoint + lifespan
├── publisher/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── publisher.py       # generator event + metrik
└── tests/
    ├── requirements.txt
    ├── conftest.py
    ├── test_models.py
    ├── test_api.py
    ├── test_dedup.py
    ├── test_concurrency.py
    ├── test_stats_ordering.py
    └── test_persistence.py
```

---

## 10. Video Demo
Video demo: 
https://www.youtube.com/watch?v=yxxArt7-3jc

