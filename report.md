# Laporan UAS Sistem Terdistribusi
## Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Kontrol Konkurensi

> Catatan: Bagian Teori (T1–T10) diisi mandiri oleh penulis (lihat kerangka di
> Bagian 8). Laporan ini memuat ringkasan sistem, keputusan desain, analisis
> performa & konkurensi, persistensi, observability, keterkaitan Bab 1–13, dan
> referensi.

---

## 1. Ringkasan Sistem dan Arsitektur

Sistem ini adalah *log aggregator* bergaya **publish–subscribe** yang dibangun
sebagai beberapa *microservice* dan diorkestrasi penuh dengan Docker Compose.
Tujuan utamanya: menerima aliran event log dalam jumlah besar (≥ 20.000 event,
≥ 30% duplikat), memprosesnya **tepat satu kali** (idempotent), dan menjaga
konsistensi data di bawah konkurensi tinggi.

Empat service berjalan pada satu jaringan internal Compose:

- **aggregator** (Python + FastAPI): menyediakan API `POST /publish`,
  `GET /events`, `GET /stats`, serta menjalankan *consumer* internal (4 worker
  paralel) yang memproses event dari broker.
- **publisher** (Python): simulator yang membangkitkan event—termasuk duplikat
  yang disengaja—lalu mem-publish secara batch dan mengukur metrik.
- **broker** (Redis 7, Redis Streams): antrian pesan durable dengan *consumer
  group* untuk semantik *at-least-once*.
- **storage** (PostgreSQL 16): *dedup store* transaksional, tabel statistik, dan
  *audit log*.

Aliran data: `publisher → POST /publish → aggregator (validasi + XADD) → Redis
Stream → consumer worker (transaksi dedup + XACK) → Postgres`. Pembaca mengakses
hasil lewat `GET /events` dan `GET /stats`.

Hanya `aggregator` yang meng-expose port 8080 untuk demo lokal; `broker` dan
`storage` tidak punya port keluar sehingga terisolasi di jaringan Compose.

---

## 2. Keputusan Desain

### 2.1 Idempotency dan Deduplication
Dedup dijamin secara struktural oleh **primary key `(topic, event_id)`** pada
tabel `processed_events`. Setiap event diproses dengan:

```sql
INSERT INTO processed_events (topic, event_id, event_ts, source, payload)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING seq;
```

Bila `RETURNING` mengembalikan baris, event baru → `unique_processed += 1`. Bila
tidak (konflik), event duplikat → `duplicate_dropped += 1`. Seluruhnya berada di
**satu transaksi**, sehingga insert + update counter + audit log bersifat
atomik. Pendekatan *insert-on-conflict* ini adalah **idempotent write pattern**:
mengirim event yang sama berkali-kali (atau memprosesnya ulang setelah crash)
tidak mengubah hasil akhir.

### 2.2 Dedup Store Persisten
Dedup store diletakkan di PostgreSQL pada *named volume* `pg_data`. Karena state
dedup ada di database (bukan di memori proses), restart/recreate container
aggregator maupun storage tidak menghapus jejak event yang sudah diproses.

### 2.3 Transaksi & Kontrol Konkurensi
- **Cegah double-process:** *unique constraint* memastikan—di antara banyak
  worker yang memproses event identik secara bersamaan—hanya satu transaksi yang
  berhasil meng-insert; sisanya mendapat konflik dan dihitung duplikat. Tidak
  diperlukan *lock* eksplisit di aplikasi; korektness diserahkan ke jaminan
  atomik database.
- **Cegah lost-update:** counter statistik diupdate dengan
  `UPDATE stats SET col = col + 1 WHERE id = 1`. Postgres mengambil *row-level
  lock* pada baris stats selama transaksi, sehingga increment dari banyak worker
  terserialisasi dengan benar tanpa kehilangan update.

### 2.4 Isolation Level dan Trade-off
Sistem memakai **READ COMMITTED** (default Postgres). Pertimbangan:
- Anomali utama yang relevan untuk dedup adalah *write–write conflict* pada
  event identik. Ini sudah dimitigasi sepenuhnya oleh *unique constraint*
  (bukan oleh isolation), sehingga READ COMMITTED memadai dan lebih murah.
- READ COMMITTED rentan terhadap *phantom read* dan *write skew* pada pola
  baca-lalu-tulis. Sistem ini sengaja menghindari pola tersebut: keputusan
  "duplikat atau bukan" tidak diambil dengan `SELECT` lebih dulu lalu `INSERT`
  (yang rawan *time-of-check/time-of-use*), melainkan dengan satu pernyataan
  atomik `INSERT ... ON CONFLICT`.
- Bila kebutuhan berubah ke pola yang rawan *write skew*, mitigasinya adalah
  naik ke **SERIALIZABLE** disertai **retry** pada `serialization_failure`
  (SQLSTATE 40001). Trade-off-nya: throughput lebih rendah dan kemungkinan abort
  yang harus di-retry. Untuk beban aggregator ini, biaya tersebut tidak
  diperlukan.

### 2.5 Reliability, Ordering, dan Retry
- **At-least-once delivery:** broker memakai Redis Streams + *consumer group*.
  Worker `XACK` **hanya setelah** transaksi DB commit. Jika worker crash sebelum
  ACK, pesan tetap di *Pending Entries List* dan di-`XAUTOCLAIM` oleh worker lain
  (crash recovery). Redelivery aman karena pemrosesan idempotent.
- **Ordering:** *total ordering* global tidak dibutuhkan untuk korektness dedup,
  karena identitas event ditentukan `(topic, event_id)`, bukan urutan. Sistem
  menyediakan **ordering monotonik praktis** via kolom `seq BIGSERIAL` (urutan
  ingest) ditambah `event_ts` (timestamp event). `GET /events` mengembalikan
  hasil terurut `seq`. Event yang datang *out-of-order* tetap diterima.
- **Retry + backoff:** consumer me-retry error transient DB dengan *exponential
  backoff* (0.1s → 2s, maks 5 kali) sebelum membiarkan pesan tetap pending untuk
  reclaim.

### 2.6 Batch Atomic
`POST /publish` menerima single atau batch. Validasi skema bersifat atomik: bila
ada satu item batch tidak valid, seluruh request ditolak `HTTP 422` dan tidak ada
yang ter-enqueue. Kebijakan ini menjaga *integrity* boundary batch.

---

## 3. Analisis Performa / Metrik

Diukur pada lingkungan pengembangan (Docker Desktop, 4 consumer worker,
`BATCH_SIZE=500`, `CONCURRENCY=8`). Angka absolut bergantung perangkat keras;
yang penting adalah **konsistensi data** tetap terjaga.

**Beban uji:** 20.000 event, 30% duplikat.

| Metrik | Nilai terukur |
|---|---|
| Total event dikirim | 20.000 |
| Event unik | 14.006 |
| Event duplikat | 5.994 (30,0%) |
| Durasi publish (enqueue) | 0,48 s |
| Throughput publish | ± 41.514 event/s |
| Latency batch p50 | 93,7 ms |
| Latency batch p95 | 176,6 ms |
| Consumer worker | 4 (paralel) |

**Konsistensi setelah antrian habis (`pending_in_stream = 0`):**

```
received           = 22.409
unique_processed   = 15.657
duplicate_dropped  =  6.752
=> unique_processed + duplicate_dropped = 22.409 = received   ✔ konsisten
```

(Angka `received` mencakup akumulasi event dari uji fungsional + beban 20.000.)
Invarian `received == unique_processed + duplicate_dropped` terpenuhi, yang
membuktikan tidak ada event yang hilang maupun diproses ganda.

---

## 4. Hasil Uji Konkurensi

Diverifikasi otomatis oleh `tests/` (21 test lulus, 1 opt-in):

- **`test_concurrent_same_event_no_double_process`** — 50 request paralel
  mengirim `(topic, event_id)` identik; hasil akhir tepat **1 baris** unik.
- **`test_concurrent_unique_events_all_processed`** — 200 event unik paralel;
  seluruhnya terproses tepat sekali, `unique_processed` bertambah tepat 200.
- **`test_stats_no_lost_update_under_load`** — 100 pengiriman event identik
  paralel; hasil tepat `unique = 1`, `duplicate = 99`, `received = 100`
  (bukti bebas *lost-update*).
- **`test_dedup_survives_container_recreate`** (opt-in) — setelah
  `docker compose up --force-recreate storage aggregator`, event lama tetap ada
  dan publish ulang tidak diproses kembali (bukti persistensi + idempotency
  lintas restart).

---

## 5. Persistensi

Data berada pada *named volumes*: `pg_data` (PostgreSQL) dan `broker_data`
(Redis AOF). `docker compose down` (tanpa `-v`) menghentikan container tetapi
mempertahankan volume; data—termasuk seluruh dedup store dan counter
statistik—tetap utuh saat container dibuat ulang. Hal ini dibuktikan oleh test
persistensi pada Bagian 4.

---

## 6. Observability

- **Logging:** logger `aggregator.*` mencetak event penting; duplikat dicatat
  eksplisit (`DUPLICATE dropped topic=... event_id=... worker=...`).
- **Audit log:** tabel `audit_log` menyimpan tiap *outcome* (`processed` /
  `duplicate`) beserta nama worker dan waktu—berguna untuk telusur.
- **Metrik runtime:** `GET /stats` (received, unique, duplicate, daftar topic,
  uptime, pending in stream). Publisher mencetak throughput & latency.
- **Health probe:** `GET /health/live` dan `GET /health/ready` (mengecek
  Postgres + Redis) dipakai healthcheck Compose untuk *readiness/liveness*.

---

## 7. Keterkaitan dengan Bab 1–13

Tabel berikut memetakan fitur implementasi ke bab buku utama (Coulouris dkk.,
2012). Pembahasan teoritis mendalam ada di Bagian 8 (T1–T10).

| Bab | Topik | Wujud dalam sistem |
|---|---|---|
| 1 | Karakteristik sistem terdistribusi | Multi-service, *failure independence*, concurrency |
| 2 | Model arsitektur | Pub–sub + microservices, pemisahan broker/storage |
| 3–4 | Komunikasi & penamaan | HTTP/JSON, penamaan `topic` + `event_id` unik |
| 5 | Waktu & ordering | `seq` monotonik + `event_ts`; toleransi out-of-order |
| 6 | Toleransi kegagalan | Retry+backoff, XAUTOCLAIM, graceful restart, healthcheck |
| 7 | Konsistensi & replikasi | Eventual consistency via idempotency + dedup |
| 8 | Transaksi | Transaksi ACID per-event, batch atomic |
| 9 | Kontrol konkurensi | Unique constraint, upsert, anti lost-update, isolation |
| 10 | Keamanan | Isolasi jaringan Compose, non-root container, tanpa eksternal |
| 11 | Penyimpanan terdistribusi | Named volumes, Redis AOF, persistensi |
| 12 | Sistem berbasis web | API REST FastAPI |
| 13 | Koordinasi | Consumer group, orkestrasi Compose, readiness/liveness |

---

## 8. Bagian Teori (T1–T10) — kerangka untuk diisi

> Jawab ringkas 150–250 kata per poin, sertakan sitasi APA 7th, soroti Bab 8–9
> dengan contoh dari rancangan pada laporan ini.

- **T1 (Bab 1):** Karakteristik sistem terdistribusi dan trade-off desain pub-sub aggregator.
- **T2 (Bab 2):** Kapan memilih publish–subscribe dibanding client–server? Alasan teknis.
- **T3 (Bab 3):** At-least-once vs exactly-once; peran idempotent consumer.
- **T4 (Bab 4):** Skema penamaan `topic` & `event_id` (unik, collision-resistant) untuk dedup.
- **T5 (Bab 5):** Ordering praktis (timestamp + monotonic counter); batasan & dampaknya.
- **T6 (Bab 6):** Failure modes & mitigasi (retry, backoff, durable dedup store, crash recovery).
- **T7 (Bab 7):** Eventual consistency pada aggregator; peran idempotency + dedup.
- **T8 (Bab 8):** Desain transaksi: ACID, isolation level, strategi menghindari lost-update.
- **T9 (Bab 9):** Kontrol konkurensi: locking/unique constraint/upsert; idempotent write pattern.
- **T10 (Bab 10–13):** Orkestrasi Compose, keamanan jaringan lokal, persistensi (volume), observability.

---

## 9. Referensi (APA 7th)

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed
systems: Concepts and design* (5th ed.). Pearson Education.

PostgreSQL Global Development Group. (2024). *PostgreSQL 16 documentation*.
https://www.postgresql.org/docs/16/

Redis Ltd. (2024). *Redis Streams documentation*. https://redis.io/docs/latest/develop/data-types/streams/
