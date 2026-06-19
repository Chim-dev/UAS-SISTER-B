<#
    jelaskan-arsitektur.ps1
    -----------------------
    Skrip bantu untuk DEMO VIDEO UAS Sister.
    Menampilkan: penjelasan arsitektur + peta file + ringkasan keputusan desain.

    Cara pakai:
        .\jelaskan-arsitektur.ps1            # tampilkan semua, jeda antar bagian (tekan ENTER untuk lanjut)
        .\jelaskan-arsitektur.ps1 -NoPause   # tampilkan semua sekaligus tanpa jeda

    Skrip ini HANYA menampilkan teks (tidak mengubah apa pun). Aman dijalankan kapan saja.
#>

param(
    [switch]$NoPause
)

function Show-Title {
    param([string]$Text)
    Write-Host ""
    Write-Host ("=" * 68) -ForegroundColor DarkCyan
    Write-Host ("  " + $Text) -ForegroundColor Cyan
    Write-Host ("=" * 68) -ForegroundColor DarkCyan
}

function Show-Sub {
    param([string]$Text)
    Write-Host ""
    Write-Host (">> " + $Text) -ForegroundColor Yellow
}

function Pause-Step {
    if (-not $NoPause) {
        Write-Host ""
        Write-Host "   [tekan ENTER untuk lanjut...]" -ForegroundColor DarkGray
        [void](Read-Host)
    }
}

# ----------------------------------------------------------------------
Show-Title "PROYEK: Pub-Sub Log Aggregator Terdistribusi"

Write-Host @"

  Sebuah sistem terdistribusi yang menerima EVENT LOG dalam jumlah besar,
  lalu menyimpannya TEPAT SATU KALI walaupun event dikirim berkali-kali
  (idempoten + deduplikasi), dengan jaminan TRANSAKSI agar bebas dari
  race condition. Seluruhnya berjalan di atas Docker Compose lokal.

  Tujuan inti yang dibuktikan:
    1. Idempotency  - tiap event diproses sekali saja
    2. Deduplikasi  - duplikat terdeteksi & dibuang via constraint unik
    3. Transaksi    - bebas lost-update walau request paralel
    4. Persistensi  - data selamat walau container dihancurkan
    5. Isolasi      - jaringan lokal, tanpa layanan eksternal
"@ -ForegroundColor Gray
Pause-Step

# ----------------------------------------------------------------------
Show-Title "ARSITEKTUR (4 service, alur kerja)"

Write-Host @"

   [publisher] --POST /publish--> [aggregator] --XADD--> [broker / Redis]
    pengirim                       FastAPI                   antrian
    event +                        (validasi)                   |
    duplikat                                                    | worker ambil ('>')
                                                                v
                                                       [4 consumer worker]
                                                        INSERT ... ON CONFLICT
                                                        (1 transaksi / event)
                                                                |
                                                                v
                                                       [storage / PostgreSQL]
                                                        dedup store permanen
                                                        (named volume pg_data)

  Penjelasan peran (ELI5):
    - publisher  = orang yang mengirim surat (event), kadang ngirim ganda
    - aggregator = kantor pos: terima surat (REST) lalu titip ke antrian
    - broker     = loket antrian: surat menunggu giliran diproses
    - worker     = petugas: ambil surat dari antrian, catat ke buku besar
    - storage    = buku besar (database): catatan permanen + cek 'sudah ada?'

  Kenapa pakai antrian (broker), tidak langsung tulis DB?
    -> Memisahkan TERIMA dan PROSES. Saat ribuan event datang serentak,
       aggregator cukup cepat menampung ke Redis, lalu worker memproses
       dengan kecepatan stabil. Ini pola Pub-Sub / producer-consumer.
"@ -ForegroundColor Gray
Pause-Step

# ----------------------------------------------------------------------
Show-Title "PETA FILE & FUNGSINYA"

Show-Sub "aggregator/ (service utama: REST API + consumer)"
Write-Host @"
   Dockerfile          Resep image (Python 3.11-slim, user non-root, uvicorn)
   requirements.txt    Library: fastapi, uvicorn, asyncpg, redis, pydantic
   app/config.py       Baca konfigurasi dari environment (URL DB, Redis, jumlah worker)
   app/models.py       Skema Event (Pydantic) + aturan validasi + model response
   app/db.py        *  JANTUNG SISTEM. Koneksi Postgres, buat tabel,
                       process_event() = INSERT ... ON CONFLICT DO NOTHING (dedup),
                       fetch_events(), fetch_stats()
   app/broker.py       Helper Redis Streams: publish(XADD), read_group, ack(XACK),
                       autoclaim (recovery worker mati)
   app/consumer.py     ConsumerManager: 4 worker paralel ambil dari Redis ->
                       process_event -> XACK (hanya setelah commit DB sukses)
   app/main.py         Aplikasi FastAPI: endpoint /publish /events /stats /health,
                       startup menyalakan pool DB + Redis + worker
"@ -ForegroundColor Gray

Show-Sub "publisher/ (simulator pengirim event)"
Write-Host @"
   Dockerfile          Resep image publisher
   requirements.txt    Library httpx (kirim HTTP)
   publisher.py        Bangkitkan 20.000 event (30% duplikat), kirim batch,
                       ukur throughput & latency p50/p95
"@ -ForegroundColor Gray

Show-Sub "tests/ (pengujian otomatis - 21 test)"
Write-Host @"
   conftest.py             Perkakas bersama: make_event, poll_until, get_stats
   test_models.py          Validasi skema event (6, tanpa service)
   test_api.py             Health, publish single/batch, tolak 422 (6)
   test_dedup.py           Idempotency & deduplikasi (3)
   test_concurrency.py     Bebas race condition & lost-update (3)
   test_stats_ordering.py  /stats, ordering seq, stress (3)
   test_persistence.py     Data tahan recreate container (1, opt-in)
"@ -ForegroundColor Gray

Show-Sub "root (orkestrasi & dokumentasi)"
Write-Host @"
   docker-compose.yml  *  ORKESTRATOR. Definisi 4 service, jaringan internal,
                          named volumes, healthcheck. Penyala & perekat semua.
   pytest.ini             Konfigurasi pytest
   README.md              Dokumentasi: build, run, endpoint, test
   report.md              Laporan: desain, metrik, mapping Bab 1-13, teori
   DEMO_SCRIPT.md         Skrip rekaman video
"@ -ForegroundColor Gray
Pause-Step

# ----------------------------------------------------------------------
Show-Title "RINGKASAN KEPUTUSAN DESAIN (yang ditanya saat sidang)"

Show-Sub "1. Dedup pakai PRIMARY KEY (topic, event_id) + INSERT ON CONFLICT"
Write-Host @"
   Keputusan : Kunci dedup ditegakkan oleh DATABASE, bukan oleh kode aplikasi.
   Alasan    : Constraint unik bersifat atomik & tahan paralel. Walau 50 worker
               menyisipkan event sama di saat bersamaan, hanya 1 yang berhasil;
               sisanya kena ON CONFLICT DO NOTHING. Tidak mungkin dobel.
   Alternatif ditolak: cek 'SELECT dulu lalu INSERT' -> ada celah race (TOCTOU).
"@ -ForegroundColor Gray

Show-Sub "2. Isolation level READ COMMITTED (default), bukan SERIALIZABLE"
Write-Host @"
   Keputusan : Cukup READ COMMITTED.
   Alasan    : Kebenaran dedup berasal dari CONSTRAINT UNIK, bukan dari urutan
               serialisasi transaksi. Jadi tak perlu isolation termahal yang
               bisa memunculkan serialization-failure & menurunkan throughput.
"@ -ForegroundColor Gray

Show-Sub "3. Anti lost-update pada counter: UPDATE stats SET kolom = kolom + 1"
Write-Host @"
   Keputusan : Increment dilakukan DI DALAM database (read-modify-write 1 perintah).
   Alasan    : Perintah ini memegang row-lock, jadi penambahan dari banyak
               transaksi terakumulasi benar (tidak ada update yang 'hilang').
   Salah jika: baca nilai ke aplikasi, tambah 1, tulis balik -> lost-update.
"@ -ForegroundColor Gray

Show-Sub "4. Broker Redis Streams + consumer group (at-least-once)"
Write-Host @"
   Keputusan : Pakai XREADGROUP + XACK; ACK hanya SETELAH commit DB sukses.
   Alasan    : Kalau worker mati sebelum ACK, pesan tetap 'pending' dan
               diambil ulang via XAUTOCLAIM -> tidak ada event hilang.
               Pengiriman ulang aman karena lapisan dedup idempoten.
"@ -ForegroundColor Gray

Show-Sub "5. Persistensi via NAMED VOLUME (pg_data, broker_data)"
Write-Host @"
   Keputusan : Data DB & Redis disimpan di named volume, di luar lapisan container.
   Alasan    : Container boleh dihapus/dibuat ulang, volume tetap ada -> data
               selamat. Inilah yang dibuktikan di test persistensi.
"@ -ForegroundColor Gray

Show-Sub "6. Isolasi jaringan: hanya aggregator yang expose port 8080"
Write-Host @"
   Keputusan : storage (5432) & broker (6379) TIDAK dipublikasikan ke host.
   Alasan    : Mengurangi permukaan serang; service internal hanya bisa diakses
               dari dalam jaringan Compose. Tidak ada ketergantungan eksternal.
"@ -ForegroundColor Gray

Show-Sub "7. Ordering & audit: kolom seq BIGSERIAL + tabel audit_log"
Write-Host @"
   Keputusan : Tiap event unik dapat nomor urut monotonik (seq), tiap keputusan
               (processed/duplicate) dicatat ke audit_log.
   Alasan    : Mendukung observability & pembuktian urutan pemrosesan.
"@ -ForegroundColor Gray
Pause-Step

# ----------------------------------------------------------------------
Show-Title "INVARIAN KUNCI (kalimat penutup demo)"
Write-Host @"

   received  =  unique_processed  +  duplicate_dropped

   Selama angka di /stats selalu memenuhi persamaan ini, sistem terbukti
   KONSISTEN: setiap event yang masuk dihitung tepat sekali, entah ia
   diproses (unik) atau dibuang (duplikat). Tidak ada yang hilang,
   tidak ada yang dobel.

"@ -ForegroundColor Green

Write-Host "Selesai. Jalankan dengan -NoPause untuk versi tanpa jeda." -ForegroundColor DarkGray
Write-Host ""
