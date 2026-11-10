# Progres Tugas 2 Big Data

Diperbarui terakhir: 10 November 2026 (progres: GitHub, muatan awal Databricks, eksplorasi SQL).

## Status komponen

| No | Komponen | Status | Bukti |
| :--- | :--- | :--- | :--- |
| 0 | Folder kerja, dataset, dan pemecahan batch | Selesai | `data/` |
| 1 | Repo GitHub berisi dataset | Selesai | https://github.com/Davi-ARD/tugas2-bigdata-tiktok |
| 2 | Schema, Volume, dan tabel di Databricks | Selesai (muatan awal batch 01) | `notebooks/01_ingest_github_ke_volume.py`, `bukti/screenshots/01` sampai `03` |
| 3 | Eksplorasi SQL dengan Spark | Selesai | `sql/01_eksplorasi.sql`, `bukti/screenshots/04-sql-eksplorasi-hasil.jpg` |
| 4 | Pipeline Bronze, Silver, Gold | Belum | |
| 5 | Job terjadwal atau ber-trigger, terbukti berhasil | Belum | |
| 6 | Model klasifikasi dari Silver | Belum | |
| 7 | Serving endpoint aktif | Belum | |
| 8 | Pengujian Postman | Belum | |
| 9 | Dashboard dari Gold dan custom SQL | Belum | |
| 10 | Nilai Tambah gambar (opsional) | Belum diputuskan | |
| 11 | Video Progres Tugas (10 November) | Naskah siap, belum direkam | `bukti/video/naskah-progress.md` |
| 12 | Demo Tugas (16 November) | Belum | |

## Log Keputusan

| Tanggal | Keputusan | Diputuskan oleh |
| :--- | :--- | :--- |
| 10 Nov 2026 | Studi kasus platform TikTok, dataset claim vs opinion (19.382 baris) | Davi |
| 10 Nov 2026 | Dataset disimpan di GitHub, bukan Google Drive | Davi |
| 10 Nov 2026 | Dikerjakan individu | Davi |
| 10 Nov 2026 | Cakupan video progres: GitHub, schema, Volume, tabel awal, dan 3 query SQL | Davi |
| 10 Nov 2026 | Repo `Davi-ARD/tugas2-bigdata-tiktok`, visibilitas public | Davi |
| 10 Nov 2026 | Push dataset raw dan batch 01. Batch 02 dan 03 ditahan lokal sampai demo akhir | Davi |
| 10 Nov 2026 | Bekerja di Databricks lewat browser, belum memasang Databricks CLI | Davi |
| 10 Nov 2026 | Tabel sementara `tiktok_raw_eksplorasi` untuk eksplorasi, bukan bagian pipeline | Davi |
| 10 Nov 2026 | Video progres direkam sendiri dengan narasi suara | Davi |

## Hal yang masih terbuka

- Sintaks pipeline: `import dlt` sesuai panduan dosen, atau `pyspark.pipelines` yang baru.
- Jenis trigger job: jadwal per 2 menit atau file arrival pada Volume.
- Mengerjakan Nilai Tambah gambar atau tidak.

## Catatan teknis

- Notebook serverless Free Edition bisa mengunduh dari `raw.githubusercontent.com` langsung ke Volume (teruji 10 November 2026), jadi tidak perlu cara pengganti.
- Temuan eksplorasi batch 01: 87,5 persen video dari akun banned berisi klaim (akun aktif 43 persen), dan rata-rata view video klaim sekitar 100 kali video opini.
