# Tugas 2 Big Data: End-to-End Databricks Platform (Studi Kasus TikTok)

Muhammad Davi Ardian, NIM 102022330384, SI-47-EDM 01. Mata kuliah Pengelolaan Big Data, Telkom University.

Proyek ini membangun pipeline data end-to-end di Databricks untuk memilah video TikTok yang berisi klaim dari yang berisi opini. Dataset disimpan di repo GitHub ini, disalin ke Unity Catalog Volume, diolah melalui layer Bronze, Silver, dan Gold, dipakai melatih model klasifikasi yang disajikan sebagai endpoint API, lalu divisualisasikan dalam dashboard.

## Struktur folder

| Folder | Isi |
| :--- | :--- |
| `PROGRESS.md` | Status tiap komponen dan log keputusan |
| `docs/` | PDF tugas, ringkasannya, dan panduan Databricks |
| `data/raw/` | Dataset asli `tiktok_dataset.csv` (19.382 baris) |
| `data/batches/` | Dataset yang dipecah tiga untuk demo ingest incremental (batch 02 dan 03 menyusul saat demo) |
| `scripts/` | Skrip pemecah dataset |
| `notebooks/`, `pipelines/`, `sql/` | Kode yang dijalankan di Databricks |
| `postman/`, `dashboard/` | Koleksi pengujian API dan ekspor dashboard |
| `bukti/` | Screenshot dan video |

## Dataset

`tiktok_dataset.csv` berasal dari materi Google Advanced Data Analytics. Datanya sintetis, dibuat untuk keperluan pembelajaran, dan tersedia terbuka di Kaggle maupun GitHub. Kolom target adalah `claim_status` dengan nilai `claim` atau `opinion`.

## Arsitektur

```
GitHub (data/batches/*.csv)
   -> Unity Catalog Volume  workspace.tiktok_lake.raw_tiktok
   -> Bronze  tiktok_bronze   (Auto Loader, baca CSV baru secara incremental)
   -> Silver  tiktok_silver   (Spark DataFrame: bersihkan, rename, cast, fitur turunan)
   -> Gold    gold_*          (agregasi untuk laporan dan dashboard)
   -> Job terjadwal atau ber-trigger yang menjalankan pipeline
   -> Model klasifikasi claim vs opinion dari Silver (MLflow, Unity Catalog)
   -> Serving endpoint REST -> diuji dengan Postman
   -> Dashboard dari tabel Gold dan custom SQL
```

Status pengerjaan tiap komponen ada di `PROGRESS.md`.
