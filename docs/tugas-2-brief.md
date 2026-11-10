# Ringkasan Arahan Tugas 2: End-to-End Databricks Platform

Disalin dari `Tugas_2_Big_Data-v1.pdf`. Jika ada perbedaan, PDF yang berlaku.

**Skenario.** Anda adalah tim data di perusahaan Y yang memakai platform Databricks. Bangun pipeline otomatis yang memproses dataset dari sumber data eksternal, mengolahnya dengan Spark, membuat model AI/ML pada data tersebut, menguji model itu, serta membuat dashboard. Studi kasus bebas, dataset harus open access.

## Komponen wajib

| Komponen | Yang diminta |
| :--- | :--- |
| G-Drive, Table, Volume | Dataset diunggah ke folder Google Drive, GitHub, dan sejenisnya |
| SQL | Exploratory data dengan membuat tabel SQL menggunakan Spark |
| ETL Pipeline (DLT) | Tiga layer. Bronze membaca CSV baru secara incremental. Silver memakai Spark DataFrame untuk membersihkan dan menyiapkan data Bronze. Gold berisi data bersih yang siap dibuat report dan dashboard |
| Jobs (Workflow) | Job terjadwal (misalnya per 2 menit, ketika tabel update, atau ada file baru) untuk menjalankan pipeline Bronze, Silver, Gold. **Buktikan bahwa job ini berhasil** |
| AI/ML | Model klasifikasi atau regresi berdasarkan data dari Silver layer |
| Models and Serving | Deploy model dari Experiments sebagai endpoint API yang aktif |
| Pengujian | Menguji endpoint model melalui aplikasi Postman |
| Dashboard | Dashboard yang membaca dari tabel Gold dan custom SQL |

## Nilai tambah (opsional)

1. Tambahkan sumber data gambar yang relevan dengan studi kasus, disimpan di Volume.
2. Buat job scheduling yang otomatis berjalan ketika ada penambahan data gambar baru.
3. Latih atau gunakan model klasifikasi gambar.
4. Deploy model gambar ini sebagai endpoint API kedua.

## Submission

- Terdiri dari 1 sampai 2 mahasiswa (tugas ini dikerjakan individu).
- Progres Tugas: Selasa, 10 November 2026.
- Demo Tugas: Senin, 16 November 2026.

## Referensi

Playlist Databricks: https://www.youtube.com/playlist?list=PL5I6SZqMk8fGHl64dRlNEwE0g7pmPzKCi (ringkasannya ada di `panduan-praktis-databricks.md`).
