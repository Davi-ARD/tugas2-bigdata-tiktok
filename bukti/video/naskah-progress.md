# Naskah Video Progres Tugas 2

Target durasi 2 sampai 4 menit (sekitar 2 menit 50 detik). Direkam sendiri dengan OBS atau Xbox Game Bar (Win+Alt+R), dengan narasi suara.

## Persiapan sebelum merekam

Buka empat tab ini berurutan di satu jendela Chrome, lalu tutup tab lain:

1. `https://github.com/Davi-ARD/tugas2-bigdata-tiktok`
2. Databricks, Catalog, volume `raw_tiktok`, **tab Files** (bukan Overview)
3. Databricks, SQL Editor, query tersimpan `01_eksplorasi`
4. `https://github.com/Davi-ARD/tugas2-bigdata-tiktok/blob/main/PROGRESS.md`

Jaga privasi:

- Di Catalog, jangan buka tab Overview atau Details karena menampilkan email pemilik. Pakai tab Files dan Sample Data.
- Jangan buka halaman Workspace (judul foldernya berisi email) dan jangan klik avatar di pojok kanan atas.
- Jalankan query sekali sebelum merekam supaya SQL warehouse sudah menyala dan hasil keluar cepat.

## Adegan 1. Pembuka (sekitar 20 detik)

Layar: tab 1, halaman depan repo GitHub (README terlihat).

> "Assalamualaikum. Saya Muhammad Davi Ardian, NIM 102022330384, kelas SI-47-EDM-02. Ini video progres Tugas 2 Pengelolaan Big Data, yaitu membangun platform end-to-end di Databricks. Studi kasus saya adalah TikTok: memilah video yang berisi klaim dari yang berisi opini, supaya moderator bisa memprioritaskan video klaim."

## Adegan 2. Repo GitHub (sekitar 30 detik)

Layar: gulir struktur folder, buka `data/batches`, lalu klik `tiktok_batch_01.csv`.

> "Dataset saya simpan di GitHub sebagai sumber data eksternal. Datasetnya dari materi Google Advanced Data Analytics, sifatnya sintetis, berisi 19.382 baris dan 12 kolom. Dataset saya pecah menjadi tiga batch. Batch 01 berisi 15.000 baris sebagai muatan awal. Batch 02 dan 03 sengaja saya tahan untuk membuktikan ingest incremental saat demo akhir. Folder lain sudah saya siapkan untuk notebook, pipeline, SQL, Postman, dan dashboard."

## Adegan 3. Databricks Catalog (sekitar 40 detik)

Layar: tab 2. Tunjukkan pohon `workspace > tiktok_lake`, klik folder `batches` hingga `tiktok_batch_01.csv` (2,40 MB) terlihat. Lalu klik tabel `tiktok_raw_eksplorasi` dan buka tab **Sample Data**.

> "Di Databricks Free Edition saya membuat schema tiktok_lake dan sebuah Volume bernama raw_tiktok. Lewat notebook, file batch 01 diunduh langsung dari GitHub ke Volume ini. File itu lalu dibaca dengan Spark dan disimpan sebagai tabel Delta sementara untuk eksplorasi, isinya 15.000 baris. Kolom targetnya adalah claim_status."

## Adegan 4. Eksplorasi SQL (sekitar 50 detik)

Layar: tab 3. Klik **Run all**, lalu pindah antar hasil dengan panah "Results 1 of 3", "2 of 3", "3 of 3".

> "Berikutnya eksplorasi dengan SQL. Query pertama menghitung jumlah video per claim_status: 7.475 klaim dan 7.315 opini, jadi kelasnya seimbang, ditambah 210 baris kosong yang nanti dibuang di Silver layer."
>
> "Query kedua membandingkan engagement. Video klaim rata-rata ditonton sekitar 500 ribu kali, sedangkan video opini hanya sekitar 5 ribu kali. Selisihnya kira-kira seratus kali lipat."
>
> "Query ketiga melihat status akun pembuat video. Ini temuan yang paling menarik: 87,5 persen video dari akun yang di-banned berisi klaim, dan 77,4 persen untuk akun under review, sedangkan pada akun aktif hanya 43 persen. Artinya engagement dan status akun kemungkinan besar menjadi fitur yang kuat untuk model klasifikasi nanti."

## Adegan 5. Penutup (sekitar 30 detik)

Layar: tab 1, gulir README ke bagian Arsitektur. Lalu tab 4, tabel status di `PROGRESS.md`.

> "Ini rancangan arsitektur lengkapnya. Dari Volume, data masuk ke Bronze layer dengan Auto Loader secara incremental, dibersihkan di Silver, lalu diagregasi di Gold. Pipeline dijalankan oleh job otomatis. Dari Silver saya melatih model klasifikasi klaim atau opini, men-deploy-nya sebagai serving endpoint, mengujinya dengan Postman, dan terakhir membuat dashboard dari tabel Gold."
>
> "Sampai hari ini yang sudah berjalan adalah repo GitHub, schema, Volume, tabel awal, dan eksplorasi SQL. Sisanya saya kerjakan menuju demo tanggal 16 November. Terima kasih."

## Angka rujukan (batch 01)

| Query | Hasil |
| :--- | :--- |
| Jumlah per claim_status | claim 7.475, opinion 7.315, kosong 210 |
| Rata-rata view, like, share | claim 499.914, 166.584, 32.872. opinion 4.953, 1.095, 218 |
| Persen claim per status akun | banned 87,5. under review 77,4. active 43,0 |
