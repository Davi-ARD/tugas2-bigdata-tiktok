# Panduan Praktis Databricks: Konsep, Rekayasa Data, MLOps, dan Analitik Bisnis

Perkembangan arsitektur data modern menuntut integrasi yang mulus antara penyimpanan data berskala besar, komputasi analitik berkecepatan tinggi, otomasi pipeline rekayasa data, hingga siklus hidup kecerdasan buatan (*Machine Learning Operations* atau MLOps). Dokumen ini menyajikan sintesis komprehensif dari sembilan modul praktis implementasi platform Databricks, menguraikan prinsip arsitektur *Lakehouse*, pemrosesan terdistribusi dengan Apache Spark, orkestrasi pipeline *Medallion*, rekayasa fitur dan pelacakan model dengan MLflow, hingga penyajian model melalui REST API dan visualisasi data bisnis interaktif.

---

## 1. Arsitektur Lakehouse dan Fondasi Platform Databricks

Arsitektur pengelolaan data telah melalui pergeseran paradigma selama beberapa dekade terakhir. Memahami evolusi ini menjadi kunci dalam mengapresiasi nilai arsitektural yang dihadirkan oleh konsep *Lakehouse*.

### Evolusi Paradigma Manajemen Data
Sebelum lahirnya *Lakehouse*, industri bergantung pada dua paradigma utama yang memiliki keunggulan dan batasan yang saling bertolak belakang:

*   **Data Warehouse (Dekade 1980-an)**: Beroperasi dengan paradigma *Extract, Transform, Load* (ETL). Berbagai sumber data diekstraksi, ditransformasikan ke dalam skema relasional yang terstruktur rapi dan bersih, lalu dimuat ke dalam repositori tunggal untuk kebutuhan *Business Intelligence* (BI) dan pelaporan. Kelemahannya terletak pada biaya infrastruktur yang mahal, ketidakmampuan menangani data non-relasional, serta skema yang kaku.
*   **Data Lake (Sekitar 2011)**: Diperkenalkan sebagai "sungai data" dengan paradigma *Extract, Load, Transform* (ELT). Data dari berbagai sumber dialirkan dalam bentuk mentah (*raw data*), baik berformat terstruktur, semi-terstruktur (JSON, XML), maupun tidak terstruktur (gambar, audio, video). Biaya penyimpanannya jauh lebih murah dan fleksibel, namun tidak efisien dan sulit dikelola untuk kebutuhan analitik performa tinggi karena ketiadaan manajemen transaksi dan penegakan skema.
*   **Lakehouse Architecture**: Menggabungkan fleksibilitas dan efisiensi biaya penyimpanan data mentah dari *Data Lake* dengan keandalan, kebersihan skema, dan performa analitik tingkat tinggi dari *Data Warehouse*. Format penyimpanan terbuka seperti Delta Lake memungkinkan eksekusi transaksi yang andal sekaligus siap mendukung beban kerja analitik, sains data, dan pembelajaran mesin langsung di atas lapisan penyimpanan yang sama.

| Karakteristik | Data Warehouse | Data Lake | Lakehouse (Databricks) |
| :--- | :--- | :--- | :--- |
| **Paradigma Pemrosesan** | ETL (*Extract, Transform, Load*) | ELT (*Extract, Load, Transform*) | Terpadu (Mendukung ETL dan ELT) |
| **Tipe Data yang Didukung** | Data terstruktur relasional | Terstruktur, semi-terstruktur, tidak terstruktur | Semua format data dalam satu platform |
| **Karakteristik Biaya** | Relatif tinggi dan mahal | Murah dan berskala elastis | Efisien dengan pemisahan *storage* dan *compute* |
| **Beban Kerja Utama** | BI, SQL, dan pelaporan statis | Penyimpanan arsip dan eksplorasi data mentah | BI, SQL, Analitik Streaming, AI/ML, dan Deep Learning |

### Komponen Inti Antarmuka Databricks
Sebagai platform *Lakehouse*, Databricks menyediakan lingkungan terintegrasi yang dapat berjalan di atas infrastruktur penyedia komputasi awan utama (Amazon Web Services, Microsoft Azure, maupun Google Cloud Platform). Antarmuka Databricks tersusun atas modul-modul esensial berikut:

*   **Workspace**: Ruang kerja terpusat untuk mengelola direktori proyek, membuat notebook interaktif, serta menyimpan skrip kode dan kueri analitik.
*   **Catalog**: Lapisan tata kelola data yang mengorganisasi metadata dalam hierarki katalog, skema, tabel, dan *volume*.
*   **Compute Engine**: Kluster komputasi terdistribusi yang mengeksekusi beban kerja. Databricks menyediakan kluster serbaguna (*all-purpose compute*) untuk pekerjaan analitik dan sains data di notebook, serta *Serverless SQL Warehouse* yang dioptimalkan untuk eksekusi kueri cepat.
*   **Jobs & Pipelines**: Mesin orkestrasi untuk membangun pipeline transformasi data terstruktur (*Delta Live Tables*) serta menjadwalkan eksekusi tugas otomatis.
*   **Databricks AI Assistant**: Asisten terintegrasi berbasis model bahasa besar yang membantu pengguna menulis skrip Spark, menyusun kueri SQL, mendiagnosis pesan kesalahan, dan mengoptimalkan kode pemrosesan.

### Manajemen Penyimpanan di Catalog: Tabel vs. Volume
Saat mengelola data di Databricks Catalog, data engineer dihadapkan pada dua opsi penyimpanan utama tergantung pada struktur data yang ditangani:

1.  **Create or Modify Table (Delta Table)**: Digunakan untuk data tabular (misalnya file CSV atau Parquet). Data yang diunggah akan dibaca dan ditransformasikan ke dalam format Delta Lake sehingga langsung terindeks, mendukung penegakan skema, dan siap dieksekusi menggunakan kueri SQL maupun Spark DataFrame.
2.  **Upload Files to Volume (Managed Volume)**: Digunakan untuk menyimpan file mentah, data semi-terstruktur, maupun data tidak terstruktur seperti gambar, video, dan audio yang belum siap dipetakan ke dalam bentuk tabular relasional.

Proses inisialisasi lingkungan analitik dimulai dengan pembuatan skema penyimpanan (misalnya `lake_schema`) di dalam katalog, diikuti dengan pengunggahan data tabular target marketing perbankan (`marketing.csv`) dengan konfigurasi pembatas pemisah titik koma (`;`). Pembacaan tabel ini ke dalam Databricks Notebook dilakukan melalui perintah PySpark:

```python
# Membaca Delta Table dari Catalog ke dalam Spark DataFrame
df = spark.table("workspace.lake_schema.marketing")
display(df.limit(5))
```

Eksekusi kueri pada kluster terdistribusi pertama kali memerlukan waktu inisialisasi untuk mengalokasikan sumber daya komputasi, namun eksekusi berikutnya berlangsung sangat cepat karena memanfaatkan kapabilitas komputasi terdistribusi dalam memori.

---

## 2. Pemrosesan Data Terdistribusi dengan Apache Spark DataFrame

Apache Spark merupakan mesin komputasi terdistribusi berskala besar (*large-scale data analytics engine*) yang menjadi fondasi utama pemrosesan data di Databricks.

### Arsitektur dan Keunggulan Apache Spark
Spark dirancang untuk mengatasi inefisiensi arsitektur pemrosesan data generasi sebelumnya seperti Apache Hadoop MapReduce. Keunggulan operasional Spark bersumber dari:

*   **Pemrosesan Dalam Memori (*In-Memory Processing*)**: Spark mampu mengeksekusi operasi data hingga 100 kali lebih cepat dibandingkan Hadoop karena meminimalkan operasi penulisan dan pembacaan ulang ke disk fisik di antara setiap tahap komputasi.
*   **Dukungan Multi-Bahasa**: Memungkinkan pengembangan menggunakan antarmuka Python (PySpark), SQL, Scala, Java, maupun R.
*   **Komponen Ekosistem Terpadu**: Menyediakan modul terintegrasi yang mencakup Spark SQL dan DataFrame untuk data terstruktur, Spark Streaming untuk data *real-time*, Pandas API on Spark, serta Spark MLlib untuk pembelajaran mesin terdistribusi.

### Operasi Inti Spark DataFrame
Di dalam lingkungan Databricks Notebook, DataFrame Spark merepresentasikan koleksi data terdistribusi yang diorganisasi ke dalam kolom-kolom bernama, menyerupai struktur tabel relasional atau Pandas DataFrame namun dengan kapabilitas eksekusi paralel pada banyak simpul (*worker nodes*).

Perbedaan praktis antara fungsi `show()` dan `display()` terletak pada format keluarannya. Perintah `show()` menghasilkan teks ASCII standar di konsol, sedangkan perintah bawaan Databricks `display()` memvisualisasikan DataFrame ke dalam tabel interaktif lengkap dengan penomoran baris, visualisasi cepat, dan kemampuan penataan kolom.

#### 1. Agregasi Frekuensi dan Pengurutan
Untuk mengetahui sebaran profesi nasabah pada dataset marketing yang memuat lebih dari 45.000 baris rekaman, operasi pengelompokan (*grouping*) dan penghitungan (*counting*) dieksekusi secara instan:

```python
# Menghitung jumlah nasabah per kategori pekerjaan dan mengurutkannya
df_job_count = df.groupBy("job").count().orderBy("count", ascending=False)
display(df_job_count)
```

Hasil agregasi mengidentifikasi kategori pekerjaan dominan seperti *management*, *blue-collar*, dan *technician*, serta kategori dengan frekuensi terkecil seperti *student* dan nilai yang tidak terdata (*unknown*).

#### 2. Agregasi Statistik Numerik
Operasi agregasi lanjutan dapat diterapkan untuk menghitung nilai rata-rata saldo perbankan (*average balance*) berdasarkan latar belakang pendidikan nasabah:

```python
# Menghitung rata-rata saldo perbankan berdasarkan jenjang pendidikan
df_education_balance = df.groupBy("education").avg("balance")
display(df_education_balance)
```

Hasil evaluasi menunjukkan bahwa nasabah dengan latar belakang pendidikan tinggi (*tertiary*) memiliki rata-rata saldo tertinggi dibandingkan jenjang pendidikan menengah (*secondary*) dan dasar (*primary*).

#### 3. Pemfilteran Multikondisi
Pemfilteran rekaman pada DataFrame Spark dilakukan menggunakan metode `filter()` untuk mengisolasi populasi nasabah dengan karakteristik finansial tertentu:

```python
# Memfilter nasabah dengan saldo di atas 25.000 unit moneter
df_wealthy = df.filter(df["balance"] > 25000)
display(df_wealthy)
```

---

## 3. Analisis Data Interaktif Menggunakan SQL Databricks

Selain pemrosesan berbasis skrip kode di notebook, Databricks menyediakan lingkungan khusus *SQL Editor* yang memungkinkan analis data dan praktisi intelijen bisnis melakukan eksplorasi data secara deklaratif.

### Komparasi Penggunaan Notebook dan SQL Editor
Pemilihan antara *Notebook* dan *SQL Editor* ditentukan oleh tujuan analitik dan beban kerja yang dijalankan:

*   **Notebook**: Ideal untuk rekayasa fitur tingkat lanjut, transformasi data kompleks multi-tahap, integrasi pustaka eksternal, pemodelan pembelajaran mesin, serta alur kerja yang membutuhkan kombinasi kode Python, Scala, dan visualisasi kustom.
*   **SQL Editor**: Dikhususkan bagi analisis data cepat, pembuatan kueri ad-hoc, penyusunan laporan periodik (*slicing and dicing*), serta perancangan dataset yang akan dikonsumsi langsung oleh dashboard bisnis.

### Arsitektur Serverless Starter Warehouse
Eksekusi kueri di SQL Editor didukung oleh komputasi khusus bernama *Serverless Starter Warehouse*. Berbeda dengan kluster serbaguna pada notebook yang memerlukan konfigurasi manual dan waktu *warm-up* yang lebih panjang, *Serverless Warehouse* menyediakan infrastruktur instan yang dikelola penuh oleh Databricks. 

Salah satu keunggulan ekonomis dari arsitektur ini adalah mekanisme penghentian otomatis (*auto-suspend*). Apabila gudang data tidak menerima kueri baru selama periode waktu tertentu saat berada dalam kondisi menganggur (*idle*), komputasi akan dinonaktifkan secara otomatis untuk menghentikan pemakaian kredit biaya, dan akan aktif kembali secara transparan saat ada kueri baru yang dikirimkan.

### Pola Analisis Menggunakan Klausa SQL Standar
SQL Editor mendukung sintaks ANSI SQL lengkap untuk mengeksplorasi tabel-tabel Delta di Catalog.

```sql
-- 1. Menampilkan 5 sampel rekaman teratas
SELECT * 
FROM workspace.lake_schema.marketing 
LIMIT 5;

-- 2. Agregasi jumlah nasabah berdasarkan jenis pekerjaan
SELECT job, count(*) AS total_nasabah
FROM workspace.lake_schema.marketing
GROUP BY job
ORDER BY total_nasabah DESC;

-- 3. Menghitung rata-rata saldo berdasarkan bidang pekerjaan
SELECT job, avg(balance) AS rata_rata_saldo
FROM workspace.lake_schema.marketing
GROUP BY job
ORDER BY rata_rata_saldo DESC;

-- 4. Pemfilteran nasabah bernilai tinggi dengan kriteria spesifik
SELECT job, education, balance
FROM workspace.lake_schema.marketing
WHERE balance > 25000;
```

Eksekusi kueri pada Databricks SQL Editor juga dilengkapi fitur *Run Selected*, di mana pengguna dapat menyorot blok pernyataan tertentu untuk dijalankan tanpa harus mengeksekusi keseluruhan naskah SQL di lembar kerja. Riwayat kueri (*Query History*) secara otomatis merekam setiap eksekusi, memfasilitasi audit performa, dan mencatat waktu respons kueri.

---

## 4. Pemrosesan Data Tidak Terstruktur: Klasifikasi Gambar Terdistribusi

Tantangan mendasar dalam platform data konvensional adalah ketidakmampuannya memproses data tidak terstruktur secara efisien. Databricks mengatasi batasan ini dengan menyatukan tata kelola penyimpanan file melalui *Volumes* dan mendistribusikan inferensi *Deep Learning* menggunakan Apache Spark.

### Arsitektur Tata Kelola Volume untuk File Biner
File non-relasional seperti citra digital tidak dapat disimpan langsung di dalam tabel Delta terstruktur. Databricks Catalog menyediakan fasilitas *Managed Volume* (misalnya `lake_volume` di bawah skema `lake_schema`) sebagai media penyimpanan objek biner.

Untuk menavigasi, memeriksa direktori, dan memvalidasi keberadaan file biner di dalam volume, Databricks menyediakan utilitas sistem berkas melalui modul `dbutils`:

```python
# Memeriksa daftar file citra yang tersimpan di dalam Catalog Volume
files = dbutils.fs.ls("workspace.lake_schema.lake_volume")
display(files)
```

### Inferensi Terdistribusi dengan ResNet dan Pandas UDF
Ketika model pembelajaran mendalam (*Deep Learning*) dijalankan pada kumpulan data citra berskala masif, eksekusi sekuensial pada satu simpul akan menciptakan hambatan performa yang parah. Databricks memungkinkan eksekusi inferensi paralel melalui integrasi Spark DataFrame dan model pra-latih (*pre-trained model*) convolutional neural network (CNN) seperti ResNet-50 yang dilatih menggunakan dataset terbuka ImageNet (mencakup 1.000 kelas objek dari 1 juta citra pelatihan).

Mekanisme inferensi terdistribusi diimplementasikan menggunakan pola *Pandas User Defined Function* (UDF) melalui tahapan terstruktur:

1.  **Pemisahan Alur Kerja (Split)**: Simpul pengemudi Spark (*Driver Node*) membaca daftar lokasi path file citra di volume ke dalam Spark DataFrame, lalu memecah daftar path tersebut menjadi beberapa batch terpartisi ke simpul-simpul pekerja (*Worker Nodes*).
2.  **Penerapan Inferensi Paralel (Apply)**: Di setiap simpul pekerja, fungsi Pandas UDF menginisialisasi pustaka TensorFlow dan memuat arsitektur model ResNet satu kali per batch data (bukan memuat ulang model di setiap baris citra individual). Citra biner dibaca, diubah menjadi representasi matriks array numerik, dan diprediksi kelas objeknya.
3.  **Penggabungan Hasil (Combine)**: Seluruh keluaran prediksi dari masing-masing simpul pekerja dikumpulkan kembali oleh Spark Driver dan disatukan ke dalam Spark DataFrame akhir (`df_prediction`).

Hasil klasifikasi terhadap citra uji di dalam volume mengidentifikasi objek secara presisi (misalnya klasifikasi citra kucing sebagai label `tiger_cat`), dan seluruh metadata prediksi langsung disimpan ke dalam Spark DataFrame terstruktur tanpa perlu menulis ulang file citra biner ke basis data.

---

## 5. Ingesti Data Eksternal Otomatis Menggunakan Konektor Fivetran

Selain pengunggahan file manual, arsitektur data perusahaan membutuhkan mekanisme penyerapan data (*data ingestion*) otomatis dari aplikasi pihak ketiga, repositori awan, dan database operasional.

### Ekosistem Konektivitas Pihak Ketiga
Databricks terintegrasi secara natif dengan *Fivetran*, sebuah platform integrasi data terkelola yang menyediakan ratusan konektor otomatis ke berbagai sumber eksternal:

*   **Penyimpanan Cloud & Kolaborasi**: Google Drive, Microsoft OneDrive, Amazon S3, Azure Blob Storage.
*   **Database Relasional & Operasional**: Azure SQL Database, Amazon RDS, PostgreSQL, MySQL, MariaDB.
*   **Aplikasi SaaS & CRM**: Salesforce, HubSpot, Zendesk, Marketo.

### Alur Sinkronisasi Google Drive ke Databricks
Implementasi pipeline ingesti dari Google Drive ke Databricks Catalog dilakukan melalui konfigurasi berbasis antarmuka grafis yang terstandarisasi:

1.  **Autentikasi Aman**: Melakukan otorisasi akun menggunakan protokol *Single Sign-On* (SSO) pengguna Google untuk memberikan hak akses baca terhadap folder target.
2.  **Pemetaan Skema Tujuan**: Menentukan skema penerima di Databricks Catalog (misalnya skema `gdrive`). Fivetran akan secara otomatis membuat dan mengelola skema tujuan tersebut.
3.  **Konfigurasi Sumber**: Memasukkan URL folder Google Drive spesifik (misalnya folder `databricks`) yang menampung file-file yang akan disinkronkan.
4.  **Uji Koneksi & Penjadwalan Frekuensi**: Memvalidasi konektivitas jaringan dan mengatur interval sinkronisasi otomatis (tersedia opsi mulai dari 1 menit, 5 menit, hingga harian). Pada tingkat akun gratis (*Free Plan*), Fivetran menyediakan kuota pemrosesan hingga 500.000 rekaman per bulan.

### Validasi Otomasi Sinkronisasi
Keandalan konektor dibuktikan melalui pengujian dua tahap:

*   **Sinkronisasi Awal**: File `marketing.csv` berukuran ~4 MB yang berisi puluhan ribu rekaman diunggah ke Google Drive. Melalui proses sinkronisasi, data secara otomatis ditransformasikan ke dalam tabel Delta di Databricks dengan nama `gdrive.marketing`.
*   **Deteksi Perubahan Otomatis**: Tanpa menekan tombol sinkronisasi manual, file teks baru hasil *web scraping* berita (`news_dataset.csv`) dimasukkan ke dalam folder Google Drive. Dalam interval 1 menit sesuai jadwal yang ditetapkan, konektor mendeteksi kedatangan file baru dan secara otomatis membentuk tabel tujuan `gdrive.news_dataset`.

Data yang diserap dari repositori eksternal ini langsung tersedia untuk diproses menggunakan skrip Spark di dalam notebook:

```python
# Membaca data hasil sinkronisasi otomatis Fivetran di skema gdrive
df_news = spark.table("workspace.gdrive.news_dataset")
display(df_news.limit(5))
```

---

## 6. Automasi Pipeline ETL dengan Delta Live Tables dan Medallion Architecture

Pengelolaan alur kerja data dari kondisi mentah hingga siap dikonsumsi bisnis membutuhkan arsitektur terstruktur yang menjamin keterlacakan data (*lineage*), kebersihan kualitas data, dan otomasi penjadwalan.

### Prinsip Desain Medallion Architecture
Databricks mempopulerkan kerangka kerja *Medallion Architecture* yang membagi pemrosesan data ke dalam tiga lapisan logis bertingkat:

1.  **Bronze Layer (Raw Data Ingestion)**: Berfungsi sebagai repositori penyimpan data mentah persis seperti format aslinya yang diekstraksi dari sumber eksternal. Tujuannya adalah mengisolasi data sumber sehingga jika terjadi kesalahan pemrosesan di lapisan berikutnya, data mentah tetap aman dan dapat diproses ulang tanpa perlu mengekstraksi ulang dari sistem sumber. Pembacaan data di lapisan ini diatur menggunakan *streaming read* (`spark.readStream`) untuk menangkap data baru secara bertahap.
2.  **Silver Layer (Cleansing & Standardization)**: Data dari lapisan Bronze dibersihkan, divalidasi, dan distandardisasi. Pada tahap ini, kolom-kolom yang ambigu diubah namanya (misalnya kolom `y` menjadi `subscribe`, kolom `loan` menjadi `has_personal_loan`, dan `housing` menjadi `has_housing_loan`), tipe data diperbaiki, dan nilai-nilai anomali disaring. Lapisan Silver menjadi sumber data bersih yang ideal untuk pelatihan model analitik dan kecerdasan buatan.
3.  **Gold Layer (Business Aggregations)**: Data dari lapisan Silver diagregasikan dan dimodelkan secara spesifik sesuai kebutuhan pelaporan bisnis, metrik kunci kinerja (KPI), atau dashboard eksekutif (misalnya tabel ringkasan konversi pemasaran per kategori pekerjaan).

### Deklarasi Pipeline Menggunakan Delta Live Tables (DLT)
*Delta Live Tables* (DLT) adalah kerangka kerja deklaratif di Databricks yang menyederhanakan pembangunan pipeline ETL andal dengan mengotomatisasi manajemen status, penanganan dependensi, dan penegakan kualitas data.

```python
import dlt
from pyspark.sql.functions import col

# 1. Deklarasi Lapisan Bronze: Ingesti Data Mentah secara Streaming
@dlt.table(
    name="marketing_bronze",
    comment="Data mentah nasabah marketing yang diserap langsung dari tabel sumber"
)
def marketing_bronze():
    return spark.readStream.table("workspace.lake_schema.marketing")

# 2. Deklarasi Lapisan Silver: Standardisasi dan Pembersihan Data
@dlt.table(
    name="marketing_silver",
    comment="Data nasabah yang telah dibersihkan dan distandardisasi skemanya"
)
def marketing_silver():
    return (
        dlt.read("marketing_bronze")
        .withColumnRenamed("y", "subscribe")
        .withColumnRenamed("loan", "has_personal_loan")
        .withColumnRenamed("housing", "has_housing_loan")
        .select(
            "age", "job", "marital", "education", "balance",
            "has_housing_loan", "has_personal_loan", "duration", 
            "campaign", "poutcome", "subscribe"
        )
    )

# 3. Deklarasi Lapisan Gold: Agregasi Metrik Konversi untuk Pelaporan Bisnis
@dlt.table(
    name="laporan_konversi_per_pekerjaan",
    comment="Tabel agregat konversi nasabah yang siap dikonsumsi dashboard"
)
def laporan_konversi_per_pekerjaan():
    return (
        dlt.read("marketing_silver")
        .groupBy("job")
        .agg(
            {"*": "count", "subscribe": "sum"}
        )
    )
```

Saat pipeline dieksekusi, Databricks membentuk visualisasi *Directed Acyclic Graph* (DAG) yang menampilkan hubungan ketergantungan antar tabel dari Bronze ke Silver hingga Gold secara transparan.

### Otomasi Orkestrasi Melalui Databricks Jobs
Agar pipeline ETL berjalan otomatis tanpa intervensi manual, Databricks menyediakan modul *Jobs*. Pemicuan (*trigger*) eksekusi tugas dapat dikonfigurasikan berdasarkan jadwal waktu berkala maupun deteksi pembaruan data (*Table Update Trigger*).

Untuk memanfaatkan *Table Update Trigger* pada tabel sumber, mekanisme *Change Data Feed* (CDF) harus diaktifkan terlebih dahulu melalui perintah SQL:

```sql
-- Mengaktifkan pencatatan perubahan data pada Delta Table sumber
ALTER TABLE workspace.lake_schema.marketing 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

Dengan CDF aktif, pemicu Jobs akan memantau log perubahan tabel secara berkelanjutan. Ketika ada baris baru yang ditambahkan ke tabel sumber `marketing` (misalnya penyisipan data nasabah dengan profesi baru seperti `lecturer` dan `scientist`), Job secara otomatis terpicu, mengeksekusi pipeline DLT, memperbarui data lapisan Bronze (~45.221 baris menjadi 45.225 baris), menyelaraskan skema di Silver, dan mengagregasi ulang tabel Gold hingga memuat 14 kategori pekerjaan baru secara otomatis.

---

## 7. Pengembangan dan Pelacakan Model AI/ML dengan MLflow

Evolusi platform Databricks kini menempatkan kapabilitas kecerdasan buatan dan pembelajaran mesin sebagai komponen inti dari arsitektur *Lakehouse*.

### Lingkungan Eksplorasi: LLM Playground
Sebelum melatih model kustom, Databricks menyediakan fitur *LLM Playground* di menu AI/ML. Antarmuka ini memungkinkan praktisi menguji berbagai model bahasa besar sumber terbuka (seperti model dengan parameter 20 miliar hingga 120 miliar) secara interaktif untuk rekayasa prompt, pengujian tugas teks, dan evaluasi keluaran model sebelum diintegrasikan ke alur kerja produksi.

### Siklus Pembelajaran Mesin: Studi Kasus Klasifikasi Nasabah
Dalam skenario bisnis pemasaran perbankan, tujuan analitik prediktif adalah memprediksi apakah seorang nasabah akan berlangganan deposito (*subscribe*) berdasarkan karakteristik demografis dan riwayat interaksi finansial mereka.

*   **Variabel Target ($Y$)**: Kolom biner `subscribe` (bernilai *True/False* atau 1/0).
*   **Fitur Prediktor ($X$)**: Atribut nasabah yang mencakup usia (`age`), pekerjaan (`job`), status pernikahan (`marital`), tingkat pendidikan (`education`), saldo (`balance`), keberadaan kredit perumahan (`has_housing_loan`), pinjaman pribadi (`has_personal_loan`), durasi kontak (`duration`), jumlah kampanye (`campaign`), dan hasil kampanye sebelumnya (`poutcome`).

### Alur Rekayasa Fitur dan Pelatihan Model
Pelatihan model dilakukan menggunakan data bersih dari lapisan `marketing_silver` dengan langkah-langkah terstruktur:

1.  **Transformasi Fitur Kategorikal (One-Hot Encoding)**: Algoritma pembelajaran mesin mensyaratkan masukan numerik. Fitur berbasis teks diubah menjadi kolom biner menggunakan fungsi `pd.get_dummies()` sehingga setiap nilai kategori unik (misalnya `job_management`, `job_technician`, `job_lecturer`) menjadi kolom independen bernilai 0 atau 1.
2.  **Pemisahan Data Latih dan Uji Terstratifikasi (*Stratified Split*)**: Data dibagi menjadi 80% data latih (*training set*) dan 20% data uji (*test set*). Parameter `stratify=y` diterapkan untuk memastikan proporsi kelas nasabah yang berlangganan dan tidak berlangganan terdistribusi secara seimbang antara data latih dan data uji.
3.  **Pelatihan Algoritma Random Forest**: Menggunakan `RandomForestClassifier` untuk membangun ansambel pohon keputusan yang mampu menangkap interaksi non-linear antar variabel nasabah.

### Tata Kelola Eksperimen dengan MLflow dan Model Registry
Pelatihan model diintegrasikan dengan `mlflow` untuk mencatat parameter hiperparameter, metrik performa, dan artefak biner model secara otomatis:

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier

# Menautkan proses pelatihan ke MLflow Experiment aktif
mlflow.set_experiment("/Users/workspace/classifier_marketing")

with mlflow.start_run():
    # Inisialisasi dan pelatihan model Random Forest
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    
    # Pencatatan parameter dan model
    mlflow.sklearn.log_model(rf, "random_forest_model")
```

Evaluasi pada pengujian model menghasilkan akurasi pelatihan mencapai rentang 88% hingga 89%. Model terbaik selanjutnya didaftarkan ke *Databricks Model Registry* di bawah jalur namespace Catalog (misalnya `workspace.gdrive.classifier_marketing_v1`). Registrasi ini mengunci versi model, mencatat deskripsi operasional, dan menetapkan kepemilikan model untuk tata kelola MLOps yang siap didistribusikan ke lingkungan produksi.

---

## 8. Penerapan dan Pelayanan Model (Model Deployment & Serving)

Model pembelajaran mesin yang telah tersimpan di *Model Registry* harus dapat diakses oleh aplikasi hilir secara *real-time* dengan latensi rendah. Databricks menyediakan fasilitas *Model Serving* untuk mempublikasikan model sebagai endpoint REST API yang terkelola penuh.

### Konfigurasi Endpoint Model Serving
Penerapan model dilakukan dengan memilih model terdaftar dari Catalog dan mengaktifkan opsi *Serve this Model*. Konfigurasi endpoint mencakup:

*   **Nama Endpoint**: Menentukan identitas unik endpoint (misalnya `classifier_marketing`) yang akan menjadi basis URL layanan API.
*   **Spesifikasi Komputasi**: Menentukan kapasitas komputasi (CPU/GPU dan memori) yang menampung model. Databricks mengelola skalabilitas infrastruktur secara dinamis dari status inisialisasi (*pending*) hingga siap melayani permintaan (*ready*).

### Validasi Skema Payload JSON
Kunci keberhasilan inferensi pada model terletak pada kesesuaian struktur data masukan dengan skema fitur saat pelatihan. Payload JSON yang dikirimkan harus merefleksikan seluruh kolom hasil *One-Hot Encoding*, termasuk variabel profesi yang ditambahkan secara dinamis (seperti `job_lecturer` dan `job_scientist`).

Pengujian sensitivitas parameter mengungkapkan faktor-faktor kunci yang membalikkan keputusan klasifikasi model:
*   Jika nasabah memiliki durasi panggilan (`duration`) singkat dan status kampanye sebelumnya gagal, model memprediksi nilai negatif (`false`).
*   Jika durasi panggilan ditingkatkan (misalnya menjadi 600 detik), status kampanye sebelumnya bernilai sukses (`poutcome_success = true`), dan nasabah tidak memiliki beban pinjaman rumah (`has_housing_loan = false`), respons prediksi model beralih menjadi positif (`true` / nasabah berminat berlangganan).

### Integrasi Sistem Eksternal Menggunakan Postman
Model yang disajikan dapat dikonsumsi oleh aplikasi perbankan eksternal melalui protokol HTTP standar. Pengujian interkoneksi dilakukan menggunakan perangkat lunak *Postman* dengan spesifikasi permintaan berikut:

*   **Metode HTTP**: `POST`
*   **Alamat URL**: `https://<databricks-instance-url>/serving-endpoints/classifier_marketing/invocations`
*   **Autentikasi Keamanan**: Skema *Bearer Token*. Token akses dibuat melalui menu *User Settings* > *Developer* > *Access Tokens* pada antarmuka Databricks. Token tersebut dimasukkan ke dalam *Header* permintaan HTTP (`Authorization: Bearer <personal-access-token>`).
*   **Format Body**: Raw JSON yang memuat struktur array rekaman nasabah.

```json
{
  "dataframe_records": [
    {
      "age": 35,
      "balance": 15000,
      "duration": 600,
      "campaign": 1,
      "has_housing_loan": false,
      "has_personal_loan": false,
      "poutcome_success": true,
      "poutcome_failure": false,
      "job_lecturer": false,
      "job_scientist": false,
      "job_management": true
    }
  ]
}
```

Permintaan yang dikirimkan langsung menerima respons payload JSON dari kluster komputasi Databricks dengan keluaran nilai prediksi:

```json
{
  "predictions": [true]
}
```

Integrasi ini membuktikan kapabilitas *end-to-end* Databricks dalam menjembatani pelatihan model sains data internal dengan konsumsi aplikasi bisnis eksternal secara terstandarisasi dan aman.

---

## 9. Visualisasi Data dan Business Intelligence dengan Databricks Dashboard

Tahap akhir dari siklus hidup data adalah penyajian wawasan strategis kepada pembuat keputusan bisnis melalui *Databricks Dashboard* (sebelumnya dikenal sebagai Lakeview / AI/BI Dashboard).

### Optimalisasi Performa Dashboard Berbasis Gold Layer
Kesalahan umum dalam perancangan dashboard analitik adalah menghubungkan widget visualisasi langsung ke tabel mentah atau lapisan Bronze/Silver yang berisi puluhan ribu hingga jutaan baris data. Praktik ini membebani komputasi dan memperlambat waktu muat dashboard.

Arsitektur Databricks mengatasi masalah ini dengan menghubungkan dashboard langsung ke lapisan **Gold Layer** (misalnya tabel `laporan_konversi_per_pekerjaan`). Tabel Gold ini telah diagregasikan sebelumnya dalam pipeline ETL sehingga hanya memuat sekitar 14 baris data ringkas yang merepresentasikan setiap kelompok profesi nasabah. Akibatnya, pembaruan grafik dan visualisasi berlangsung instan.

### Komponen Widget dan Tipe Visualisasi
Penyusunan dashboard bisnis interaktif menggabungkan berbagai komponen visual untuk mengungkap wawasan dari berbagai sudut pandang:

*   **Header & Textbox**: Memberikan label kontekstual, deskripsi metrik, dan panduan navigasi bagi pengambil keputusan.
*   **Distribusi Total Nasabah (Bar Chart / Heatmap)**: Memetakan sebaran volume nasabah berdasarkan kategori pekerjaan. Visualisasi ini memperlihatkan bahwa segmen nasabah terbesar terkonsentrasi pada sektor manajemen, pekerja kasar (*blue-collar*), dan teknisi.
*   **Tingkat Konversi Relatif (Pie Chart)**: Menunjukkan persentase kontribusi konversi antar profesi. Grafik ini mengungkap anomali positif di mana profesi dosen (*lecturer*) dan mahasiswa (*student*) memiliki rasio konversi yang sangat tinggi meskipun total populasi nasabahnya relatif kecil.
*   **Analisis Korelasi Linier (Scatter Plot)**: Digunakan untuk menguji hipotesis hubungan antara total nasabah yang dihubungi dengan jumlah nasabah yang bersedia berlangganan. Plot sebaran menunjukkan kecenderungan linier positif, mengonfirmasi bahwa penambahan volume kontak nasabah secara umum berbanding lurus dengan peningkatan jumlah nasabah berlangganan deposito.

### Penyempurnaan Dataset Menggunakan Custom SQL Kueri
Ketika data agregat awal masih memuat anomali pelaporan—seperti keberadaan kategori pekerjaan yang tidak terdefinisi (`unknown`)—penyesuaian data dapat dilakukan langsung di dalam lapisan dataset dashboard tanpa harus merombak tabel basis data fisik.

Melalui editor SQL bawaan dashboard, analis dapat membuat kueri kustom untuk mengecualikan rekaman tidak relevan dan membatasi visualisasi pada peringkat sepuluh besar (*Top 10*):

```sql
-- Menyaring kategori anomali dan menampilkan 10 profesi dengan konversi tertinggi
SELECT job, total_nasabah, jumlah_berlangganan, tingkat_konversi
FROM workspace.gdrive.laporan_konversi_per_pekerjaan
WHERE job != 'unknown'
ORDER BY tingkat_konversi DESC
LIMIT 10;
```

Pembaruan kueri ini secara otomatis mengeliminasi kategori `unknown` dari visualisasi *Top 10 Jobs* dan memperbarui seluruh widget turunan yang terikat pada dataset tersebut.

### Tata Kelola Penerbitan dan Keamanan Dashboard
Dashboard yang telah selesai dirancang dapat dipublikasikan melalui mekanisme *Publish Dashboard*. Databricks memberlakukan kontrol akses berbasis peran (*Role-Based Access Control* / RBAC):
*   Dashboard internal tidak dapat diakses secara publik terbuka di internet.
*   Akses hanya dapat diberikan kepada pengguna atau grup kerja terdaftar di dalam ruang kerja Databricks dengan tingkat izin tertentu (*Can View*, *Can Run*, atau *Can Edit*).

Mekanisme tata kelola ini memastikan bahwa wawasan bisnis strategis tetap terlindungi sekaligus mudah diakses oleh jajaran manajemen puncak (*top management*) untuk perumusan kebijakan perbankan berbasis data yang terpercaya.
