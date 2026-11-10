# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Ingest dataset TikTok dari GitHub ke Volume
# MAGIC Muatan awal: batch 01 (15.000 baris) disalin dari repo GitHub ke Unity Catalog Volume,
# MAGIC lalu dibaca dengan Spark dan disimpan sebagai tabel Delta sementara untuk eksplorasi SQL.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.tiktok_lake")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.tiktok_lake.raw_tiktok")

# COMMAND ----------

import urllib.request

URL = "https://raw.githubusercontent.com/Davi-ARD/tugas2-bigdata-tiktok/main/data/batches/tiktok_batch_01.csv"
FOLDER = "/Volumes/workspace/tiktok_lake/raw_tiktok/batches"
TUJUAN = f"{FOLDER}/tiktok_batch_01.csv"

dbutils.fs.mkdirs(FOLDER)
urllib.request.urlretrieve(URL, TUJUAN)
display(dbutils.fs.ls(FOLDER))

# COMMAND ----------

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .csv(TUJUAN)
)
assert df.count() == 15000, f"Jumlah baris tidak sesuai: {df.count()}"
df.printSchema()

# COMMAND ----------

# Tabel sementara untuk eksplorasi. Tabel resmi (Bronze, Silver, Gold) dibuat oleh pipeline.
df.write.mode("overwrite").saveAsTable("workspace.tiktok_lake.tiktok_raw_eksplorasi")
display(spark.table("workspace.tiktok_lake.tiktok_raw_eksplorasi").limit(10))
