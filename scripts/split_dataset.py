"""Memecah data/raw/tiktok_dataset.csv menjadi beberapa batch untuk demo ingest incremental.

Baris diacak dengan seed tetap supaya tiap batch punya proporsi claim/opinion yang mirip.
Batch 01 dipakai untuk muatan awal, batch 02 dan 03 disimpan untuk membuktikan
bahwa Bronze layer dan Job terpicu saat ada file CSV baru.

Jalankan dari root folder proyek:  python scripts/split_dataset.py
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "raw" / "tiktok_dataset.csv"
OUT = ROOT / "data" / "batches"
BATAS = [(0, 15000), (15000, 17000), (17000, None)]


def main() -> None:
    df = pd.read_csv(SRC).sample(frac=1, random_state=42).reset_index(drop=True)
    OUT.mkdir(parents=True, exist_ok=True)
    for i, (awal, akhir) in enumerate(BATAS, start=1):
        bagian = df.iloc[awal:akhir]
        tujuan = OUT / f"tiktok_batch_{i:02d}.csv"
        bagian.to_csv(tujuan, index=False)
        print(f"{tujuan.name}: {len(bagian)} baris")


if __name__ == "__main__":
    main()
