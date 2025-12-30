import pandas as pd
import numpy as np


NUMERIC_COLS = [
    "Sebelumnya",
    "Open Price",
    "First Trade",
    "Tertinggi",
    "Terendah",
    "Penutupan",
    "Selisih",
    "Volume",
    "Nilai",
    "Frekuensi",
    "Index Individual",
    "Offer",
    "Offer Volume",
    "Bid",
    "Bid Volume",
    "Listed Shares",
    "Tradeble Shares",
    "Weight For Index",
    "Foreign Sell",
    "Foreign Buy",
    "Non Regular Volume",
    "Non Regular Value",
    "Non Regular Frequency",
]


PRICE_COLS_FOR_INDICATORS = ["Open Price", "First Trade", "Tertinggi", "Terendah", "Penutupan"]


def parse_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["Tanggal Perdagangan Terakhir"] = pd.to_datetime(
        out["Tanggal Perdagangan Terakhir"],
        errors="raise",
        dayfirst=True,
    ).dt.date

    for c in NUMERIC_COLS:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


def make_indicator_inputs(df: pd.DataFrame) -> pd.DataFrame:
    # Untuk perhitungan indikator: 0 pada harga diperlakukan sebagai NaN
    out = df.copy()
    for c in PRICE_COLS_FOR_INDICATORS:
        out[c] = out[c].where(out[c] != 0, np.nan)
    return out
