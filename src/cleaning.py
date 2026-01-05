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

MONTH_MAP_ID = {
    "januari":"january","februari":"february","maret":"march","mei":"may","juni":"june","juli":"july",
    "agustus":"august","oktober":"october","desember":"december",
}

def _normalize_month_id(s: str) -> str:
    t = str(s)
    for k, v in MONTH_MAP_ID.items():
        t = t.replace(k, v).replace(k.title(), v.title()).replace(k.upper(), v.upper())
    return t



def parse_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    col = "Tanggal Perdagangan Terakhir"
    x = out[col]

    # 1) parse umum: menangkap datetime excel + teks english yang valid
    dt = pd.to_datetime(x, errors="coerce")

    # 2) fallback: hanya yang gagal, normalisasi bulan Indonesia -> English lalu parse lagi
    mask = dt.isna()
    if mask.any():
        x2 = x[mask].astype(str).map(_normalize_month_id)
        dt2 = pd.to_datetime(x2, errors="coerce", dayfirst=True)
        dt.loc[mask] = dt2

    # 3) safety: kalau masih gagal, stop supaya tidak menulis data salah
    if dt.isna().any():
        bad = out.loc[dt.isna(), col].head(10).tolist()
        raise ValueError(f"Tanggal tidak terbaca (contoh): {bad}")

    out[col] = dt.dt.date

    for c in NUMERIC_COLS:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


def make_indicator_inputs(df: pd.DataFrame) -> pd.DataFrame:
    # Untuk perhitungan indikator: 0 pada harga diperlakukan sebagai NaN
    out = df.copy()
    for c in PRICE_COLS_FOR_INDICATORS:
        out[c] = out[c].where(out[c] != 0, np.nan)
    return out
