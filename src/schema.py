CANON_COLS_28 = [
    "No",
    "Kode Saham",
    "Nama Perusahaan",
    "Remarks",
    "Sebelumnya",
    "Open Price",
    "Tanggal Perdagangan Terakhir",
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


def _norm(s: str) -> str:
    # Normalisasi: trim + spasi ganda jadi 1 + lower
    return " ".join(str(s).strip().split()).lower()


def normalize_and_validate_columns(df):
    # mapping normalized->canonical
    canonical_map = {_norm(c): c for c in CANON_COLS_28}

    incoming = list(df.columns)
    mapped = []
    unknown = []
    for c in incoming:
        key = _norm(c)
        if key in canonical_map:
            mapped.append(canonical_map[key])
        else:
            unknown.append(str(c))

    if unknown:
        raise ValueError(f"Kolom tidak dikenal: {unknown}")

    # cek missing / duplicate
    missing = [c for c in CANON_COLS_28 if c not in mapped]
    if missing:
        raise ValueError(f"Kolom wajib tidak ada: {missing}")

    if len(mapped) != len(CANON_COLS_28):
        raise ValueError(
            f"Jumlah kolom harus {len(CANON_COLS_28)}; terdeteksi {len(mapped)}"
        )

    # rename ke canonical
    df2 = df.copy()
    df2.columns = mapped
    return df2
