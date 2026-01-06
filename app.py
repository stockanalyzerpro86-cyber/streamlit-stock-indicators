import pandas as pd
import streamlit as st

from src.io_excel import read_input_excel
from src.schema import normalize_and_validate_columns
from src.cleaning import parse_and_cast, make_indicator_inputs
from src.indicators import compute_indicators
from src.export import to_excel_bytes
from src.sheets_client import build_sheets_service, get_values, write_values
from src.retention import filter_keep_last_trading_days


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

KEY_COLS = ["Tanggal Perdagangan Terakhir", "Kode Saham", "Nama Perusahaan"]
KEY2 = ["Tanggal Perdagangan Terakhir", "Kode Saham"]


OUT_A_INDICATORS = [
    "SMA-5",
    "OBV",
    "TR",
    "ATR-9",
    "Gain Harian",
    "Loss Harian",
    "AvgGain-9",
    "AvgLoss-9",
    "RSI-9",
    "Min Low-9",
    "Max High-9",
    "%K Stoch-9",
    "%D Stoch-3",
    "VWAP-5",
    "MFM",
    "CMF-9",
    "MA-20",
    "MA-50",
    "Vol 20D Avg",
    "4-Week High",
]

OUT_B_INDICATORS = [
    "8-Week High",
    "13-Week High",
    "52-Week High",
    "BB Middle",
    "BB Upper",
    "BB Lower",
    "Std Dev 20D",
    "EMA-5",
    "EMA-12",
    "MFI-14 (Money Flow Index)",
    "ADL (Accumulation/Distribution Line)",
    "VPT (Volume Price Trend)",
    "Range Ratio (Daily Range / ATR)",
    "Close Position % (0-100%)",
    "Force Index (Raw)",
    "Force Index EMA-13",
    "Keltner Upper",
    "Keltner Lower",
    "EMA-20",
]


def _read_sheet_as_df(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    values = get_values(service, spreadsheet_id, f"{sheet_name}!A1:ZZ")
    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)

    width = len(header)
    norm_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]
    return pd.DataFrame(norm_rows, columns=header)


def _df_to_values(df: pd.DataFrame):
    return [df.columns.tolist()] + df.astype(object).where(pd.notnull(df), "").values.tolist()


def _upsert_by_key(existing: pd.DataFrame, incoming: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    if existing is None or existing.empty:
        out = incoming.copy()
        return out

    # Samakan kolom: pastikan existing punya semua kolom incoming
    for c in incoming.columns:
        if c not in existing.columns:
            existing[c] = ""

    for c in existing.columns:
        if c not in incoming.columns:
            incoming[c] = ""

    existing = existing[incoming.columns.tolist()]

    ex = existing.copy()
    inc = incoming.copy()

    for c in key_cols:
        ex[c] = ex[c].astype(str)
        inc[c] = inc[c].astype(str)

    ex_idx = ex.set_index(key_cols, drop=False)
    inc_idx = inc.set_index(key_cols, drop=False)

    ex_idx.update(inc_idx)
    new_keys = inc_idx.index.difference(ex_idx.index)
    appended = pd.concat([ex_idx, inc_idx.loc[new_keys]], axis=0)

    out = appended.reset_index(drop=True)
    return out


def _sort_date_emiten(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Tanggal Perdagangan Terakhir"] = pd.to_datetime(out["Tanggal Perdagangan Terakhir"], errors="coerce")
    out = out.sort_values(["Tanggal Perdagangan Terakhir", "Kode Saham"], kind="mergesort")
    out["Tanggal Perdagangan Terakhir"] = out["Tanggal Perdagangan Terakhir"].dt.date.astype(str)
    return out


st.set_page_config(page_title="Stock Indicators", layout="wide")
st.title("Streamlit Stock Indicators App")
st.caption("Upload Excel harian, validasi schema 28 kolom, hitung indikator, dan download output.")

st.divider()
st.subheader("Download dari Database (tanpa upload)")

service_db = build_sheets_service(st.secrets["google_service_account"])
raw_id_db = st.secrets["SPREADSHEET_RAW_ID"]
out_a_id_db = st.secrets["SPREADSHEET_OUTPUT_A_ID"]
out_b_id_db = st.secrets["SPREADSHEET_OUTPUT_B_ID"]

if "db_loaded" not in st.session_state:
    st.session_state.db_loaded = False

if st.button("Load DB", key="btn_load_db"):
    raw_db = _read_sheet_as_df(service_db, raw_id_db, "RAW")
    out_a_db = _read_sheet_as_df(service_db, out_a_id_db, "OUTPUT_A")
    out_b_db = _read_sheet_as_df(service_db, out_b_id_db, "OUTPUT_B")

    st.session_state.raw_db = raw_db
    st.session_state.out_a_db = out_a_db
    st.session_state.out_b_db = out_b_db
    st.session_state.db_loaded = True

    st.success(f"Loaded DB: RAW={len(raw_db)} rows, OUT_A={len(out_a_db)} rows, OUT_B={len(out_b_db)} rows")

if st.session_state.db_loaded:
    raw_db = st.session_state.raw_db.copy()
    out_a_db = st.session_state.out_a_db.copy()
    out_b_db = st.session_state.out_b_db.copy()

    if raw_db.empty:
        st.warning("RAW masih kosong, tidak ada data untuk didownload.")
        st.stop()

    # Range berdasarkan RAW (source-of-truth tanggal)
    raw_db["Tanggal Perdagangan Terakhir"] = pd.to_datetime(
        raw_db["Tanggal Perdagangan Terakhir"], errors="coerce"
    ).dt.date

    min_d = raw_db["Tanggal Perdagangan Terakhir"].min()
    max_d = raw_db["Tanggal Perdagangan Terakhir"].max()

    picked = st.date_input(
        "Pilih range tanggal (berdasarkan RAW)",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d,
        key="db_range_picker",
    )

    # lanjutkan blok start_date,end_date dan merge di sini...


    if isinstance(picked, tuple) and len(picked) == 2:
        start_date, end_date = picked

        # Filter RAW by range
        raw_range = raw_db[
            (raw_db["Tanggal Perdagangan Terakhir"] >= start_date) &
            (raw_db["Tanggal Perdagangan Terakhir"] <= end_date)
        ].copy()

        # Kunci join harus string (konsisten dengan sheet output)
        raw_range["Tanggal Perdagangan Terakhir"] = raw_range["Tanggal Perdagangan Terakhir"].astype(str)

        # Pastikan schema OUTPUT_A/B sesuai header source-of-truth
        out_a_cols = KEY_COLS + OUT_A_INDICATORS
        out_b_cols = KEY_COLS + OUT_B_INDICATORS
        out_a_db = out_a_db.reindex(columns=out_a_cols)
        out_b_db = out_b_db.reindex(columns=out_b_cols)

        # Buat tanggal output A/B jadi string date, lalu filter range
        out_a_db["Tanggal Perdagangan Terakhir"] = pd.to_datetime(
            out_a_db["Tanggal Perdagangan Terakhir"], errors="coerce"
        ).dt.date.astype(str)

        out_b_db["Tanggal Perdagangan Terakhir"] = pd.to_datetime(
            out_b_db["Tanggal Perdagangan Terakhir"], errors="coerce"
        ).dt.date.astype(str)

        start_s = str(start_date)
        end_s = str(end_date)

        out_a_range = out_a_db[
            (out_a_db["Tanggal Perdagangan Terakhir"] >= start_s) &
            (out_a_db["Tanggal Perdagangan Terakhir"] <= end_s)
        ].copy()

        out_b_range = out_b_db[
            (out_b_db["Tanggal Perdagangan Terakhir"] >= start_s) &
            (out_b_db["Tanggal Perdagangan Terakhir"] <= end_s)
        ].copy()

        # Robust: jangan join pakai Nama Perusahaan (ambil dari RAW saja)
        out_a_range = out_a_range.drop(columns=["Nama Perusahaan"], errors="ignore")
        out_b_range = out_b_range.drop(columns=["Nama Perusahaan"], errors="ignore")

        # (Opsional safety) hilangkan duplikat key kalau ada edit manual di Sheets
        raw_range = raw_range.drop_duplicates(subset=KEY2, keep="last")
        out_a_range = out_a_range.drop_duplicates(subset=KEY2, keep="last")
        out_b_range = out_b_range.drop_duplicates(subset=KEY2, keep="last")

        merged = (
            raw_range[CANON_COLS_28]
            .merge(out_a_range, how="left", on=KEY2)
            .merge(out_b_range, how="left", on=KEY2)
        )

        # Sort final
        merged["Tanggal Perdagangan Terakhir"] = pd.to_datetime(merged["Tanggal Perdagangan Terakhir"], errors="coerce")
        merged = merged.sort_values(["Tanggal Perdagangan Terakhir", "Kode Saham"], kind="mergesort")
        merged["Tanggal Perdagangan Terakhir"] = merged["Tanggal Perdagangan Terakhir"].dt.date.astype(str)

        xbytes_db = to_excel_bytes(merged, sheet_name="OUTPUT")
        fname = f"RekapSahamIndikator-{start_date:%d%m%y}-{end_date:%d%m%y}.xlsx"

        st.download_button(
            "Download OUTPUT (DB by range)",
            data=xbytes_db,
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_db_range",
        )
    else:
        st.info("Pilih start dan end date dulu.")


uploaded = st.file_uploader("Upload file Excel Ringkasan Saham", type=["xlsx"])

if "validated_df" not in st.session_state:
    st.session_state.validated_df = None

col1, col2 = st.columns(2)

with col1:
    do_validate = st.button("Validate")

with col2:
    do_process = st.button("Process + Upsert + Download")

if do_validate:
    if uploaded is None:
        st.error("Silakan upload file Excel dulu.")
    else:
        try:
            df0 = read_input_excel(uploaded)
            df1 = normalize_and_validate_columns(df0)
            df2 = parse_and_cast(df1)
            # sorting wajib: tanggal lalu emiten
            df2 = df2.sort_values(["Tanggal Perdagangan Terakhir", "Kode Saham"], kind="mergesort")
            st.session_state.validated_df = df2
            st.success("Validasi berhasil.")
            st.dataframe(df2.head(20), use_container_width=True)
        except Exception as e:
            st.session_state.validated_df = None
            st.error(f"Validasi gagal: {e}")

if do_process:
    if st.session_state.validated_df is None:
        st.error("Harus Validate dulu sampai berhasil (no write before validation).")
    else:
        try:
            # Build Sheets service from secrets
            service = build_sheets_service(st.secrets["google_service_account"])

            raw_id = st.secrets["SPREADSHEET_RAW_ID"]
            out_a_id = st.secrets["SPREADSHEET_OUTPUT_A_ID"]
            out_b_id = st.secrets["SPREADSHEET_OUTPUT_B_ID"]

            df_today_raw = st.session_state.validated_df.copy()
            df_today_raw["Tanggal Perdagangan Terakhir"] = df_today_raw["Tanggal Perdagangan Terakhir"].astype(str)

            # --- RAW: read existing -> upsert -> write back
            existing_raw = _read_sheet_as_df(service, raw_id, "RAW")
            raw_merged = _upsert_by_key(existing_raw, df_today_raw[CANON_COLS_28], ["Tanggal Perdagangan Terakhir", "Kode Saham"])
            raw_merged = _sort_date_emiten(raw_merged)
            raw_merged = filter_keep_last_trading_days(raw_merged, "Tanggal Perdagangan Terakhir", keep_days=280)
            raw_merged = _sort_date_emiten(raw_merged)

            show_debug = st.checkbox("Show debug", value=False)
            if show_debug:
                st.write(
                    "DEBUG: RAW last 15 unique dates:",
                    pd.Series(raw_merged["Tanggal Perdagangan Terakhir"].unique()).tail(15).tolist()
                )
            
            write_values(service, raw_id, "RAW!A1", _df_to_values(raw_merged))

            # --- Build historis untuk indikator dari RAW (pakai yang sudah tersimpan)
            # Convert back to numeric/date
            raw_hist = raw_merged.copy()
            raw_hist = normalize_and_validate_columns(raw_hist)
            raw_hist = parse_and_cast(raw_hist)

            # buat input indikator: price 0 -> NaN
            raw_for_ind = make_indicator_inputs(raw_hist)

            # hitung indikator (per emiten, urutan tanggal dijaga di engine)
            df_ind = compute_indicators(raw_for_ind)

            # ambil tanggal hari ini saja (sesuai file input)
            today_dates = pd.to_datetime(st.session_state.validated_df["Tanggal Perdagangan Terakhir"]).dt.date.unique()
            df_ind["Tanggal Perdagangan Terakhir"] = pd.to_datetime(df_ind["Tanggal Perdagangan Terakhir"]).dt.date
            df_today_ind = df_ind[df_ind["Tanggal Perdagangan Terakhir"].isin(today_dates)].copy()

            # persiapan OUTPUT A/B
            out_a_cols = KEY_COLS + OUT_A_INDICATORS
            out_b_cols = KEY_COLS + OUT_B_INDICATORS
            
            # key harus selalu dari input hari ini (agar output tidak kosong)
            df_today_key = st.session_state.validated_df[KEY_COLS].copy()
            df_today_key["Tanggal Perdagangan Terakhir"] = df_today_key["Tanggal Perdagangan Terakhir"].astype(str)
            
            # indikator hari ini (keyed)
            df_today_ind2 = df_today_ind.copy()
            df_today_ind2["Tanggal Perdagangan Terakhir"] = pd.to_datetime(df_today_ind2["Tanggal Perdagangan Terakhir"]).dt.date.astype(str)
            
            ind_a = df_today_ind2.reindex(columns=["Tanggal Perdagangan Terakhir", "Kode Saham"] + OUT_A_INDICATORS)
            ind_b = df_today_ind2.reindex(columns=["Tanggal Perdagangan Terakhir", "Kode Saham"] + OUT_B_INDICATORS)
            
            out_a = df_today_key.merge(ind_a, how="left", on=["Tanggal Perdagangan Terakhir", "Kode Saham"]).reindex(columns=out_a_cols)
            out_b = df_today_key.merge(ind_b, how="left", on=["Tanggal Perdagangan Terakhir", "Kode Saham"]).reindex(columns=out_b_cols)

            # sort wajib untuk output: tanggal lalu emiten
            out_a = _sort_date_emiten(out_a)
            out_b = _sort_date_emiten(out_b)

            # upsert ke OUTPUT_A
            existing_a = _read_sheet_as_df(service, out_a_id, "OUTPUT_A")
            merged_a = _upsert_by_key(existing_a, out_a, ["Tanggal Perdagangan Terakhir", "Kode Saham"])
            
            # sort -> prune 280 hari -> sort lagi (biar rapi)
            merged_a = _sort_date_emiten(merged_a)
            merged_a = filter_keep_last_trading_days(
                merged_a,
                date_col="Tanggal Perdagangan Terakhir",
                keep_days=280,
            )
            merged_a = _sort_date_emiten(merged_a)
            
            write_values(service, out_a_id, "OUTPUT_A!A1", _df_to_values(merged_a))

            # upsert ke OUTPUT_B
            existing_b = _read_sheet_as_df(service, out_b_id, "OUTPUT_B")
            merged_b = _upsert_by_key(existing_b, out_b, ["Tanggal Perdagangan Terakhir", "Kode Saham"])
            
            # sort -> prune 280 hari -> sort lagi
            merged_b = _sort_date_emiten(merged_b)
            merged_b = filter_keep_last_trading_days(
                merged_b,
                date_col="Tanggal Perdagangan Terakhir",
                keep_days=280,
            )
            merged_b = _sort_date_emiten(merged_b)
            
            write_values(service, out_b_id, "OUTPUT_B!A1", _df_to_values(merged_b))

            # --- generate 1 file excel download: 28 kolom input + semua indikator untuk hari ini
            df_today_input = st.session_state.validated_df.copy()
            df_today_input["Tanggal Perdagangan Terakhir"] = df_today_input["Tanggal Perdagangan Terakhir"].astype(str)
            
            # build indikator table dengan key, lalu merge by key (bukan concat)
            ind_cols = ["Tanggal Perdagangan Terakhir", "Kode Saham"] + OUT_A_INDICATORS + OUT_B_INDICATORS
            
            df_today_ind2 = df_today_ind.copy()
            df_today_ind2["Tanggal Perdagangan Terakhir"] = pd.to_datetime(df_today_ind2["Tanggal Perdagangan Terakhir"]).dt.date.astype(str)
            
            ind_table = df_today_ind2.reindex(columns=ind_cols).copy()
            
            out_download = df_today_input[CANON_COLS_28].merge(
                ind_table,
                how="left",
                on=["Tanggal Perdagangan Terakhir", "Kode Saham"],
            )

            # final sort: tanggal, emiten
            out_download["Tanggal Perdagangan Terakhir"] = pd.to_datetime(out_download["Tanggal Perdagangan Terakhir"], errors="coerce")
            out_download = out_download.sort_values(["Tanggal Perdagangan Terakhir", "Kode Saham"], kind="mergesort")
            out_download["Tanggal Perdagangan Terakhir"] = out_download["Tanggal Perdagangan Terakhir"].dt.date.astype(str)

            xbytes = to_excel_bytes(out_download, sheet_name="OUTPUT")
            st.success("Selesai. Silakan download file output.")

            # Ambil tanggal dari file upload (ambil yang paling baru)
            dmax = pd.to_datetime(st.session_state.validated_df["Tanggal Perdagangan Terakhir"]).max()
            ddmmyy = pd.to_datetime(dmax).strftime("%d%m%y")

            st.download_button(
            "Download OUTPUT (.xlsx)",
            data=xbytes,
            file_name=f"RekapSahamIndikator-{ddmmyy}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"Process gagal: {e}")
