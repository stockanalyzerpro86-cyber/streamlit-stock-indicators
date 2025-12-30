import streamlit as st


st.set_page_config(page_title="Stock Indicators", layout="wide")

st.title("Streamlit Stock Indicators App")
st.caption("Upload Excel harian, validasi schema 28 kolom, hitung indikator, dan download output.")

st.info(
    "Tahap 1 (GitHub): skeleton app sudah dibuat. "
    "Tahap berikutnya: koneksi Google Sheets + engine indikator."
)

uploaded = st.file_uploader("Upload file Excel Ringkasan Saham", type=["xlsx"])

col1, col2 = st.columns(2)

with col1:
    st.button("Validate (coming soon)", disabled=True)

with col2:
    st.button("Process + Upsert + Download (coming soon)", disabled=True)

if uploaded is not None:
    st.write("File terupload:", uploaded.name)
    st.warning("Fitur baca/validasi akan diaktifkan setelah Tahap 2 (Google Sheets) selesai.")
