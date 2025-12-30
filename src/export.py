import io
import pandas as pd


def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "OUTPUT") -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buf.getvalue()
