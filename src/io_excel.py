import pandas as pd


def read_input_excel(uploaded_file) -> pd.DataFrame:
    # Input Anda selalu 1 sheet, jadi cukup read sheet pertama
    df = pd.read_excel(uploaded_file, engine="openpyxl")
    return df
