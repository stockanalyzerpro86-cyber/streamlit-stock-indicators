import numpy as np
import pandas as pd


def _wilder_rma(series: pd.Series, period: int) -> pd.Series:
    # Wilder's RMA ~ EMA(alpha=1/period) dengan adjust=False
    return series.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def compute_indicators(df_hist: pd.DataFrame) -> pd.DataFrame:
    df = df_hist.copy()
    df["Tanggal Perdagangan Terakhir"] = pd.to_datetime(df["Tanggal Perdagangan Terakhir"])

    # Wajib: urut per emiten lalu tanggal
    df = df.sort_values(["Kode Saham", "Tanggal Perdagangan Terakhir"], kind="mergesort")
    g = df.groupby("Kode Saham", sort=False)

    close = df["Penutupan"]
    high = df["Tertinggi"]
    low = df["Terendah"]
    vol = df["Volume"]

    prev_close = g["Penutupan"].shift(1)

    # =========================
    # 1) Return-based basics
    # =========================
    delta = g["Penutupan"].diff()
    df["Gain Harian"] = delta.clip(lower=0)
    df["Loss Harian"] = (-delta.clip(upper=0))

    df["AvgGain-9"] = g["Gain Harian"].transform(lambda s: _wilder_rma(s, 9))
    df["AvgLoss-9"] = g["Loss Harian"].transform(lambda s: _wilder_rma(s, 9))

    rs = df["AvgGain-9"] / df["AvgLoss-9"]
    df["RSI-9"] = 100 - (100 / (1 + rs))

    # =========================
    # 2) Moving averages
    # =========================
    df["SMA-5"] = g["Penutupan"].transform(lambda s: s.rolling(5, min_periods=5).mean())

    df["EMA-5"] = g["Penutupan"].transform(lambda s: s.ewm(span=5, adjust=False, min_periods=5).mean())
    df["EMA-12"] = g["Penutupan"].transform(lambda s: s.ewm(span=12, adjust=False, min_periods=12).mean())
    df["EMA-20"] = g["Penutupan"].transform(lambda s: s.ewm(span=20, adjust=False, min_periods=20).mean())

    df["MA-20"] = g["Penutupan"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["MA-50"] = g["Penutupan"].transform(lambda s: s.rolling(50, min_periods=50).mean())

    df["Std Dev 20D"] = g["Penutupan"].transform(lambda s: s.rolling(20, min_periods=20).std(ddof=0))
    df["BB Middle"] = df["MA-20"]
    df["BB Upper"] = df["BB Middle"] + 2 * df["Std Dev 20D"]
    df["BB Lower"] = df["BB Middle"] - 2 * df["Std Dev 20D"]

    df["Vol 20D Avg"] = g["Volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())

    # Week Highs (hari perdagangan): 4/8/13/52 weeks = 20/40/65/260
    df["4-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(20, min_periods=20).max())
    df["8-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(40, min_periods=40).max())
    df["13-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(65, min_periods=65).max())
    df["52-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(260, min_periods=260).max())

    # =========================
    # 3) ATR (Wilder)
    # =========================
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["ATR-9"] = g["TR"].transform(lambda s: _wilder_rma(s, 9))

    df["Range Ratio (Daily Range / ATR)"] = (high - low) / df["ATR-9"]
    df["Close Position % (0-100%)"] = ((close - low) / (high - low)) * 100

    # =========================
    # 4) Stochastic
    # =========================
    df["Min Low-9"] = g["Terendah"].transform(lambda s: s.rolling(9, min_periods=9).min())
    df["Max High-9"] = g["Tertinggi"].transform(lambda s: s.rolling(9, min_periods=9).max())
    df["%K Stoch-9"] = ((close - df["Min Low-9"]) / (df["Max High-9"] - df["Min Low-9"])) * 100
    df["%D Stoch-3"] = g["%K Stoch-9"].transform(lambda s: s.rolling(3, min_periods=3).mean())

    # =========================
    # 5) Typical Price family
    # =========================
    tp = (high + low + close) / 3

    # VWAP-5: sum(TP*Vol)/sum(Vol) over 5 days, strict
    pv = tp * vol
    pv_sum5 = g.apply(lambda x: (x["Tertinggi"] + x["Terendah"] + x["Penutupan"]) / 3 * x["Volume"]).reset_index(level=0, drop=True)
    # gunakan rolling via groupby transform agar index stabil
    pv_sum5 = g.apply(lambda x: (((x["Tertinggi"] + x["Terendah"] + x["Penutupan"]) / 3) * x["Volume"]).rolling(5, min_periods=5).sum()).reset_index(level=0, drop=True)
    v_sum5 = g["Volume"].transform(lambda s: s.rolling(5, min_periods=5).sum())
    df["VWAP-5"] = pv_sum5 / v_sum5

    # =========================
    # 6) OBV (seed = volume hari pertama)
    # =========================
    # sign per bar: +1 jika close > prev_close, -1 jika close < prev_close, 0 jika sama
    sign = np.where(close > prev_close, 1, np.where(close < prev_close, -1, 0)).astype(float)
    signed_vol = sign * vol

    # seed OBV = volume hari pertama (sesuai keputusan)
    # implement: untuk bar pertama per emiten, set signed_vol = volume (bukan 0/NaN)
    first_mask = g.cumcount() == 0
    signed_vol = pd.Series(signed_vol, index=df.index)
    signed_vol.loc[first_mask] = vol.loc[first_mask]

    df["OBV"] = signed_vol.groupby(df["Kode Saham"], sort=False).cumsum()

    # =========================
    # 7) ADL / CMF
    # =========================
    denom = (high - low)
    mfm = (((close - low) - (high - close)) / denom).replace([np.inf, -np.inf], np.nan)
    df["MFM"] = mfm

    mfv = mfm * vol

    # seed ADL = MFV hari pertama (sesuai keputusan)
    mfv2 = mfv.copy()
    mfv2.loc[first_mask] = mfv.loc[first_mask]
    df["ADL (Accumulation/Distribution Line)"] = mfv2.groupby(df["Kode Saham"], sort=False).cumsum()

    # CMF-9 = sum(MFV,9)/sum(Vol,9)
    mfv_sum9 = g["MFM"].transform(lambda s: s) * vol
    mfv_sum9 = mfv_sum9.groupby(df["Kode Saham"], sort=False).transform(lambda s: s.rolling(9, min_periods=9).sum())
    v_sum9 = g["Volume"].transform(lambda s: s.rolling(9, min_periods=9).sum())
    df["CMF-9"] = mfv_sum9 / v_sum9

    # =========================
    # 8) Force Index
    # =========================
    # raw = (Close - prevClose) * Volume (bar pertama per emiten -> NaN)
    df["Force Index (Raw)"] = delta * vol
    df["Force Index EMA-13"] = g["Force Index (Raw)"].transform(
        lambda s: s.ewm(span=13, adjust=False, min_periods=13).mean()
    )

    # =========================
    # 9) VPT (seed = 0)
    # =========================
    # per bar: Volume * pct_change(Close)
    pct = g["Penutupan"].pct_change()
    vpt_step = vol * pct
    vpt_step = vpt_step.replace([np.inf, -np.inf], np.nan)

    vpt_step2 = vpt_step.copy()
    vpt_step2.loc[first_mask] = 0.0
    df["VPT (Volume Price Trend)"] = vpt_step2.groupby(df["Kode Saham"], sort=False).cumsum()

    # =========================
    # 10) MFI-14 (pakai TP, sesuai definisi)
    # =========================
    # Money Flow = TP * Volume
    mf = tp * vol
    tp_delta = g.apply(lambda x: ((x["Tertinggi"] + x["Terendah"] + x["Penutupan"]) / 3).diff()).reset_index(level=0, drop=True)

    pos_mf = mf.where(tp_delta > 0, 0.0)
    neg_mf = mf.where(tp_delta < 0, 0.0).abs()

    pos_sum14 = pos_mf.groupby(df["Kode Saham"], sort=False).transform(lambda s: s.rolling(14, min_periods=14).sum())
    neg_sum14 = neg_mf.groupby(df["Kode Saham"], sort=False).transform(lambda s: s.rolling(14, min_periods=14).sum())

    money_ratio = pos_sum14 / neg_sum14
    df["MFI-14 (Money Flow Index)"] = 100 - (100 / (1 + money_ratio))

    # =========================
    # 11) Keltner
    # =========================
    df["Keltner Upper"] = df["EMA-20"] + 2 * df["ATR-9"]
    df["Keltner Lower"] = df["EMA-20"] - 2 * df["ATR-9"]

    return df
