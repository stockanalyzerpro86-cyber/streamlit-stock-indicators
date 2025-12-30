import numpy as np
import pandas as pd


def _wilder_rma(series: pd.Series, period: int) -> pd.Series:
    # Wilder's RMA (smoothed moving average) ~ EMA with alpha=1/period, adjust=False
    return series.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def compute_indicators(df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    Input:
      - df_hist sudah hasil make_indicator_inputs(): harga 0 -> NaN
      - kolom minimal: Kode Saham, Tanggal Perdagangan Terakhir, Tertinggi, Terendah, Penutupan, Volume, Nilai
    Output:
      - df dengan semua kolom indikator (sesuai daftar) ditambahkan.
    """
    df = df_hist.copy()
    df["Tanggal Perdagangan Terakhir"] = pd.to_datetime(df["Tanggal Perdagangan Terakhir"])

    # Sort untuk kalkulasi: emiten dulu, lalu tanggal (anti cross-emiten)
    df = df.sort_values(["Kode Saham", "Tanggal Perdagangan Terakhir"], kind="mergesort")

    g = df.groupby("Kode Saham", sort=False)

    close = df["Penutupan"]
    high = df["Tertinggi"]
    low = df["Terendah"]
    vol = df["Volume"]

    prev_close = g["Penutupan"].shift(1)

    # Gain/Loss harian (berdasarkan perubahan close)
    delta = g["Penutupan"].diff()
    df["Gain Harian"] = delta.clip(lower=0)
    df["Loss Harian"] = (-delta.clip(upper=0))

    # AvgGain-9 / AvgLoss-9 (Wilder)
    df["AvgGain-9"] = g["Gain Harian"].transform(lambda s: _wilder_rma(s, 9))
    df["AvgLoss-9"] = g["Loss Harian"].transform(lambda s: _wilder_rma(s, 9))

    # RSI-9 (Wilder)
    rs = df["AvgGain-9"] / df["AvgLoss-9"]
    df["RSI-9"] = 100 - (100 / (1 + rs))

    # SMA-5
    df["SMA-5"] = g["Penutupan"].transform(lambda s: s.rolling(5, min_periods=5).mean())

    # EMA-5 / EMA-12 / EMA-20 (standard EMA)
    df["EMA-5"] = g["Penutupan"].transform(lambda s: s.ewm(span=5, adjust=False, min_periods=5).mean())
    df["EMA-12"] = g["Penutupan"].transform(lambda s: s.ewm(span=12, adjust=False, min_periods=12).mean())
    df["EMA-20"] = g["Penutupan"].transform(lambda s: s.ewm(span=20, adjust=False, min_periods=20).mean())

    # MA-20 / MA-50
    df["MA-20"] = g["Penutupan"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["MA-50"] = g["Penutupan"].transform(lambda s: s.rolling(50, min_periods=50).mean())

    # Std Dev 20D + Bollinger Bands (20D)
    df["Std Dev 20D"] = g["Penutupan"].transform(lambda s: s.rolling(20, min_periods=20).std(ddof=0))
    df["BB Middle"] = df["MA-20"]
    df["BB Upper"] = df["BB Middle"] + 2 * df["Std Dev 20D"]
    df["BB Lower"] = df["BB Middle"] - 2 * df["Std Dev 20D"]

    # Vol 20D Avg
    df["Vol 20D Avg"] = g["Volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())

    # Week Highs (trading days): 4/8/13/52 weeks = 20/40/65/260
    df["4-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(20, min_periods=20).max())
    df["8-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(40, min_periods=40).max())
    df["13-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(65, min_periods=65).max())
    df["52-Week High"] = g["Tertinggi"].transform(lambda s: s.rolling(260, min_periods=260).max())

    # True Range (TR)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # ATR-9 (Wilder)
    df["ATR-9"] = g["TR"].transform(lambda s: _wilder_rma(s, 9))

    # Range Ratio (Daily Range / ATR)
    df["Range Ratio (Daily Range / ATR)"] = (high - low) / df["ATR-9"]

    # Close Position % (0-100%)
    df["Close Position % (0-100%)"] = ((close - low) / (high - low)) * 100

    # Stochastic %K 9: (Close - MinLow9) / (MaxHigh9 - MinLow9) * 100
    df["Min Low-9"] = g["Terendah"].transform(lambda s: s.rolling(9, min_periods=9).min())
    df["Max High-9"] = g["Tertinggi"].transform(lambda s: s.rolling(9, min_periods=9).max())
    df["%K Stoch-9"] = ((close - df["Min Low-9"]) / (df["Max High-9"] - df["Min Low-9"])) * 100
    df["%D Stoch-3"] = g["%K Stoch-9"].transform(lambda s: s.rolling(3, min_periods=3).mean())

    # VWAP-5 (pakai typical price)
    typical = (high + low + close) / 3
    pv = typical * vol
    df["VWAP-5"] = g.apply(
        lambda x: ( ( ( (x["Tertinggi"] + x["Terendah"] + x["Penutupan"]) / 3 ) * x["Volume"] )
                  .rolling(5, min_periods=5).sum()
                  / x["Volume"].rolling(5, min_periods=5).sum()
        )
    ).reset_index(level=0, drop=True)

    # OBV
    prev_close2 = g["Penutupan"].shift(1)
    sign = np.where(close > prev_close2, 1, np.where(close < prev_close2, -1, 0))
    df["OBV"] = g.apply(lambda x: (np.where(x["Penutupan"] > x["Penutupan"].shift(1), x["Volume"],
                                           np.where(x["Penutupan"] < x["Penutupan"].shift(1), -x["Volume"], 0)))
                        .cumsum()).reset_index(level=0, drop=True)

    # MFM + CMF-9
    denom = (high - low)
    mfm = (((close - low) - (high - close)) / denom).replace([np.inf, -np.inf], np.nan)
    df["MFM"] = mfm
    mfv = mfm * vol
    df["CMF-9"] = g.apply(lambda x: ( (x["MFM"] * x["Volume"]).rolling(9, min_periods=9).sum()
                                    / x["Volume"].rolling(9, min_periods=9).sum()
                                   )).reset_index(level=0, drop=True)

    # Force Index (Raw) + EMA-13
    df["Force Index (Raw)"] = delta * vol
    df["Force Index EMA-13"] = g["Force Index (Raw)"].transform(
        lambda s: s.ewm(span=13, adjust=False, min_periods=13).mean()
    )

    # ADL (Accumulation/Distribution Line) + VPT
    df["ADL (Accumulation/Distribution Line)"] = g.apply(
        lambda x: (x["MFM"] * x["Volume"]).fillna(0).cumsum()
    ).reset_index(level=0, drop=True)

    df["VPT (Volume Price Trend)"] = g.apply(
        lambda x: (x["Volume"] * (x["Penutupan"].pct_change())).fillna(0).cumsum()
    ).reset_index(level=0, drop=True)

    # MFI-14 (Money Flow Index) pakai typical price & Volume
    raw_mf = typical * vol
    tp_delta = g.apply(lambda x: x["Penutupan"].diff()).reset_index(level=0, drop=True)
    pos_mf = np.where(tp_delta > 0, raw_mf, 0.0)
    neg_mf = np.where(tp_delta < 0, raw_mf, 0.0)
    df["MFI-14 (Money Flow Index)"] = g.apply(
        lambda x: (
            100 - (100 / (1 + (
                pd.Series(pos_mf, index=df.index).loc[x.index].rolling(14, min_periods=14).sum()
                / pd.Series(neg_mf, index=df.index).loc[x.index].rolling(14, min_periods=14).sum()
            )))
        )
    ).reset_index(level=0, drop=True)

    # Keltner Upper/Lower: EMA-20 ± 2*ATR-9
    df["Keltner Upper"] = df["EMA-20"] + 2 * df["ATR-9"]
    df["Keltner Lower"] = df["EMA-20"] - 2 * df["ATR-9"]

    return df
