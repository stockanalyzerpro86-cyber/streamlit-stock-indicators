import pandas as pd


def compute_cutoff_trading_day(dates: pd.Series, keep_days: int = 280):
    # dates: datetime-like
    uniq = pd.Series(pd.to_datetime(dates).dropna().unique()).sort_values()
    if len(uniq) <= keep_days:
        return None
    return uniq.iloc[-keep_days]


def filter_keep_last_trading_days(df: pd.DataFrame, date_col: str, keep_days: int = 280) -> pd.DataFrame:
    cutoff = compute_cutoff_trading_day(df[date_col], keep_days=keep_days)
    if cutoff is None:
        return df
    d = pd.to_datetime(df[date_col])
    return df.loc[d >= cutoff].copy()
