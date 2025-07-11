"""
Input file loader utilities.
"""
import pandas as pd

def read_csv(path: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, **kwargs)

def read_excel(path: str, **kwargs) -> pd.DataFrame:
    return pd.read_excel(path, **kwargs)

def read_parquet(path: str, **kwargs) -> pd.DataFrame:
    return pd.read_parquet(path, **kwargs)