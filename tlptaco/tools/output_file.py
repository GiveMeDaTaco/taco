import pandas as pd
from tlptaco.logging.logging import CustomLogger

def strip_whitespace_from_dataframe(df: pd.DataFrame, logger: CustomLogger | None = None) -> pd.DataFrame:
    """
    Sometimes your output file will contain a bunch of blank spaces. This is usually when you use CAST in your query.
    This function will strip away all the extra blank spaces from your columns that are objects.
    NOTE: make sure your columns you want to strip are already "object" types.

    :param df: DataFrame to modify
    :param logger: logger to use
    :return pd.DataFrame: modified DataFrame
    """
    columns_stripped = []
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
            columns_stripped.append(col)
    if logger is not None:
        logger.info(f'Stripped white space from {", ".join(columns_stripped)}')
    return df