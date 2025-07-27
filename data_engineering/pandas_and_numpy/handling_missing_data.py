import pandas as pd
import numpy as np


def handle_missing_values(
    df: pd.DataFrame,
    strategy: str = 'median',
    threshold: float = 0.3,
    drop_cols: bool = True
) -> pd.DataFrame:
    """Handle missing values in a DataFrame based on specified strategy."""
    if drop_cols:
        missing_ratio = df.isnull().mean()
        cols_to_drop = missing_ratio[missing_ratio > threshold].index
        df = df.drop(columns=cols_to_drop)
    
    if strategy == 'median':
        df = df.fillna(df.median(numeric_only=True))
    elif strategy == 'mean':
        df = df.fillna(df.mean(numeric_only=True))
    elif strategy == 'mode':
        df = df.fillna(df.mode().iloc[0])
    elif strategy == 'drop':
        df = df.dropna()
    
    return df


# Example usage:
data_with_nulls = pd.DataFrame({
    'A': [1, 2, np.nan, 4],
    'B': [np.nan, 2, 3, 4],
    'C': [1, np.nan, np.nan, np.nan]
})

cleaned = handle_missing_values(data_with_nulls, strategy='median', threshold=0.5)
print(cleaned)
