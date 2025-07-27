import pandas as pd
from pandas import DataFrame


def clean_and_transform_data(
    raw_data: DataFrame, 
    column_mapping: dict[str, str],
    date_columns: list[str]
) -> DataFrame:
    """Clean and transform raw data into a structured format."""
    df = raw_data.rename(columns=column_mapping)
    
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    string_cols = df.select_dtypes(include='object').columns
    df[string_cols] = df[string_cols].apply(lambda x: x.str.strip().str.lower())
    
    return df


# Example usage:
raw_data = pd.DataFrame({
    'Cust_ID': [1, 2, 3],
    'Join Date': ['2023-01-01', '2023-02-15', '2023-03-30'],
    'AMOUNT': [100.50, 200.75, 300.25]
})

cleaned_data = clean_and_transform_data(
    raw_data,
    column_mapping={'Cust_ID': 'customer_id', 'Join Date': 'join_date', 'AMOUNT': 'amount'},
    date_columns=['join_date']
)

print(cleaned_data.dtypes)
