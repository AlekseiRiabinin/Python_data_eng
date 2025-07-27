import pandas as pd


def merge_dataframes(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    merge_keys: str | list[str],
    merge_type: str = 'inner'
) -> pd.DataFrame:
    """Merge two DataFrames with proper type checking and validation."""
    if isinstance(merge_keys, str):
        merge_keys = [merge_keys]
    
    for key in merge_keys:
        if key not in left_df.columns or key not in right_df.columns:
            raise ValueError(f"Merge key '{key}' not found in both DataFrames")
    
    merged_df = pd.merge(
        left_df, 
        right_df, 
        on=merge_keys, 
        how=merge_type,
        validate='one_to_one'  # Ensures no unexpected duplicates
    )
    
    return merged_df


# Example usage:
customers = pd.DataFrame({
    'customer_id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie']
})

orders = pd.DataFrame({
    'order_id': [101, 102, 103],
    'customer_id': [1, 2, 4],
    'amount': [100, 200, 300]
})

try:
    customer_orders = merge_dataframes(
        customers, 
        orders, 
        merge_keys='customer_id',
        merge_type='left'
    )
    print(customer_orders)
except ValueError as e:
    print(f"Merge error: {e}")
