import pandas as pd


def process_large_data(
    file_path: str,
    chunksize: int = 10000,
    filter_condition: str = None
) -> pd.DataFrame:
    """Process large datasets efficiently using chunking."""
    chunks = []
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        if filter_condition:
            chunk = chunk.query(filter_condition)
        
        for col in chunk.select_dtypes(include='object'):
            if chunk[col].nunique() / len(chunk) < 0.5:
                chunk[col] = chunk[col].astype('category')
        
        chunks.append(chunk)
    
    return pd.concat(chunks, ignore_index=True)

# processed_data = process_large_data('large_dataset.csv', filter_condition='amount > 100')
