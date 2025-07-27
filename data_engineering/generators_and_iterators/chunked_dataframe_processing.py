import pandas as pd
from typing import Iterator


def chunked_dataframe_reader(
    file_path: str,
    chunk_size: int = 10_000
) -> Iterator[pd.DataFrame]:
    """Generator that yields DataFrame chunks for memory-efficient processing."""
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        yield chunk


# Usage:
# for df_chunk in chunked_dataframe_reader('large_data.csv'):
#     transformed = preprocess_chunk(df_chunk)
#     load_to_db(transformed)
