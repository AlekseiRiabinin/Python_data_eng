from typing import TypeVar, Iterator

T = TypeVar('T')


def batched(iterable: Iterator[T], batch_size: int) -> Iterator[list[T]]:
    """Batch data from an iterable into fixed-size chunks."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


# Usage:
# for batch in batched(log_lines_generator(), 1000):
#     insert_batch_into_db(batch)
