from collections.abc import Generator


class GeneratorOps:
    """Demonstrates generator expressions for memory efficiency."""

    @staticmethod
    def square_gen(n: int) -> Generator[int, None, None]:
        """Generator yielding squares (no list created)."""
        return (x**2 for x in range(n))

    @staticmethod
    def file_word_counter(filepath: str) -> Generator[int, None, None]:
        """Count words per line lazily."""
        return (len(line.split()) for line in open(filepath) if line.strip())

    @staticmethod
    def chained_operations(data: list[int]) -> Generator[int, None, None]:
        """Chain generator expressions."""
        squared = (x**2 for x in data)
        return (x for x in squared if x % 2 == 0)

    @staticmethod
    def sum_until_limit(limit: int) -> Generator[int, None, str]:
        """Yields numbers until sum exceeds limit, returns status."""
        total = 0
        for i in range(1, 100):
            total += i
            yield i
            if total >= limit:
                return f"Reached limit {limit}"
        return "Completed all iterations"

    @staticmethod
    def data_processor() -> Generator[float, list[float], dict[str, float]]:
        """Processes batches of data, yields progress, returns stats."""
        total = 0.0
        count = 0
        while True:
            batch = yield (total / count if count > 0 else 0.0)  # Yield avg
            if batch is None:  # Sentinel for completion
                break
            total += sum(batch)
            count += len(batch)
        return {"total": total, "average": total / count}


# Usage
gen = GeneratorOps()

print(list(gen.square_gen(5)))  # [0, 1, 4, 9, 16] (but consumes generator)

# File processing example (memory efficient for large files)
# word_counts = GeneratorOps.file_word_counter('large_log.txt')

for num in gen.sum_until_limit(10):
    print(num, end=" ")  # 1 2 3 4 (sum=10)
try:
    next(gen)
except StopIteration as e:
    print(f"\nFinal result: {e.value}")  # "Reached limit 10"


gen1 = GeneratorOps.data_processor()
next(gen1)  # Prime the generator (yields 0.0)
gen1.send([1.5, 2.5])  # Sends batch, yields average (2.0)
gen1.send([3.0, 4.0])  # Yields new average (2.75)
try:
    gen1.send(None)  # Signal completion
except StopIteration as e:
    print(f"Final stats: {e.value}")  # {'total': 11.0, 'average': 2.75}
