import time
from typing import Iterable, TypeVar

T = TypeVar('T')  # Generic type for input items


class SetOperations:
    """Demonstrates set operations and optimizations."""

    @staticmethod
    def remove_duplicates(items: Iterable[T]) -> list[T]:
        """Remove duplicates from a sequence."""
        return list(set(items))

    @staticmethod
    def performance_comparison() -> str:
        """Compare membership test performance."""
        large_list = list(range(1000000))
        large_set = set(large_list)

        start = time.time()
        999999 in large_list  # O(n) - Linear search one-by-one
        list_time = time.time() - start

        start = time.time()
        999999 in large_set  # O(1) - Hash table lookup is used
        set_time = time.time() - start

        return f"List: {list_time:.6f}s, Set: {set_time:.6f}s"

    @staticmethod
    def set_operations() -> dict[str, set]:
        """Demonstrate common set operations."""
        a = {1, 2, 3, 4}
        b = {3, 4, 5, 6}
        return {
            'union': a.union(b),
            'intersection': a.intersection(b),
            'difference': a.difference(b)
        }


# Usage examples
set_ops = SetOperations()
print(set_ops.remove_duplicates([1, 2, 2, 3, 3, 3]))  # [1, 2, 3]
print(set_ops.performance_comparison())  # List: 0.012345s, Set: 0.000001s
print(set_ops.set_operations())  # {'union': {1, 2, 3, 4, 5, 6}, ...}
