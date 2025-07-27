from collections import deque
from typing import Iterable, Callable, TypeVar

T = TypeVar('T')  # Generic type for input items


class ListManager:
    """Demonstrates optimized list operations."""

    @staticmethod
    def create_with_comprehension(n: int) -> list[int]:
        """Create list using comprehension (faster than loop)."""
        return [x**2 for x in range(n)]

    @staticmethod
    def efficient_front_back_operations() -> deque:
        """Show efficient front/back operations using deque."""
        dq = deque([1, 2, 3])
        dq.appendleft(0)  # O(1) operation
        dq.append(4)      # O(1) operation
        return dq

    @staticmethod
    def filter_list(items: Iterable[T],
                    condition: Callable[[T], bool]) -> list[T]:
        """Filter using list comprehensions."""
        return [x for x in items if condition(x)]


# Usage examples
list_mgr = ListManager()
print(list_mgr.create_with_comprehension(5))  # [0, 1, 4, 9, 16]
print(list_mgr.efficient_front_back_operations())  # deque([0, 1, 2, 3, 4])
print(list_mgr.filter_list(range(10), lambda x: x % 2 == 0))  # [0, 2, 4, 6, 8]
