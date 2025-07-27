from collections import defaultdict
from typing import TypeVar

T = TypeVar('T')  # Generic type for input items


class DictManager:
    """Demonstrates dictionary operations and optimizations."""

    @staticmethod
    def merge_dicts(dict1: dict[T], dict2: dict[T]) -> dict[T]:
        """Merge dictionaries (Python 3.9+)."""
        return dict1 | dict2

    @staticmethod
    def handle_missing_keys() -> dict[str, int | dict[str, int]]:
        """Show different ways to handle missing keys."""

        # Standard dict
        d1 = {'a': 1, 'b': 2}
        value = d1.get('c', 0)  # Returns 0 if key doesn't exist

        # defaultdict
        word_counts = defaultdict(int)
        word_counts['hello'] += 1  # Automatically initializes to 0

        return {
            'standard_dict': value,
            'defaultdict': dict(word_counts)
        }

    @staticmethod
    def dict_comprehension() -> dict[int]:
        """Show efficient dictionary creation."""
        return {x: x**3 for x in range(5)}  # {0: 0, 1: 1, 2: 8, 3: 27, 4: 64}


# Usage examples
dict_mgr = DictManager()
print(dict_mgr.merge_dicts({'a': 1}, {'b': 2}))  # {'a': 1, 'b': 2}
print(dict_mgr.handle_missing_keys())  # {'st_dict': 0,'defdict': {'hello': 1}}
print(dict_mgr.dict_comprehension())   # {0: 0, 1: 1, 2: 8, 3: 27, 4: 64}
