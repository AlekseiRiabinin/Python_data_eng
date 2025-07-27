class DictOps:
    """Demonstrates dictionary comprehension patterns."""

    @staticmethod
    def create_square_map(n: int) -> dict[int, int]:
        """Map numbers to their squares."""
        return {x: x**2 for x in range(n)}

    @staticmethod
    def filter_dict_items(prices: dict[str, float]) -> dict[str, float]:
        """Filter items with value > 100."""
        return {k: v for k, v in prices.items() if v > 100}

    @staticmethod
    def invert_mapping(original: dict[str, str]) -> dict[str, str]:
        """Swap keys and values."""
        return {v: k for k, v in original.items()}


# Usage
print(DictOps.create_square_map(5))  # {0: 0, 1: 1, 2: 4, 3: 9, 4: 16}
print(DictOps.filter_dict_items({'a': 99, 'b': 101}))  # {'b': 101}
print(DictOps.invert_mapping({'a': 'x', 'b': 'y'}))  # {'x': 'a', 'y': 'b'}
