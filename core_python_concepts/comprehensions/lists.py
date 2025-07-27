class ListOps:
    """Demonstrates list comprehension patterns."""

    @staticmethod
    def square_numbers(n: int) -> list[int]:
        """Create squares using list comprehension."""
        return [x**2 for x in range(n)]

    @staticmethod
    def filter_evens(numbers: list[int]) -> list[int]:
        """Filter even numbers with condition."""
        return [x for x in numbers if x % 2 == 0]

    @staticmethod
    def nested_loop(matrix: list[list[int]]) -> list[int]:
        """Flatten matrix using nested comprehension."""
        return [num for row in matrix for num in row]


# Usage
print(ListOps.square_numbers(5))  # [0, 1, 4, 9, 16]
print(ListOps.filter_evens([1, 2, 3, 4]))  # [2, 4]
print(ListOps.nested_loop([[1, 2], [3, 4]]))  # [1, 2, 3, 4]
