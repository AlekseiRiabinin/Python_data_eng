class DataTransformer:
    """Combines multiple comprehension types."""

    @staticmethod
    def process_nested_data(data: list[dict]) -> dict[str, list[int]]:
        """Convert list of dicts to dict of filtered lists."""
        return {
            key: [val**2 for val in values if val > 0]
            for item in data
            for key, values in item.items()
        }


# Usage
input_data = [{'A': [1, -2, 3]}, {'B': [4, 5, -6]}]
print(DataTransformer.process_nested_data(input_data))
# Output: {'A': [1, 9], 'B': [16, 25]}
