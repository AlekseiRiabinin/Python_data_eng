def process_immutable_data(
        raw_data: tuple[tuple[str, int], ...]
) -> list[dict[str, int]]:
    """Convert immutable input to mutable output safely."""
    processed = []
    for name, count in raw_data:
        processed.append({"name": name, "count": count})
    return processed


# Usage: Safe for parallel processing
input_data = (("Alice", 120), ("Bob", 85))   # Immutable
output = process_immutable_data(input_data)  # Mutable result
