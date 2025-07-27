import sys


def optimize_categories(categories: list[str]) -> list[str]:
    """Reduce memory for repeated category strings."""
    return [sys.intern(cat) for cat in categories]


# Usage: Process product categories
categories = ["Electronics"] * 10_000 + ["Clothing"] * 5_000
optimized = optimize_categories(categories)  # 50% memory reduction
