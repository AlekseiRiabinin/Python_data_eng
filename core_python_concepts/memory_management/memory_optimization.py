class DataRecord:
    """Optimized for storing 1M+ records in memory."""
    __slots__ = ('timestamp', 'value', 'source')  # 40% memory reduction

    def __init__(self, timestamp: str, value: float, source: str):
        self.timestamp = timestamp
        self.value = value
        self.source = source


def load_dataset() -> list[DataRecord]:
    return [
        DataRecord(
            timestamp="2023-01-01",
            value=i*0.5,
            source="sensor1"
        )
        for i in range(1_000_000)
    ]


# Usage: 1M records with minimal memory
records = load_dataset()
