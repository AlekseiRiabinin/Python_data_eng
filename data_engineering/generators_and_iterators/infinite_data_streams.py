import random
from typing import Iterator


def sensor_data_stream(sensor_id: str) -> Iterator[float]:
    """Simulate an infinite stream of sensor readings."""
    while True:
        yield random.uniform(0, 100)  # Simulated reading
        # In real usage, would connect to actual sensor


# Usage:
# for reading in sensor_data_stream('temp_sensor_1'):
#     process_reading(reading)
#     if stop_condition:
#         break
