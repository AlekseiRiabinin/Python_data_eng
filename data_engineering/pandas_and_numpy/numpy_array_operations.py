import numpy as np
import numpy.typing as npt


def normalize_array(
    arr: npt.NDArray[np.float64],
    method: str = 'zscore'
) -> npt.NDArray[np.float64]:
    """Normalize a NumPy array using specified method."""

    if method == 'zscore':
        mean = np.mean(arr)
        std = np.std(arr)
        if std == 0:
            return arr - mean
        return (arr - mean) / std

    elif method == 'minmax':
        min_val = np.min(arr)
        max_val = np.max(arr)
        if min_val == max_val:
            return np.zeros_like(arr)
        return (arr - min_val) / (max_val - min_val)

    elif method == 'log':
        return np.log1p(arr)

    else:
        raise ValueError(f"Unknown normalization method: {method}")


# Example usage:
data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
normalized = normalize_array(data, method='zscore')
print(f"Original: {data}")
print(f"Normalized: {normalized}")
