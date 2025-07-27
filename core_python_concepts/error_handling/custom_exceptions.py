from typing import Type, Any


class DataValidationError(Exception):
    """Base class for data validation errors."""
    ...


class MissingColumnError(DataValidationError):
    """Raised when a required column is missing."""
    def __init__(
        self,
        column_name: str,
        message: str = "Required column is missing"
    ) -> None:
        self.column_name = column_name
        self.message = f"{message}: {column_name}"
        super().__init__(self.message)


class InvalidDataTypeError(DataValidationError):
    """Raised when data has incorrect type."""
    def __init__(
        self,
        column: str,
        expected_type: Type,
        actual_type: Type,
        row: int = None
    ) -> None:
        self.column = column
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.row = row
        message = (
            f"Column '{column}' has invalid type. "
            f"Expected {expected_type.__name__}, got {actual_type.__name__}"
        )
        if row is not None:
            message += f" at row {row}"
        super().__init__(message)


def validate_data(
        data: list[dict[str, Any]],
        required_columns: dict[str, Type]
) -> None:
    """Validate data structure and data types using standard Python."""
    if not data:
        return
    first_row = data[0]
    missing_cols = [col for col in required_columns if col not in first_row]
    if missing_cols:
        raise MissingColumnError(missing_cols[0])

    for i, row in enumerate(data):
        for col, expected_type in required_columns.items():
            if col not in row:
                continue

            value = row[col]
            if not isinstance(value, expected_type):
                raise InvalidDataTypeError(
                    col,
                    expected_type,
                    type(value),
                    row=i
                )


# Usage example
try:
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": "twenty-five"},
    ]

    # Will raise InvalidDataTypeError for age in second row
    validate_data(data, {"id": int, "name": str, "age": int})

except DataValidationError as e:
    print(f"Data validation failed: {e}")
