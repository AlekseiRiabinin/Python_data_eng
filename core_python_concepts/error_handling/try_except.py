import json


def divide_numbers(a: float, b: float) -> float:
    """Divide two numbers with proper error handling."""
    try:
        result = a / b
    except ZeroDivisionError as e:
        print(f"Error: {e}. Cannot divide by zero.")
        raise  # Re-raise the exception after logging
    except TypeError as e:
        print(f"Error: {e}. Both arguments must be numbers.")
        raise
    else:
        print("Division successful")
        return result
    finally:
        print("Division operation attempted")


def process_data_file(file_path: str) -> list[dict]:
    """"Read and parse a JSON lines file into a list of dictionaries."""
    try:
        with open(file_path, 'r') as f:
            data = [json.loads(line) for line in f]
    except (FileNotFoundError | PermissionError) as e:
        print(f"File error: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Invalid JSON at line {e.lineno}: {e.msg}")
        raise
    else:
        return data


# Usage 1
try:
    print(divide_numbers(10, 2))
    print(divide_numbers(10, 0))  # This will raise ZeroDivisionError
except Exception as e:
    print(f"Caught exception in caller: {type(e).__name__}: {e}")


# Usage 2
try:
    data = process_data_file("data.json")
except Exception as e:
    print(f"Failed to process file: {e}")
else:
    print(f"Processed {len(data)} records")
