import sys


class TupleOperations:
    """Demonstrates tuple operations and optimizations."""

    @staticmethod
    def memory_comparison() -> str:
        """Compare memory usage between list and tuple."""
        list_size = sys.getsizeof([1, 2, 3, 4, 5])
        tuple_size = sys.getsizeof((1, 2, 3, 4, 5))
        return f"List: {list_size} bytes, Tuple: {tuple_size} bytes"

    @staticmethod
    def tuple_as_key() -> str:
        """Show using tuple as dictionary key."""
        locations = {
            (52.5200, 13.4050): 'Berlin',
            (48.1351, 11.5820): 'Munich'
        }
        return locations[(52.5200, 13.4050)]

    @staticmethod
    def unpack_tuples() -> str:
        """Demonstrate tuple unpacking."""
        person = ('Alice', 28, 'Berlin')
        name, age, city = person
        return f"{name} is {age} years old from {city}"


# Usage examples
tuple_ops = TupleOperations()
print(tuple_ops.memory_comparison())  # List: 96 bytes, Tuple: 80 bytes
print(tuple_ops.tuple_as_key())       # Berlin
print(tuple_ops.unpack_tuples())      # Alice is 28 years old from Berlin
