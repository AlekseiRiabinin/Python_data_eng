from typing import NamedTuple


class Advanced:
    """Demonstrates more advanced optimized structures."""

    class Person(NamedTuple):
        name: str
        age: int
        city: str

    @staticmethod
    def namedtuple_example() -> Person:
        """Show namedtuple usage."""
        return Advanced.Person('Alice', 28, 'Berlin')

    @staticmethod
    def slots_example() -> str:
        """Show memory optimization with __slots__."""
        class DataPoint:
            __slots__ = ['x', 'y']  # Restricts attributes to only these two

            def __init__(self, x, y):
                self.x = x
                self.y = y

        p = DataPoint(10, 20)
        return f"Point at ({p.x}, {p.y})"


# Usage examples
advanced = Advanced()
person = advanced.namedtuple_example()
print(person.name, person.age)  # Alice 28
print(advanced.slots_example())  # Point at (10, 20)
