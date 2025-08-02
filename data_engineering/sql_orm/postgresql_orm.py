import psycopg2
from dataclasses import dataclass
from typing import Optional, Self
from datetime import datetime


@dataclass
class Product:
    """Dataclass representing a product."""
    id: Optional[int] = None
    name: str = ""
    price: float = 0.0
    in_stock: bool = False
    created_at: datetime = datetime.now()


class PostgreSQLORM:
    """PostgreSQL ORM with connection pooling."""
    
    def __init__(self: Self, dsn: str):
        self.dsn = dsn
        self._create_table()
    
    def _get_connection(self: Self):
        return psycopg2.connect(self.dsn)
    
    def _create_table(self: Self) -> None:
        """Initialize database schema."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    price DECIMAL(10, 2) CHECK (price >= 0),
                    in_stock BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """)
                conn.commit()
    
    def insert_product(self: Self, product: Product) -> Product:
        """Insert a product record."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO products (name, price, in_stock)
                VALUES (%s, %s, %s)
                RETURNING id, created_at
                """, (product.name, product.price, product.in_stock))
                
                row = cur.fetchone()
                product.id = row[0]
                product.created_at = row[1]
                conn.commit()
                return product


# Usage Example:
if __name__ == "__main__":
    db = PostgreSQLORM(
        "dbname=test user=postgres password=secret host=localhost"
    )
    
    # Insert a product
    new_product = Product(
        name="Python Programming Book",
        price=39.99,
        in_stock=True
    )
    inserted = db.insert_product(new_product)
    print(f"Inserted product ID: {inserted.id}")
