import psycopg2
from typing import Generator, Optional, TypedDict, Self
from datetime import datetime
from contextlib import contextmanager


class Customer(TypedDict):
    """Type definition for customer records."""
    id: Optional[int]
    name: str
    email: str
    created_at: datetime
    is_active: bool


@contextmanager
def get_db_connection(db_url: str) -> Generator:
    """Context manager for database connections."""
    conn = psycopg2.connect(db_url)
    try:
        yield conn
    finally:
        conn.close()


class PostgreSQLClient:
    """PostgreSQL ORM with type-safe operations."""
    
    def __init__(self: Self, db_url: str) -> None:
        self.db_url = db_url
        self._init_db()
    
    def _init_db(self: Self) -> None:
        """Initialize database schema."""
        with get_db_connection(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    is_active BOOLEAN DEFAULT TRUE
                )
                """)
                conn.commit()
    
    def insert_customer(self: Self, customer: Customer) -> Customer:
        """Insert a customer record with type validation."""
        with get_db_connection(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO customers (name, email, is_active)
                VALUES (%s, %s, %s)
                RETURNING id, created_at
                """, (
                    customer['name'],
                    customer['email'],
                    customer.get('is_active', True)
                ))
                
                row = cur.fetchone()
                customer['id'] = row[0]
                customer['created_at'] = row[1]
                conn.commit()
                return customer
    
    def get_active_customers(self: Self) -> list[Customer]:
        """Retrieve all active customers."""
        with get_db_connection(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT id, name, email, created_at, is_active
                FROM customers
                WHERE is_active = TRUE
                """)
                return [
                    {
                        'id': row[0],
                        'name': row[1],
                        'email': row[2],
                        'created_at': row[3],
                        'is_active': row[4]
                    }
                    for row in cur.fetchall()
                ]


# Usage Example
if __name__ == "__main__":
    db = PostgreSQLClient("postgresql://user:password@localhost/mydb")
    
    # Insert customer
    new_customer: Customer = {
        'name': 'Alice Smith',
        'email': 'alice@example.com'
    }
    inserted = db.insert_customer(new_customer)
    print(f"Inserted customer ID: {inserted['id']}")

    # Query customers
    active_customers = db.get_active_customers()
    print(f"Active customers: {len(active_customers)}")
