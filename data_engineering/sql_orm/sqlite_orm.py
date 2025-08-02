import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Self
from datetime import datetime


@dataclass
class Customer:
    """Dataclass representing a customer record."""
    id: Optional[int] = None
    name: str = ""
    email: str = ""
    created_at: datetime = datetime.now()
    is_active: bool = True


class SQLiteORM:
    """Lightweight SQLite ORM with type hints."""
    
    def __init__(self: Self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self._create_tables()
    
    def _create_tables(self: Self) -> None:
        """Initialize database schema."""
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
        """)
    
    def insert_customer(self: Self, customer: Customer) -> Customer:
        """Insert a customer record."""
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO customers (name, email, is_active)
        VALUES (?, ?, ?)
        """, (customer.name, customer.email, customer.is_active))
        
        customer.id = cursor.lastrowid
        self.conn.commit()
        return customer
    
    def get_customer(self: Self, customer_id: int) -> Optional[Customer]:
        """Retrieve a single customer by ID."""
        cursor = self.conn.execute("""
        SELECT id, name, email, created_at, is_active
        FROM customers
        WHERE id = ?
        """, (customer_id,))
        
        row = cursor.fetchone()
        if row:
            return Customer(*row)
        return None
    
    def get_active_customers(self: Self) -> List[Customer]:
        """Retrieve all active customers."""
        cursor = self.conn.execute("""
        SELECT id, name, email, created_at, is_active
        FROM customers
        WHERE is_active = TRUE
        """)
        
        return [Customer(*row) for row in cursor.fetchall()]


# Usage Example:
if __name__ == "__main__":
    db = SQLiteORM("customers.db")
    
    # Insert a new customer
    new_customer = Customer(
        name="Alice Smith",
        email="alice@example.com"
    )
    inserted = db.insert_customer(new_customer)
    print(f"Inserted customer ID: {inserted.id}")
    
    # Query customers
    active_customers = db.get_active_customers()
    print(f"Active customers: {len(active_customers)}")
