from typing import TypedDict, Optional, Self
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId


class Customer(TypedDict):
    """Type definition for customer document."""
    _id: Optional[ObjectId]
    name: str
    email: str
    created_at: datetime
    is_active: bool


class MongoDBORM:
    """MongoDB ORM with type-safe operations."""
    
    def __init__(self: Self, uri: str = "mongodb://localhost:27017/"):
        self.client = MongoClient(uri)
        self.db = self.client["business"]
        self.customers = self.db["customers"]
    
    def insert_customer(self: Self, customer: Customer) -> Customer:
        """Insert a customer document."""
        customer["created_at"] = customer.get("created_at", datetime.now())
        customer["is_active"] = customer.get("is_active", True)
        
        result = self.customers.insert_one(customer)
        customer["_id"] = result.inserted_id
        return customer
    
    def get_customer(self: Self, customer_id: str) -> Optional[Customer]:
        """Retrieve a customer by ID."""
        doc = self.customers.find_one({"_id": ObjectId(customer_id)})
        return doc if doc else None
    
    def get_active_customers(self: Self) -> list[Customer]:
        """Retrieve all active customers."""
        return list(self.customers.find({"is_active": True}))
    
    def update_customer_email(self: Self, customer_id: str, new_email: str) -> bool:
        """Update a customer's email."""
        result = self.customers.update_one(
            {"_id": ObjectId(customer_id)},
            {"$set": {"email": new_email}}
        )
        return result.modified_count > 0


# Usage Example:
if __name__ == "__main__":
    mongo_orm = MongoDBORM()
    
    # Insert document
    customer: Customer = {
        "name": "Bob Johnson",
        "email": "bob@example.com"
    }
    inserted = mongo_orm.insert_customer(customer)
    print(f"Inserted customer ID: {inserted['_id']}")
    
    # Query documents
    active_customers = mongo_orm.get_active_customers()
    print(f"Active customers: {len(active_customers)}")
