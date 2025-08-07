from typing import Optional, Self
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from bson import ObjectId


class MongoDBClient:
    """MongoDB ORM with type-safe operations."""
    
    def __init__(
            self: Self,
            uri: str = "mongodb://localhost:27017/"
    ) -> None:
        self.client = MongoClient(uri)
        self.db = self.client["business"]
        self.customers: Collection = self.db["customers"]
        self._create_indexes()

    def _create_indexes(self: Self) -> None:
        """Ensure indexes for performance."""
        self.customers.create_index([("email", ASCENDING)], unique=True)
        self.customers.create_index([("is_active", ASCENDING)])

    def insert_customer(self: Self, customer: dict) -> dict:
        """Insert a customer document with automatic timestamps."""
        document = {
            **customer,
            "created_at": datetime.now(),
            "is_active": customer.get("is_active", True)
        }
        result = self.customers.insert_one(document)
        return self.get_customer(result.inserted_id)

    def get_customer(self: Self, customer_id: str) -> Optional[dict]:
        """Retrieve a customer by ID."""
        doc = self.customers.find_one({"_id": ObjectId(customer_id)})
        if doc:
            doc['id'] = str(doc.pop('_id'))
        return doc

    def get_active_customers(self: Self) -> list[dict]:
        """Retrieve all active customers."""
        return [
            {**doc, 'id': str(doc.pop('_id'))}
            for doc in self.customers.find({"is_active": True})
        ]

    def update_customer_status(
            self: Self,
            customer_id: str,
            active: bool
    ) -> bool:
        """Update a customer's active status."""
        result = self.customers.update_one(
            {"_id": ObjectId(customer_id)},
            {"$set": {"is_active": active}}
        )
        return result.modified_count > 0


# Usage Example
if __name__ == "__main__":
    mongo = MongoDBClient()
    
    # Insert document
    customer = {
        "name": "Bob Johnson",
        "email": "bob@example.com",
        "preferences": {"newsletter": True}
    }
    inserted = mongo.insert_customer(customer)
    print(f"Inserted customer ID: {inserted['id']}")

    # Query documents
    active_customers = mongo.get_active_customers()
    print(f"Active customers: {len(active_customers)}")
