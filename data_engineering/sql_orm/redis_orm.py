import redis
from typing import TypedDict, Optional, Self
from datetime import datetime, timedelta
import json


class UserSession(TypedDict):
    """Type definition for user session."""
    user_id: str
    session_token: str
    created_at: str
    expires_at: str
    ip_address: str


class RedisORM:
    """Redis ORM for session management"""
    
    def __init__(self: Self, host: str = "localhost", port: int = 6379):
        self.r = redis.Redis(host=host, port=port)
    
    def store_session(self, session: UserSession) -> bool:
        """Store session data with TTL."""
        key = f"session:{session['user_id']}"
        value = json.dumps(session)
        # Set expiration to 24 hours
        return self.r.setex(key, 86400, value)
    
    def get_session(self: Self, user_id: str) -> Optional[UserSession]:
        """Retrieve session data."""
        key = f"session:{user_id}"
        data = self.r.get(key)
        return json.loads(data) if data else None


# Usage Example:
if __name__ == "__main__":
    redis_orm = RedisORM()
    
    session: UserSession = {
        "user_id": "u12345",
        "session_token": "abc123",
        "created_at": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(days=1)).isoformat(),
        "ip_address": "192.168.1.1"
    }
    
    redis_orm.store_session(session)
    retrieved = redis_orm.get_session("u12345")
    print(f"Session exists: {retrieved is not None}")
