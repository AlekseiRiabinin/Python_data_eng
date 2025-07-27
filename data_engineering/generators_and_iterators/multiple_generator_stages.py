from typing import Iterator, TypedDict


class User(TypedDict):
    """Type definition for user data structure."""
    user_id: str
    name: str
    is_active: bool
    account_age_days: int
    monthly_purchases: float
    payment_delays: int
    credit_score: float | None  # Will be added by the pipeline


def calculate_score(user: User) -> float:
    """Calculate a credit score based on user attributes."""
    score = 600.0
    
    age_factor = min(user['account_age_days'] / 365 * 10, 100)
    score += age_factor
    
    purchase_factor = min(user['monthly_purchases'] / 1000 * 20, 100)
    score += purchase_factor
    
    delay_penalty = min(user['payment_delays'] * 15, 200)
    score -= delay_penalty
    
    return max(300.0, min(score, 850.0))


def filter_active_users(users: Iterator[User]) -> Iterator[User]:
    """Filter only active users from a stream."""
    for user in users:
        if user['is_active']:
            yield user


def add_credit_score(users: Iterator[User]) -> Iterator[User]:
    """Calculate and add credit score to user data."""
    for user in users:
        user['credit_score'] = calculate_score(user)
        yield user


def user_processing_pipeline(source: Iterator[User]) -> Iterator[User]:
    """Complete processing pipeline."""
    active_users = filter_active_users(source)
    scored_users = add_credit_score(active_users)
    yield from scored_users


# Example Usage:
def generate_sample_users() -> Iterator[User]:
    """Mock user data generator for testing."""
    users = [
        {
            'user_id': 'u1',
            'name': 'Alice',
            'is_active': True,
            'account_age_days': 730,
            'monthly_purchases': 2500.0,
            'payment_delays': 2
        },
        {
            'user_id': 'u2',
            'name': 'Bob',
            'is_active': False,  # Inactive user
            'account_age_days': 180,
            'monthly_purchases': 500.0,
            'payment_delays': 0
        },
        {
            'user_id': 'u3',
            'name': 'Charlie',
            'is_active': True,
            'account_age_days': 365,
            'monthly_purchases': 1200.0,
            'payment_delays': 5
        }
    ]
    yield from users


# Process the users
processed_users = user_processing_pipeline(generate_sample_users())

for user in processed_users:
    print(f"User: {user['name']} | Score: {user['credit_score']:.0f}")
