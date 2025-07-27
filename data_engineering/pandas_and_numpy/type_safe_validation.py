import pandas as pd
from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Self, Any, TypedDict


class ValidationResult(TypedDict):
    """Type definition for validation result dictionary"""
    row_id: Any
    valid: bool
    errors: str | None


class CustomerData(BaseModel):
    """Data model for customer data validation."""
    customer_id: int
    name: str
    join_date: datetime
    lifetime_value: float
    
    @field_validator('name')
    def name_must_contain_space(cls: type[Self], v: str) -> str:
        """Validate that name contains a space and return title case version."""
        if ' ' not in v:
            raise ValueError('Name must contain a space')
        return v.title()
    
    @field_validator('lifetime_value')
    def value_must_be_positive(cls: type[Self], v: float) -> float:
        """Validate that lifetime value is positive."""
        if v < 0:
            raise ValueError('Lifetime value cannot be negative')
        return v


def validate_dataframe(df: pd.DataFrame) -> list[ValidationResult]:
    """Validate DataFrame rows against a Pydantic model."""
    results: list[ValidationResult] = []
    for _, row in df.iterrows():
        try:
            CustomerData(**row.to_dict())
            results.append({
                'row_id': row.name,
                'valid': True,
                'errors': None
            })
        except Exception as e:
            results.append({
                'row_id': row.name,
                'valid': False,
                'errors': str(e)
            })
    return results


# Example usage:
customer_df: pd.DataFrame = pd.DataFrame({
    'customer_id': [1, 2],
    'name': ['John Doe', 'Alice'],
    'join_date': ['2023-01-01', '2023-02-01'],
    'lifetime_value': [100.50, -50.0]
})
customer_df['join_date'] = pd.to_datetime(customer_df['join_date'])

validation_results: list[ValidationResult] = validate_dataframe(customer_df)
print(validation_results)
