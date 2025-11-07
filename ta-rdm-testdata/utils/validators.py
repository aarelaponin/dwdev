"""
Data validation utilities for TA-RDM Test Data Generator.

Provides validation functions for TINs, NICs, BRNs, and other
business rules.
"""

import re
from datetime import date
from typing import Optional


def validate_tin(tin: str) -> bool:
    """
    Validate Sri Lankan TIN format.

    Format: 9 digits

    Args:
        tin: Tax Identification Number

    Returns:
        bool: True if valid
    """
    if not tin:
        return False

    # Must be exactly 9 digits
    pattern = r'^\d{9}$'
    return bool(re.match(pattern, tin))


def validate_nic_old_format(nic: str) -> bool:
    """
    Validate old format Sri Lankan NIC.

    Format: YYDDDGGGGV
    - YY: Year (2 digits)
    - DDD: Day of year (001-366 or 501-866)
    - GGGG: Sequential number (4 digits)
    - V: Literal 'V'

    Args:
        nic: National Identity Card number

    Returns:
        bool: True if valid format
    """
    if not nic:
        return False

    # Pattern: 2 digits + 3 digits + 4 digits + V
    pattern = r'^\d{9}V$'
    if not re.match(pattern, nic):
        return False

    # Extract day of year (positions 2-5)
    day_of_year = int(nic[2:5])

    # Valid range: 001-366 (males) or 501-866 (females)
    return (1 <= day_of_year <= 366) or (501 <= day_of_year <= 866)


def validate_nic_new_format(nic: str) -> bool:
    """
    Validate new format Sri Lankan NIC.

    Format: YYYYDDDGGGGG
    - YYYY: Year (4 digits)
    - DDD: Day of year (001-366 or 501-866)
    - GGGGG: Sequential number (5 digits)

    Args:
        nic: National Identity Card number

    Returns:
        bool: True if valid format
    """
    if not nic:
        return False

    # Pattern: 12 digits
    pattern = r'^\d{12}$'
    if not re.match(pattern, nic):
        return False

    # Extract components
    year = int(nic[0:4])
    day_of_year = int(nic[4:7])

    # Validate year (reasonable range)
    if year < 1900 or year > date.today().year:
        return False

    # Valid range: 001-366 (males) or 501-866 (females)
    return (1 <= day_of_year <= 366) or (501 <= day_of_year <= 866)


def validate_nic(nic: str) -> bool:
    """
    Validate Sri Lankan NIC (either format).

    Args:
        nic: National Identity Card number

    Returns:
        bool: True if valid
    """
    if not nic:
        return False

    # Check length to determine format
    if len(nic) == 10:
        return validate_nic_old_format(nic)
    elif len(nic) == 12:
        return validate_nic_new_format(nic)
    else:
        return False


def validate_brn(brn: str) -> bool:
    """
    Validate Sri Lankan BRN format.

    Format: PV######## (PV followed by 8 digits)

    Args:
        brn: Business Registration Number

    Returns:
        bool: True if valid
    """
    if not brn:
        return False

    # Pattern: PV + 8 digits
    pattern = r'^PV\d{8}$'
    return bool(re.match(pattern, brn))


def validate_email(email: str) -> bool:
    """
    Validate email address format.

    Args:
        email: Email address

    Returns:
        bool: True if valid
    """
    if not email:
        return False

    # Simple email pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def validate_phone_number(phone: str) -> bool:
    """
    Validate Sri Lankan phone number format.

    Formats:
    - Mobile: +94 7X XXX XXXX
    - Landline: +94 XX XXX XXXX

    Args:
        phone: Phone number

    Returns:
        bool: True if valid
    """
    if not phone:
        return False

    # Remove spaces for validation
    phone_clean = phone.replace(' ', '')

    # Pattern: +94 followed by 9 digits
    pattern = r'^\+94\d{9,10}$'
    return bool(re.match(pattern, phone_clean))


def validate_date_range(start_date: date, end_date: Optional[date]) -> bool:
    """
    Validate that end_date is after start_date.

    Args:
        start_date: Start date
        end_date: End date (can be None)

    Returns:
        bool: True if valid

    Raises:
        ValueError: If dates are invalid
    """
    if end_date is None:
        return True  # NULL end date is valid (means current/active)

    if end_date <= start_date:
        raise ValueError(
            f"End date ({end_date}) must be after start date ({start_date})"
        )

    return True


def validate_amount(amount: float, min_value: float = 0.0,
                   max_value: float = None) -> bool:
    """
    Validate monetary amount.

    Args:
        amount: Amount to validate
        min_value: Minimum allowed value
        max_value: Maximum allowed value

    Returns:
        bool: True if valid

    Raises:
        ValueError: If amount is invalid
    """
    if amount < min_value:
        raise ValueError(f"Amount {amount} is less than minimum {min_value}")

    if max_value is not None and amount > max_value:
        raise ValueError(f"Amount {amount} exceeds maximum {max_value}")

    # Check for valid decimal places (2 decimal places for currency)
    if round(amount, 2) != amount:
        raise ValueError(f"Amount {amount} has more than 2 decimal places")

    return True


def validate_percentage(percentage: float) -> bool:
    """
    Validate percentage value.

    Args:
        percentage: Percentage to validate (0-100)

    Returns:
        bool: True if valid

    Raises:
        ValueError: If percentage is invalid
    """
    if not 0 <= percentage <= 100:
        raise ValueError(f"Percentage {percentage} must be between 0 and 100")

    return True


def validate_postal_code(postal_code: str) -> bool:
    """
    Validate Sri Lankan postal code.

    Format: 5 digits

    Args:
        postal_code: Postal code

    Returns:
        bool: True if valid
    """
    if not postal_code:
        return False

    # Pattern: 5 digits
    pattern = r'^\d{5}$'
    return bool(re.match(pattern, postal_code))


def validate_uuid(uuid_str: str) -> bool:
    """
    Validate UUID format.

    Format: 8-4-4-4-12 hexadecimal characters

    Args:
        uuid_str: UUID string

    Returns:
        bool: True if valid
    """
    if not uuid_str:
        return False

    # Pattern: UUID v4
    pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    return bool(re.match(pattern, uuid_str.lower()))


def validate_party_name(name: str, min_length: int = 2,
                       max_length: int = 500) -> bool:
    """
    Validate party name.

    Args:
        name: Party name
        min_length: Minimum length
        max_length: Maximum length

    Returns:
        bool: True if valid

    Raises:
        ValueError: If name is invalid
    """
    if not name or not name.strip():
        raise ValueError("Party name cannot be empty")

    if len(name) < min_length:
        raise ValueError(f"Party name must be at least {min_length} characters")

    if len(name) > max_length:
        raise ValueError(f"Party name cannot exceed {max_length} characters")

    return True
