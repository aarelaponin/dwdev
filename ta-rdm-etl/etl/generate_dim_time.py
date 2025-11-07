#!/usr/bin/env python3
"""
Generate dim_time dimension data for ClickHouse.

Generates daily time dimension records for Phase B test data.
Date range: 2023-01-01 to 2025-12-31 (3 years, ~1095 days)

This is a direct-to-L3 generator (no L2 source for time dimension).
"""

import logging
from datetime import date, timedelta
from typing import List, Tuple
import calendar
import clickhouse_connect

from config.clickhouse_config import CLICKHOUSE_CONFIG


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


# Sri Lankan public holidays (simplified for Phase B)
SRI_LANKAN_HOLIDAYS = {
    # 2023
    (2023, 1, 1): "New Year's Day",
    (2023, 2, 4): "Independence Day",
    (2023, 4, 14): "Sinhala & Tamil New Year",
    (2023, 5, 1): "May Day",
    (2023, 12, 25): "Christmas Day",

    # 2024
    (2024, 1, 1): "New Year's Day",
    (2024, 2, 4): "Independence Day",
    (2024, 4, 14): "Sinhala & Tamil New Year",
    (2024, 5, 1): "May Day",
    (2024, 12, 25): "Christmas Day",

    # 2025
    (2025, 1, 1): "New Year's Day",
    (2025, 2, 4): "Independence Day",
    (2025, 4, 14): "Sinhala & Tamil New Year",
    (2025, 5, 1): "May Day",
    (2025, 12, 25): "Christmas Day",
}


def generate_time_records(start_date: date, end_date: date) -> List[Tuple]:
    """
    Generate time dimension records for date range.

    Args:
        start_date: Start date
        end_date: End date (inclusive)

    Returns:
        List of tuples for ClickHouse insert
    """
    logger.info(f"Generating time dimension records from {start_date} to {end_date}...")

    records = []
    current_date = start_date

    while current_date <= end_date:
        # Date key: YYYYMMDD format
        date_key = int(current_date.strftime('%Y%m%d'))

        # Basic date components
        year = current_date.year
        month = current_date.month
        day = current_date.day

        # Quarter
        quarter = (month - 1) // 3 + 1
        quarter_name = f'Q{quarter}'

        # Month names
        month_name = calendar.month_name[month]
        month_abbr = calendar.month_abbr[month]

        # Week of year (ISO week)
        week_of_year = current_date.isocalendar()[1]

        # Day of week (1 = Monday, 7 = Sunday)
        day_of_week = current_date.isoweekday()
        day_name = calendar.day_name[current_date.weekday()]
        day_abbr = calendar.day_abbr[current_date.weekday()]

        # Fiscal year (Sri Lanka: January-December, same as calendar year)
        fiscal_year = year
        fiscal_quarter = quarter
        fiscal_month = month

        # Weekend flag (Saturday = 6, Sunday = 7)
        is_weekend = 1 if day_of_week >= 6 else 0

        # Holiday flag
        holiday_key = (year, month, day)
        is_holiday = 1 if holiday_key in SRI_LANKAN_HOLIDAYS else 0
        holiday_name = SRI_LANKAN_HOLIDAYS.get(holiday_key, None)

        # Business day (not weekend and not holiday)
        is_business_day = 1 if (not is_weekend and not is_holiday) else 0

        # Month/quarter/year flags
        is_month_start = 1 if day == 1 else 0
        is_month_end = 1 if day == calendar.monthrange(year, month)[1] else 0
        is_quarter_start = 1 if month in [1, 4, 7, 10] and day == 1 else 0
        is_quarter_end = 1 if month in [3, 6, 9, 12] and day == calendar.monthrange(year, month)[1] else 0
        is_year_start = 1 if month == 1 and day == 1 else 0
        is_year_end = 1 if month == 12 and day == 31 else 0

        record = (
            date_key,
            current_date,
            year,
            quarter,
            quarter_name,
            month,
            month_name,
            month_abbr,
            week_of_year,
            day,
            day_of_week,
            day_name,
            day_abbr,
            fiscal_year,
            fiscal_quarter,
            fiscal_month,
            is_weekend,
            is_holiday,
            holiday_name,
            is_business_day,
            is_month_start,
            is_month_end,
            is_quarter_start,
            is_quarter_end,
            is_year_start,
            is_year_end
        )
        records.append(record)

        current_date += timedelta(days=1)

    logger.info(f"  Generated {len(records)} time dimension record(s)")
    return records


def load_to_clickhouse(records: List[Tuple], clickhouse_client) -> int:
    """
    Load time dimension records to ClickHouse.

    Args:
        records: Time dimension records
        clickhouse_client: ClickHouse client connection

    Returns:
        Number of rows loaded
    """
    logger.info("Loading time dimension to ClickHouse dim_time...")

    if not records:
        logger.warning("  No records to load")
        return 0

    # Truncate existing data
    clickhouse_client.command("TRUNCATE TABLE ta_dw.dim_time")
    logger.info("  Truncated dim_time")

    # Insert records
    clickhouse_client.insert(
        'ta_dw.dim_time',
        records,
        column_names=[
            'date_key', 'full_date', 'year', 'quarter', 'quarter_name',
            'month', 'month_name', 'month_abbr', 'week_of_year',
            'day_of_month', 'day_of_week', 'day_name', 'day_abbr',
            'fiscal_year', 'fiscal_quarter', 'fiscal_month',
            'is_weekend', 'is_holiday', 'holiday_name', 'is_business_day',
            'is_month_start', 'is_month_end', 'is_quarter_start',
            'is_quarter_end', 'is_year_start', 'is_year_end'
        ]
    )

    logger.info(f"  Loaded {len(records)} row(s) to dim_time")
    return len(records)


def verify_load(clickhouse_client) -> None:
    """
    Verify the loaded data in dim_time.

    Args:
        clickhouse_client: ClickHouse client connection
    """
    logger.info("Verifying dim_time data...")

    # Get summary statistics
    result = clickhouse_client.query("""
        SELECT
            MIN(full_date) as min_date,
            MAX(full_date) as max_date,
            COUNT(*) as total_days,
            SUM(is_business_day) as business_days,
            SUM(is_weekend) as weekends,
            SUM(is_holiday) as holidays
        FROM ta_dw.dim_time
    """)

    stats = result.result_rows[0]
    min_date, max_date, total_days, business_days, weekends, holidays = stats

    print("\n" + "=" * 80)
    print("dim_time: Time Dimension Summary")
    print("=" * 80)
    print(f"Date Range:       {min_date} to {max_date}")
    print(f"Total Days:       {total_days}")
    print(f"Business Days:    {business_days}")
    print(f"Weekends:         {weekends}")
    print(f"Holidays:         {holidays}")
    print("=" * 80)

    # Show sample records
    sample = clickhouse_client.query("""
        SELECT
            date_key,
            full_date,
            day_name,
            month_name,
            year,
            is_business_day,
            is_holiday,
            holiday_name
        FROM ta_dw.dim_time
        WHERE is_holiday = 1 OR is_month_start = 1
        ORDER BY full_date
        LIMIT 15
    """)

    print("\nSample Records (Holidays and Month Starts):")
    print("-" * 80)
    print(f"{'Date Key':<10} {'Date':<12} {'Day':<10} {'Month':<12} {'Year':<6} {'Biz':<5} {'Holiday':<30}")
    print("-" * 80)

    for row in sample.result_rows:
        date_key, full_date, day_name, month_name, year, is_biz, is_hol, hol_name = row
        holiday_display = hol_name if hol_name else '(month start)' if not is_hol else ''
        print(f"{date_key:<10} {str(full_date):<12} {day_name:<10} {month_name:<12} "
              f"{year:<6} {'Y' if is_biz else 'N':<5} {holiday_display:<30}")

    print("=" * 80 + "\n")


def main():
    """Main execution."""
    logger.info("=" * 60)
    logger.info("Starting Time Dimension Generation (Phase B)")
    logger.info("=" * 60)

    try:
        # Date range: 2023-01-01 to 2025-12-31
        start_date = date(2023, 1, 1)
        end_date = date(2025, 12, 31)

        # Connect to ClickHouse
        clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

        # Generate and load
        records = generate_time_records(start_date, end_date)
        loaded_count = load_to_clickhouse(records, clickhouse_client)

        # Verify
        verify_load(clickhouse_client)

        logger.info("=" * 60)
        logger.info("Time Dimension Generation Completed Successfully")
        logger.info(f"  Generated: {len(records)} days")
        logger.info(f"  Loaded: {loaded_count} days")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Time dimension generation failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
