#!/usr/bin/env python3
"""Quick script to check filing_assessment table schemas."""

from utils.db_utils import get_db_connection

tables = [
    'tax_return',
    'tax_return_line',
    'assessment',
    'assessment_line',
    'penalty_assessment',
    'interest_assessment'
]

with get_db_connection() as db:
    for table in tables:
        print(f"\nfiling_assessment.{table} table structure:")
        print("=" * 120)
        columns = db.fetch_all(f"DESCRIBE filing_assessment.{table}")

        print(f"{'Field':<35} {'Type':<35} {'Null':<10} {'Key':<10}")
        print("-" * 120)
        for col in columns:
            print(f"{col[0]:<35} {col[1]:<35} {col[2]:<10} {col[3]:<10}")
