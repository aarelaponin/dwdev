#!/usr/bin/env python3
"""Quick script to check tax_period table schema."""

from utils.db_utils import get_db_connection

with get_db_connection() as db:
    columns = db.fetch_all("DESCRIBE tax_framework.tax_period")

    print("tax_framework.tax_period table structure:")
    print("-" * 100)
    print(f"{'Field':<30} {'Type':<30} {'Null':<10} {'Key':<10} {'Default':<15}")
    print("-" * 100)
    for col in columns:
        print(f"{col[0]:<30} {col[1]:<30} {col[2]:<10} {col[3]:<10} {str(col[4]):<15}")
