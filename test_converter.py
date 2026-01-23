#!/usr/bin/env python3
"""
Test script for the PostgreSQL to Snowflake dialect converter.
This script allows you to test individual SQL statements with the converter.

Usage:
    python test_converter.py                    # Interactive mode
    python test_converter.py <sql_file>        # Read from file
    python test_converter.py -h                # Show help
"""

import sys
import os

# Add the code directory to the path so we can import the functions
sys.path.append(os.path.join(os.path.dirname(__file__), 'code'))

from functions.dialect_converter import convert_postgres_to_snowflake

def test_sql_conversion():
    """Test the SQL conversion with user input."""

    print("PostgreSQL to Snowflake SQL Converter Test")
    print("=" * 50)

    sql = ""

    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print("Usage:")
            print("  python test_converter.py                    # Interactive mode")
            print("  python test_converter.py <sql_file>        # Read from file")
            print("  python test_converter.py -h                # Show help")
            return

        # Read from file
        sql_file = sys.argv[1]
        if os.path.exists(sql_file):
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql = f.read()
            print(f"Reading SQL from file: {sql_file}")
        else:
            print(f"File not found: {sql_file}")
            return
    else:
        print("Enter your PostgreSQL SQL statement below (end with an empty line):")
        print()

        # Read multiline input until empty line
        sql_lines = []
        while True:
            try:
                line = input()
                if line.strip() == "":
                    break
                sql_lines.append(line)
            except EOFError:
                break

        sql = "\n".join(sql_lines)

    if not sql.strip():
        print("No SQL provided. Exiting.")
        return

    print("\nOriginal SQL:")
    print("-" * 30)
    print(sql)
    print()

    print("Converting...")
    print()

    try:
        converted = convert_postgres_to_snowflake(sql)
        print("Converted SQL:")
        print("-" * 30)
        print(converted)
    except Exception as e:
        print(f"Error during conversion: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_sql_conversion()