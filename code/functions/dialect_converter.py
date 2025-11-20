import sys
import re
import sqlglot
from sqlglot import exp
from sqlglot.dialects.snowflake import Snowflake

from code.functions.crosstabs import parse_crosstab_sql


# ---- Custom Dialect Definition ----
class FixedSnowflake(Snowflake):
    class Generator(Snowflake.Generator):
        def cast_sql(self, expression: exp.Cast) -> str:
            """Handle PostgreSQL interval casts for Snowflake compatibility"""
            # Check if this is an interval cast
            to_type = expression.to
            if to_type and to_type.this == exp.DataType.Type.INTERVAL:
                # Get the string value being cast
                if isinstance(expression.this, exp.Literal) and expression.this.is_string:
                    value = expression.this.name.strip("'\"")
                    parts = value.split()
                    
                    # Parse "5 days" -> INTERVAL '5 days'
                    if len(parts) >= 2:
                        # Reconstruct as Snowflake INTERVAL syntax
                        return f"INTERVAL '{value}'"
                    elif len(parts) == 1:
                        # If only number, assume days
                        return f"INTERVAL '{parts[0]} days'"
            
            # For all other casts, use default behavior
            return super().cast_sql(expression)
        
        def interval_sql(self, expression: exp.Interval) -> str:
            """Generate Snowflake-compatible INTERVAL syntax"""
            unit = expression.args.get("unit")
            this = expression.this
            
            if unit:
                return f"INTERVAL '{this.name}' {unit.name}"
            return f"INTERVAL '{this.name}'"
        
        def array_sql(self, expression: exp.Array) -> str:
            """Convert ARRAY[] syntax to ARRAY_CONSTRUCT() for Snowflake"""
            expressions = self.expressions(expression, flat=True)
            return f"ARRAY_CONSTRUCT({expressions})"
        
        def eq_sql(self, expression: exp.EQ) -> str:
            """Convert 'value' = ANY(array) to ARRAY_CONTAINS(TO_VARIANT(value), array)"""
            # Check if right side is ANY
            if isinstance(expression.right, exp.Any):
                array_expr = expression.right.this
                value_expr = expression.left
                # Snowflake ARRAY_CONTAINS requires value to be VARIANT type
                return f"ARRAY_CONTAINS(TO_VARIANT({self.sql(value_expr)}), {self.sql(array_expr)})"
            
            # Default behavior for other equality expressions
            return super().eq_sql(expression)

        def function_sql(self, expression: exp.Func) -> str:
            """Translate PostgreSQL generate_series into Snowflake GENERATOR + LATERAL"""
            if expression.name.lower() == "generate_series":
                args = expression.expressions
                arg_count = len(args)

                # Parse all args as SQL strings
                start = self.sql(args[0])
                end = self.sql(args[1]) if arg_count > 1 else None
                step = self.sql(args[2]) if arg_count > 2 else None

                # Default step for numeric sequences
                if step is None:
                    step = "1"

                # Detect INTERVAL steps
                interval_match = re.search(r"INTERVAL\s+'(\d+)\s+(\w+)'", step, re.IGNORECASE)

                if interval_match:
                    # ---- DATE SERIES ----
                    step_value = interval_match.group(1)       # e.g. 1
                    step_unit = interval_match.group(2).upper()  # e.g. YEAR

                    # Snowflake rowcount = number of increments
                    rowcount = (
                        f"DATEDIFF({step_unit}, {start}, {end}) / {step_value} + 1"
                    )

                    return (
                        f"TABLE(GENERATOR(ROWCOUNT => {rowcount})) AS g, "
                        f"LATERAL (SELECT DATEADD({step_unit}, SEQ4() * {step_value}, {start}) AS a) AS s"
                    )

                else:
                    # ---- NUMERIC SERIES ----
                    rowcount = f"(({end}) - ({start})) / ({step}) + 1"

                    return (
                        f"TABLE(GENERATOR(ROWCOUNT => {rowcount})) AS g, "
                        f"LATERAL (SELECT ({start}) + SEQ4() * ({step}) AS a) AS s"
                    )

            # Fallback to default behavior
            return super().function_sql(expression)


def convert_postgres_to_snowflake(sql: str) -> str:
    """Convert SQL from PostgreSQL to Snowflake dialect using sqlglot."""
    try:
        # Pre-process: Handle crosstab function (not supported in Snowflake)
        if 'crosstab' in sql.lower():
            sql = handle_crosstab(sql)

        # Pre-process: Convert unnest(ARRAY[...]) to SELECT ... FROM VALUES (...)
        sql = convert_unnest_array_to_values(sql)

        # Pre-process: Convert generate_series to Snowflake-compatible TABLE(GENERATOR(...))
        sql = convert_generate_series_to_snowflake(sql)

        # Parse with PostgreSQL dialect
        parsed = sqlglot.parse_one(sql, read="postgres")

        # Generate with custom Snowflake dialect
        converted = parsed.sql(dialect=FixedSnowflake, pretty=True)

        return converted
    except Exception as e:
        sys.stderr.write(f"[Error] Failed to convert SQL: {e}\n")
        return sql

def convert_unnest_array_to_values(sql: str) -> str:
    """
    Replace SELECT unnest(ARRAY[...]) col with SELECT col FROM (VALUES (...)) AS t(col)
    Handles both single and multi-line arrays.
    """
    pattern = re.compile(r"SELECT\s+unnest\s*\(\s*ARRAY\s*\[(.*?)\]\s*\)\s+(\w+)", re.DOTALL | re.IGNORECASE)

    def repl(match):
        array_content = match.group(1)
        col = match.group(2)
        # Split array elements, handle both quoted and unquoted
        elements = re.findall(r"'[^']*'|\"[^\"]*\"|\S+", array_content)
        # Clean up quotes and whitespace, ignore empty
        values = ",\n    ".join(f"({e.strip()})" for e in elements if e.strip() and e.strip() != '()')
        return f"SELECT\n    {col}\n  FROM (VALUES\n    {values}\n  ) AS t({col})"

    return pattern.sub(repl, sql)

def handle_crosstab(sql: str) -> str:
    """
    Replace crosstab block with a dbt-compatible crosstab SQL using parse_crosstab_sql.
    """

    # Remove all -- comments before parsing
    sql_no_comments = re.sub(r'--[^\n]*', '', sql)
    print("Crosstab function detected, converting to dbt-compatible SQL.")
    print(sql_no_comments)
    try:
        converted_sql = parse_crosstab_sql(sql_no_comments)
        print("Converted crosstab SQL:")
        print(converted_sql)
        return converted_sql
    except Exception as e:
        print(f"[WARNING] Error in parse_crosstab_sql: {e}")
        return "{# WARNING: crosstab() block could not be converted, skipped for dbt compile #}"

def convert_generate_series_to_snowflake(sql: str) -> str:
    """
    Replace FROM generate_series(start, end, step) AS s(a)
    with FROM TABLE(GENERATOR(ROWCOUNT => ...)) AS g, LATERAL (SELECT DATEADD(...) AS a) AS s
    Handles both date and numeric series.
    """
    def repl(match):
        start = match.group(1).strip()
        end = match.group(2).strip()
        step = match.group(3)
        alias = match.group(4) or "s"
        col = match.group(5) or "a"
        if step:
            step = step.strip()
        else:
            step = "INTERVAL '1 day'"
        # Detect INTERVAL steps
        interval_match = re.search(r"INTERVAL\s+'(\d+)\s+(\w+)'", step, re.IGNORECASE)
        if interval_match:
            step_value = interval_match.group(1)
            step_unit = interval_match.group(2).upper()
            rowcount = f"DATEDIFF({step_unit}, {start}, {end}) / {step_value} + 1"
            return f"FROM TABLE(GENERATOR(ROWCOUNT => {rowcount})) AS g, LATERAL (SELECT DATEADD({step_unit}, SEQ4() * {step_value}, {start}) AS {col}) AS {alias}"
        else:
            # Numeric series
            rowcount = f"(({end}) - ({start})) / ({step}) + 1"
            return f"FROM TABLE(GENERATOR(ROWCOUNT => {rowcount})) AS g, LATERAL (SELECT ({start}) + SEQ4() * ({step}) AS {col}) AS {alias}"
    # Regex: FROM generate_series(start, end, [step]) AS s(a)
    pattern = re.compile(r"FROM\s+generate_series\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*(?:,\s*([^\)]+))?\)\s+AS\s+(\w+)\s*\((\w+)\)", re.IGNORECASE)
    return pattern.sub(repl, sql)
