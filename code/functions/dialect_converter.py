import sys
import re
import logging
import sqlglot
from sqlglot import exp
from sqlglot.dialects.snowflake import Snowflake

from crosstabs.crosstabs_new import parse_crosstab_sql

# Suppress sqlglot's verbose debug output
logging.getLogger("sqlglot").setLevel(logging.WARNING)


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
                        # Convert plural to singular (days -> day, months -> month, years -> year)
                        unit = parts[1].upper().rstrip('S')
                        return f"INTERVAL '{parts[0]}' {unit}"
                    elif len(parts) == 1:
                        # If only number, assume days
                        return f"INTERVAL '{parts[0]}' DAY"
            
            # For all other casts, use default behavior
            return super().cast_sql(expression)
          
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
        
        def similarto_sql(self, expression: exp.SimilarTo) -> str:
            """Convert PostgreSQL SIMILAR TO to Snowflake RLIKE"""
            return f"{self.sql(expression.this)} RLIKE {self.sql(expression.expression)}"

        def interval_sql(self, expression: exp.Interval) -> str:
            """Convert PostgreSQL INTERVAL to Snowflake format"""
            # Get the value and unit
            value = self.sql(expression.this)
            unit = expression.unit
            
            if unit:
                # Snowflake format: INTERVAL '6' MONTH
                # Remove any plural 's' from unit (MONTHS -> MONTH)
                unit_str = str(unit).upper().rstrip('S')
                return f"INTERVAL {value} {unit_str}"
            
            # Fallback to default behavior
            return super().interval_sql(expression)

        def function_sql(self, expression: exp.Func) -> str:
            """Translate PostgreSQL functions into Snowflake equivalents"""
            
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
        # Pre-process: Replace citext with varchar (case-insensitive text type)
        if 'citext' in sql.lower():
            logging.info("Converting citext to VARCHAR")
            sql = re.sub(r'\bcitext\b', 'VARCHAR', sql, flags=re.IGNORECASE)
        
        # Pre-process: Replace array_accum with array_agg
        if 'array_accum' in sql.lower():
            count = sql.lower().count('array_accum')
            logging.info(f"Converting {count} occurrence(s) of array_accum to array_agg")
            sql = sql.replace('array_accum', 'array_agg')
            sql = sql.replace('ARRAY_ACCUM', 'ARRAY_AGG')
        
        # Pre-process: Remove MATERIALIZED keyword from CTEs (not supported in Snowflake)
        if 'materialized' in sql.lower():
            logging.info("Removing MATERIALIZED keyword from CTEs")
            # Remove AS MATERIALIZED from CTEs: WITH cte AS MATERIALIZED (...) -> WITH cte AS (...)
            sql = re.sub(r'\bAS\s+MATERIALIZED\s*\(', 'AS (', sql, flags=re.IGNORECASE)
        
        # Pre-process: Convert ARRAY[...] to ARRAY_CONSTRUCT(...)
        if 'array[' in sql.lower():
            logging.info("Converting ARRAY[...] to ARRAY_CONSTRUCT(...)")
            sql = convert_array_to_array_construct(sql)
        
        # Pre-process: Remove PostgreSQL array type casts (::text[], ::varchar[], etc.)
        if '::' in sql and '[]' in sql:
            logging.info("Removing PostgreSQL array type casts")
            sql = re.sub(r'::(text|varchar|character varying|integer|int|bigint|smallint|numeric|float|double precision|boolean|date|timestamp)\[\]', '', sql, flags=re.IGNORECASE)
        
        # Pre-process: Handle crosstab function (not supported in Snowflake)
        if 'crosstab' in sql.lower():
            sql = handle_crosstab(sql)

        # Pre-process: Convert unnest(ARRAY[...]) to SELECT ... FROM VALUES (...)
        sql = convert_unnest_array_to_values(sql)

        # Pre-process: Convert generate_series to Snowflake-compatible TABLE(GENERATOR(...))
        sql = convert_generate_series_to_snowflake(sql)
        
        # Pre-process: Convert PostgreSQL date arithmetic to Snowflake DATEADD
        sql = convert_date_arithmetic_to_snowflake(sql)

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
    # Match pattern with optional AS keyword: unnest(ARRAY[...]) [AS] col_name
    pattern = re.compile(r"SELECT\s+unnest\s*\(\s*ARRAY\s*\[(.*?)\]\s*\)\s+(?:AS\s+)?(\w+)", re.DOTALL | re.IGNORECASE)

    def repl(match):
        array_content = match.group(1)
        col = match.group(2)
        
        # Split by commas, but respect quoted strings
        elements = []
        current = []
        in_quotes = False
        quote_char = None
        
        for char in array_content:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
                current.append(char)
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current.append(char)
            elif char == ',' and not in_quotes:
                element = ''.join(current).strip()
                if element:  # Only add non-empty elements
                    elements.append(element)
                current = []
            else:
                current.append(char)
        
        # Add the last element
        element = ''.join(current).strip()
        if element:
            elements.append(element)
        
        # Create VALUES clause
        values = ",\n    ".join(f"({e})" for e in elements)
        return f"SELECT\n    {col}\n  FROM (VALUES\n    {values}\n  ) AS t({col})"

    return pattern.sub(repl, sql)

def convert_array_to_array_construct(sql: str) -> str:
    """
    Convert PostgreSQL ARRAY[...] syntax to Snowflake ARRAY_CONSTRUCT(...).
    Handles multi-line arrays and nested brackets/parentheses.
    """
    result = []
    i = 0
    
    while i < len(sql):
        # Look for ARRAY[ (case-insensitive)
        if sql[i:i+6].upper() == 'ARRAY[':
            # Found ARRAY[, now find the matching closing bracket
            result.append('ARRAY_CONSTRUCT(')
            i += 6  # Skip past 'ARRAY['
            
            # Track bracket depth to handle nested arrays
            bracket_depth = 1
            paren_depth = 0
            in_string = False
            string_char = None
            
            while i < len(sql) and bracket_depth > 0:
                char = sql[i]
                
                # Handle string literals
                if char in ('"', "'") and (i == 0 or sql[i-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                
                # Only count brackets/parens outside of strings
                if not in_string:
                    if char == '[':
                        bracket_depth += 1
                    elif char == ']':
                        bracket_depth -= 1
                        if bracket_depth == 0:
                            # Found the closing bracket for this ARRAY
                            result.append(')')
                            i += 1
                            continue
                    elif char == '(':
                        paren_depth += 1
                    elif char == ')':
                        paren_depth -= 1
                
                result.append(char)
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)

def handle_crosstab(sql: str) -> str:
    """
    Replace crosstab block with a dbt-compatible crosstab SQL using parse_crosstab_sql.
    """

    # Remove CREATE [MATERIALIZED] VIEW ... AS prefix if present
    sql_clean = re.sub(
        r'CREATE\s+(MATERIALIZED\s+)?VIEW\s+\w+\s+AS\s*',
        '',
        sql,
        count=1,
        flags=re.IGNORECASE
    )
    
    # Remove all -- comments before parsing
    sql_no_comments = re.sub(r'--[^\n]*', '', sql_clean)
    logging.info("Crosstab function detected, converting to dbt-compatible SQL.")
    try:
        converted_sql = parse_crosstab_sql(sql_no_comments)
        return converted_sql
    except Exception as e:
        logging.warning(f"Error in parse_crosstab_sql: {e}")
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

def convert_date_arithmetic_to_snowflake(sql: str) -> str:
    """
    Convert PostgreSQL date arithmetic to Snowflake DATEADD syntax.
    Handles patterns like:
    - '${peildatum}'::date-'1 year'::interval -> DATEADD(YEAR, -1, '${peildatum}'::date)
    - column::date-'3 months'::interval -> DATEADD(MONTH, -3, column::date)
    - date_col-'2 days'::interval -> DATEADD(DAY, -2, date_col)
    - voorschrijfdatum+round(hoeveelheid/0.5)*interval'1 day' -> DATEADD(DAY, round(hoeveelheid/0.5), voorschrijfdatum)
    """
    # Pattern 1: column + expression * interval 'N unit' or column - expression * interval 'N unit'
    # Also handles: column + interval 'N unit' * expression
    pattern1 = re.compile(
        r"(\w+(?:::\w+)?)"  # base column (with optional ::type cast)
        r"\s*([-+])\s*"  # operator (+ or -)
        r"(?:"  # non-capturing group for two variations
        r"(.*?)\s*\*\s*interval\s*'(\d+)\s+(year|month|day|hour|minute|second)s?'"  # expr * interval 'N unit'
        r"|"  # OR
        r"interval\s*'(\d+)\s+(year|month|day|hour|minute|second)s?'\s*\*\s*(.*?)"  # interval 'N unit' * expr
        r")",
        re.IGNORECASE
    )
    
    def repl1(match):
        base = match.group(1)
        operator = match.group(2)
        
        # Check which pattern matched (expr * interval or interval * expr)
        if match.group(3) is not None:  # expr * interval 'N unit'
            expr = match.group(3).strip()
            amount_literal = match.group(4)
            unit = match.group(5).upper()
        else:  # interval 'N unit' * expr
            amount_literal = match.group(6)
            unit = match.group(7).upper()
            expr = match.group(8).strip()
        
        # Build the expression: if amount is not 1, multiply it with the expression
        if amount_literal == '1':
            amount_expr = expr
        else:
            amount_expr = f"{amount_literal} * ({expr})"
        
        # Apply operator
        if operator == '-':
            amount_expr = f"-({amount_expr})"
        
        # Snowflake DATEADD syntax: DATEADD(unit, amount, base)
        return f"DATEADD({unit}, {amount_expr}, {base})"
    
    sql = pattern1.sub(repl1, sql)
    
    # Pattern 2: (expression)::date - 'N unit'::interval or (expression)::date + 'N unit'::interval
    # Capture: base expression, operator (+ or -), number, unit
    pattern = re.compile(
        r"(['\"]?\$?\{?[\w]+\}?['\"]?::date|\w+::date|\w+)"  # base expression (with or without ::date)
        r"\s*([-+])\s*"  # operator (+ or -)
        r"'(\d+)\s+(year|month|day|hour|minute|second)s?'::interval",  # interval
        re.IGNORECASE
    )
    
    def repl(match):
        base = match.group(1)
        operator = match.group(2)
        amount = match.group(3)
        unit = match.group(4).upper()
        
        # Convert operator and amount
        if operator == '-':
            amount_val = f"-{amount}"
        else:
            amount_val = amount
        
        # Snowflake DATEADD syntax: DATEADD(unit, amount, base)
        return f"DATEADD({unit}, {amount_val}, {base})"
    
    return pattern.sub(repl, sql)