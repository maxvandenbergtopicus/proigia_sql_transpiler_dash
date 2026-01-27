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
        # Override TRANSFORMS to handle EXTRACT(YEAR FROM AGE(...)) pattern
        TRANSFORMS = {
            **Snowflake.Generator.TRANSFORMS,
            exp.Extract: lambda self, e: (
                self.anonymous_sql(e.expression)
                if e.this and e.this.this == "YEAR" and isinstance(e.expression, exp.Anonymous) and e.expression.this.upper() == "AGE"
                else Snowflake.Generator.TRANSFORMS[exp.Extract](self, e)
            ),
        }
        
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
        
        def anonymous_sql(self, expression: exp.Anonymous) -> str:
            """Handle AGE function conversion to DATEDIFF"""
            if expression.this.upper() == "AGE":
                args = expression.expressions
                if len(args) == 2:
                    # AGE(end, start) -> DATEDIFF(year, start, end)
                    return f"DATEDIFF(year, {self.sql(args[1])}, {self.sql(args[0])})"
                elif len(args) == 1:
                    # AGE(timestamp) -> DATEDIFF(year, timestamp, CURRENT_TIMESTAMP())
                    return f"DATEDIFF(year, {self.sql(args[0])}, CURRENT_TIMESTAMP())"
            
            # For other anonymous functions, use default behavior
            return super().anonymous_sql(expression)
        
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


def convert_postgres_escape_strings(sql: str) -> str:
    """
    Convert PostgreSQL escape strings E'...' to regular strings.
    In PostgreSQL, E'\\n' means newline, E'\\\\' means one backslash.
    For Snowflake, we need to preserve the actual escape sequences.
    """
    result = []
    i = 0
    
    while i < len(sql):
        # Look for E' or E"
        if i < len(sql) - 1 and sql[i].upper() == 'E' and sql[i+1] in ("'", '"'):
            quote_char = sql[i+1]
            result.append(quote_char)  # Remove the E, keep the quote
            i += 2  # Skip past E'
            
            # Process the string content
            while i < len(sql):
                if sql[i] == '\\' and i + 1 < len(sql):
                    # Handle escape sequences
                    next_char = sql[i+1]
                    if next_char == '\\':
                        # \\\\ in E'...' means one backslash
                        result.append('\\\\')  # Keep as double backslash for Snowflake
                        i += 2
                    elif next_char in ('n', 't', 'r'):
                        # \\n, \\t, \\r are escape sequences
                        result.append('\\')
                        result.append(next_char)
                        i += 2
                    else:
                        # Other escapes like \\. or \\1
                        result.append('\\')
                        result.append(next_char)
                        i += 2
                elif sql[i] == quote_char:
                    # Check if it's an escaped quote
                    if i + 1 < len(sql) and sql[i+1] == quote_char:
                        result.append(quote_char)
                        result.append(quote_char)
                        i += 2
                    else:
                        # End of string
                        result.append(quote_char)
                        i += 1
                        break
                else:
                    result.append(sql[i])
                    i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)

def convert_postgres_to_snowflake(sql: str, function_macros: list = None) -> str:
    """Convert SQL from PostgreSQL to Snowflake dialect using sqlglot."""
    logging.info(f"[convert_postgres_to_snowflake] Input SQL:\n{sql}")
    # Pre-process: Convert LIKE ANY(array) to Snowflake-compatible EXISTS(SELECT ... LIKE ...)
    if re.search(r'LIKE\s+ANY\s*\(', sql, re.IGNORECASE):
        logging.info("Converting LIKE ANY(array) to Snowflake EXISTS(SELECT ... LIKE ...)")
        sql = convert_like_any_to_exists(sql)
    try:
        # Pre-process: Convert PostgreSQL escape strings E'...' to regular strings with proper escaping
        if "E'" in sql or 'E"' in sql:
            logging.info("Converting PostgreSQL escape strings (E'...')")
            sql = convert_postgres_escape_strings(sql)
        
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
        
        # Pre-process: Replace string_to_array with SPLIT
        if 'string_to_array' in sql.lower():
            logging.info("Converting string_to_array to SPLIT")
            sql = re.sub(r'\bstring_to_array\b', 'SPLIT', sql, flags=re.IGNORECASE)
        
        # Pre-process: Remove MATERIALIZED keyword from CTEs (not supported in Snowflake)
        if 'materialized' in sql.lower():
            logging.info("Removing MATERIALIZED keyword from CTEs")
            # Remove AS MATERIALIZED from CTEs: WITH cte AS MATERIALIZED (...) -> WITH cte AS (...)
            sql = re.sub(r'\bAS\s+MATERIALIZED\b', 'AS', sql, flags=re.IGNORECASE)
        
        # Pre-process: Convert SIMILAR TO to RLIKE (Snowflake equivalent)
        if 'similar to' in sql.lower():
            logging.info("Converting SIMILAR TO to RLIKE")
            sql = re.sub(r'\bSIMILAR\s+TO\b', 'RLIKE', sql, flags=re.IGNORECASE)
        
        # Pre-process: Convert PostgreSQL regex match operator ~ to RLIKE
        if '~' in sql:
            logging.info("Converting PostgreSQL regex match operator ~ to RLIKE")
            # Replace ~ with RLIKE (but not ~* which is case-insensitive and handled separately)
            sql = re.sub(r'(\s)~(\s)', r'\1RLIKE\2', sql)
        
        # Pre-process: Convert ARRAY[...] to ARRAY_CONSTRUCT(...)
        if 'array[' in sql.lower():
            logging.info("Converting ARRAY[...] to ARRAY_CONSTRUCT(...)")
            sql = convert_array_to_array_construct(sql)
        
        # Pre-process: Remove PostgreSQL array type casts (::text[], ::varchar[], etc.)
        if '::' in sql and '[]' in sql:
            logging.info("Removing PostgreSQL array type casts")
            sql = re.sub(r'::(text|varchar|character varying|integer|int|bigint|smallint|numeric|float|double precision|boolean|date|timestamp)\[\]', '', sql, flags=re.IGNORECASE)
        
        # Pre-process: Convert PostgreSQL array overlap operator && to ARRAYS_OVERLAP
        if '&&' in sql:
            logging.info("Converting PostgreSQL array overlap operator && to ARRAYS_OVERLAP")
            sql = convert_array_overlap_to_snowflake(sql)
        
        # Pre-process: Convert PostgreSQL date arithmetic to Snowflake DATEADD
        sql = convert_date_arithmetic_to_snowflake(sql)
        # Pre-process: Handle crosstab function (not supported in Snowflake)
        if re.search(r'\bcrosstab\s*\(', sql, re.IGNORECASE):
            sql = handle_crosstab(sql)

        # Pre-process: Convert unnest(ARRAY[...]) to SELECT ... FROM VALUES (...)
        sql = convert_unnest_array_to_values(sql)

        # Pre-process: Convert generate_series to Snowflake-compatible TABLE(GENERATOR(...))
        sql = convert_generate_series_to_snowflake(sql)
        
        # Parse with PostgreSQL dialect
        parsed = sqlglot.parse_one(sql, read="postgres")

        # Generate with custom Snowflake dialect
        converted = parsed.sql(dialect=FixedSnowflake, pretty=True)
    
        # Post-process: Convert DISTINCT ON to Snowflake-compatible syntax
        if 'distinct on' in sql.lower():
            logging.info("Converting DISTINCT ON to Snowflake-compatible syntax")
            converted = convert_distinct_on_to_snowflake(converted)
            
        # Post-process: Fix ARRAY_AGG(IFF(NOT x IS NULL, DISTINCT x, NULL)) to ARRAY_AGG(DISTINCT x)
        converted = re.sub(
            r"ARRAY_AGG\(\s*IFF\(\s*NOT\s+([a-zA-Z0-9_]+)\s+IS\s+NULL\s*,\s*DISTINCT\s+\1\s*,\s*NULL\s*\)\s*\)",
            r"ARRAY_AGG(DISTINCT \1)",
            converted,
            flags=re.IGNORECASE | re.DOTALL
        )
        logging.info(f"[convert_postgres_to_snowflake] Output SQL:\n{converted}")
        return converted
    except Exception as e:
        sys.stderr.write(f"[Error] Failed to convert SQL: {e}\n")
        # Even on error, try to convert DISTINCT ON if present
        if 'distinct on' in sql.lower():
            logging.info("Converting DISTINCT ON to Snowflake-compatible syntax on error")
            sql = convert_distinct_on_to_snowflake(sql)
        return sql

def convert_array_overlap_to_snowflake(sql: str) -> str:
    """
    Convert PostgreSQL array overlap operator && to Snowflake ARRAYS_OVERLAP function.
    Handles patterns like: array1::int[] && ARRAY[...] -> ARRAYS_OVERLAP(array1, ARRAY_CONSTRUCT(...))
    """
    # Remove ::type[] casts before && operator, then replace && with function call
    # Match: anything && ARRAY[...]
    def repl(match):
        left = match.group(1).strip()
        right = match.group(2).strip()
        # Remove ::type[] from left side
        left = re.sub(r'::\w+\[\]\s*$', '', left).strip()
        # Convert ARRAY[...] to ARRAY_CONSTRUCT(...) on right side
        right = convert_array_to_array_construct(right)
        return f"ARRAYS_OVERLAP({left}, {right})"
    
    # Pattern: capture left side (up to &&) and right side (ARRAY[...])
    return re.sub(r'([\w_]+\([^)]*(?:\([^)]*\))*[^)]*\)|[\w_]+)(?:::\w+\[\])?\s*&&\s*(ARRAY_CONSTRUCT\([^)]+\)|ARRAY\[[^\]]+\])', 
                  repl, sql, flags=re.IGNORECASE)

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

def convert_like_any_to_exists(sql: str) -> str:
    """
    Convert 'col LIKE ANY(array)' to Snowflake-compatible EXISTS(SELECT 1 FROM TABLE(FLATTEN(input => array)) f WHERE col LIKE f.value)
    Handles both qualified and unqualified column/array names.
    """
    # Pattern: <expr> LIKE ANY(<array_expr>)
    pattern = re.compile(r'(\b[\w\.]+\b)\s+LIKE\s+ANY\s*\(([^\)]+)\)', re.IGNORECASE)
    def repl(match):
        col = match.group(1).strip()
        arr = match.group(2).strip()
        return f"EXISTS (SELECT 1 FROM TABLE(FLATTEN(input => {arr})) f WHERE {col} LIKE f.value)"
    return pattern.sub(repl, sql)

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

def convert_max_array_to_array_agg(sql: str) -> str:
    """
    Convert MAX(CASE WHEN ... THEN ARRAY_CONSTRUCT(...) ELSE NULL END) to ARRAY_AGG with WITHIN GROUP.
    Snowflake doesn't support MAX() on arrays, so we need to use ARRAY_AGG with ordering.
    
    Transforms:
      MAX(CASE WHEN condition THEN ARRAY_CONSTRUCT(col1, col2, col3) ELSE NULL END)
    To:
      ARRAY_AGG(CASE WHEN condition THEN ARRAY_CONSTRUCT(col1, col2, col3) ELSE NULL END) 
        WITHIN GROUP (ORDER BY col1 DESC)[0]
    
    The ORDER BY uses the first column in ARRAY_CONSTRUCT to determine which row to pick.
    """
    # Pattern to match MAX(CASE WHEN ... THEN ARRAY_CONSTRUCT(...) ...)
    # We need to extract the CASE expression and the first column from ARRAY_CONSTRUCT
    pattern = re.compile(
        r'\bMAX\s*\(\s*'  # MAX(
        r'(CASE\s+WHEN\s+.*?'  # CASE WHEN ... (capture group 1 - start of case)
        r'THEN\s+'  # THEN
        r'ARRAY_CONSTRUCT\s*\(\s*'  # ARRAY_CONSTRUCT(
        r'(?:CAST\s*\(\s*)?'  # Optional CAST(
        r'(\w+(?:\.\w+)?)'  # First column name (capture group 2) - may have table prefix
        r'(?:\s+AS\s+\w+\s*\))?'  # Optional AS type) for CAST
        r'[^)]*'  # Rest of array construct args
        r'\)'  # Close ARRAY_CONSTRUCT
        r'.*?'  # Rest of CASE (ELSE clause, etc.)
        r'END)'  # END of CASE (capture group 1 - end)
        r'\s*\)',  # Close MAX
        re.IGNORECASE | re.DOTALL
    )
    
    def repl(match):
        case_expr = match.group(1).strip()
        order_by_col = match.group(2).strip()
        
        # Build the ARRAY_AGG replacement
        return (f"ARRAY_AGG({case_expr}) "
                f"WITHIN GROUP (ORDER BY {order_by_col} DESC)[0]")
    
    return pattern.sub(repl, sql)

def convert_date_arithmetic_to_snowflake(sql: str) -> str:
    """
    Convert PostgreSQL date arithmetic to Snowflake DATEADD syntax.
    Handles patterns like:
    - '${peildatum}'::date-'1 year'::interval -> DATEADD(YEAR, -1, '${peildatum}'::date)
    - column::date-'3 months'::interval -> DATEADD(MONTH, -3, column::date)
    - date_col-'2 days'::interval -> DATEADD(DAY, -2, date_col)
    - voorschrijfdatum+round(hoeveelheid/0.5)*interval'1 day' -> DATEADD(DAY, round(hoeveelheid/0.5), voorschrijfdatum)
    - date::date - validtime (where validtime is integer months) -> DATEADD(MONTH, -validtime, date::date)
    """
    
    # Pattern 1: column + expression * interval 'N unit' or column - expression * interval 'N unit'
    # Also handles: column + interval 'N unit' * expression
    pattern1 = re.compile(
        r"""
        (\w+(?:::\w+)?)  # base column (with optional ::type cast)
        \s*([-+])\s*  # operator (+ or -)
        (?:  # non-capturing group for two variations
        (.*?)\s*\*\s*interval\s*'([\d\.]+)\s+(year|month|week|day|hour|minute|second)s?'  # expr * interval 'N unit'
        |  # OR
        interval\s*'([\d\.]+)\s+(year|month|week|day|hour|minute|second)s?'\s*\*\s*(.*?)  # interval 'N unit' * expr
        )
        """,
        re.IGNORECASE | re.VERBOSE
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
    
    # Pattern 2a: Function calls or parenthesized expressions + 'N unit'::interval
    # Example: COALESCE(col1, col2) + '1 day'::interval
    # This needs to handle nested parentheses properly
    def find_matching_paren(text, start):
        """Find the closing parenthesis for an opening paren at start position"""
        count = 1
        i = start + 1
        while i < len(text) and count > 0:
            if text[i] == '(':
                count += 1
            elif text[i] == ')':
                count -= 1
            i += 1
        return i if count == 0 else -1
    
    # Pattern 2a: Match function_name(...) + 'N unit'::interval
    pattern2a = re.compile(
        r"\b(\w+)\s*\("  # function name + opening paren
        r"|"  # OR
        r"\("  # just an opening paren for grouped expressions
    )
    
    def repl2a(sql_text):
        """Process function calls and parenthesized expressions with interval arithmetic"""
        result = []
        i = 0
        while i < len(sql_text):
            # Look for function call or parenthesized expression
            match = pattern2a.search(sql_text, i)
            if not match:
                result.append(sql_text[i:])
                break
            
            # Add everything before the match
            result.append(sql_text[i:match.start()])
            
            # Find the closing paren
            paren_start = match.end() - 1
            paren_end = find_matching_paren(sql_text, paren_start)
            
            if paren_end == -1:
                # Couldn't find matching paren, just add the match and continue
                result.append(match.group(0))
                i = match.end()
                continue
            
            # Extract the full expression (function call or grouped expr)
            if match.group(1):  # Function call
                base_expr = sql_text[match.start():paren_end]
            else:  # Just parentheses
                base_expr = sql_text[match.start():paren_end]
            
            # Check if followed by +/- interval
            interval_match = re.match(
                r"\s*([-+])\s*'([\d\.]+)\s+(year|month|week|day|hour|minute|second)s?'::interval",
                sql_text[paren_end:],
                re.IGNORECASE
            )
            
            if interval_match:
                operator = interval_match.group(1)
                amount = interval_match.group(2)
                unit = interval_match.group(3).upper()
                
                # Convert to DATEADD
                amount_val = f"-{amount}" if operator == '-' else amount
                result.append(f"DATEADD({unit}, {amount_val}, {base_expr})")
                i = paren_end + interval_match.end()
            else:
                result.append(base_expr)
                i = paren_end
        
        return ''.join(result)
    
    sql = repl2a(sql)
    
    # Pattern 2b: Simple column or literal + 'N unit'::interval
    # Capture: base expression, operator (+ or -), number, unit
    pattern2b = re.compile(
        r"(\w+(?:::\w+)?|'[^']+'::\w+)"  # simple column or literal with cast
        r"\s*([-+])\s*"  # operator (+ or -)
        r"'([\d\.]+)\s+(year|month|week|day|hour|minute|second)s?'::interval",  # interval
        re.IGNORECASE
    )
    
    def repl2b(match):
        base = match.group(1)
        operator = match.group(2)
        amount = match.group(3)
        unit = match.group(4).upper()
        
        # Convert operator and amount
        if operator == '-':
            amount_val = f"-{amount}"
        else:
            amount_val = amount
        
        # Create DATEADD
        return f"DATEADD({unit}, {amount_val}, {base})"
    
    # Apply the pattern
    sql = pattern2b.sub(repl2b, sql)
    
    # Pattern 3: date + concat(..., ' unit')::interval (dynamic interval from string concat)
    # Example: '1900-01-01'::date + concat(cast(value as varchar), ' day')::interval
    pattern3 = re.compile(
        r"(\([^)]+\)(?:::\w+)?|'[^']+'::\w+|\w+(?:::\w+)?)"  # base: parenthesized expr, string literal, or column (with optional cast)
        r"\s*([-+])\s*"  # operator
        r"concat\s*\(([^)]+),\s*'[^']*\s*(year|month|week|day|hour|minute|second)s?[^']*'\s*\)::interval",
        re.IGNORECASE
    )
    
    def repl3(match):
        base = match.group(1).strip()
        operator = match.group(2)
        amount_expr = match.group(3).strip()
        unit = match.group(4).upper()
        
        # Apply operator to amount
        if operator == '-':
            amount_expr = f"-({amount_expr})"
        
        return f"DATEADD({unit}, {amount_expr}, {base})"
    
    sql = pattern3.sub(repl3, sql)
    
    # Pattern 4 (run LAST): Edge case for integer columns representing months (validtime, loopduur, etc.)
    # Handles: date_expr - table.column or date_expr + table.column
    # where column is validtime, loopduur, looptijd (integer months)
    # This pattern runs after all interval conversions so it can handle DATEADD results
    pattern4 = re.compile(
        r"(DATEADD\([^)]+\)|"  # DATEADD expression
        r"\([^)]+\)(?:::\w+)?|"  # parenthesized expr with optional cast
        r"'[^']+'::\w+|"  # string literal with cast
        r"\w+(?:::\w+)?)"  # column with optional cast
        r"\s*([-+])\s*"  # operator (+ or -)
        r"(\w+\.)?(validtime|loopduur|looptijd)\b",  # optional table prefix + month column name
        re.IGNORECASE
    )
    
    def repl4(match):
        base = match.group(1).strip()
        operator = match.group(2)
        table_prefix = match.group(3) if match.group(3) else ''
        column_name = match.group(4)
        
        # Build the full column reference
        full_column = f"{table_prefix}{column_name}"
        
        # Apply operator to amount
        if operator == '-':
            amount_expr = f"-{full_column}"
        else:
            amount_expr = full_column
        
        # Snowflake DATEADD with MONTH hardcoded
        return f"DATEADD(MONTH, {amount_expr}, {base})"

    # Pattern for base_expr [+|-] 'N unit'::interval (decimal, plural/singular)
    pattern_new = re.compile(
        r"([\w\(\)\{\}\$'\":, ]+)"  # base expression (function, cast, etc.)
        r"\s*([-+])\s*"
        r"'([\d\.]+)\s*(year|month|week|day|hour|minute|second)s?'::interval",
        re.IGNORECASE
    )

    def repl_new(match):
        base = match.group(1).strip()
        operator = match.group(2)
        amount = match.group(3)
        unit = match.group(4).upper()
        amount_val = f"-{amount}" if operator == '-' else amount
        return f"DATEADD({unit}, {amount_val}, {base})"

    sql = pattern_new.sub(repl_new, sql)
    
    return sql

def convert_distinct_on_to_snowflake(sql: str) -> str:
    """
    Convert PostgreSQL DISTINCT ON to Snowflake-compatible syntax using QUALIFY.
    DISTINCT ON (col1, col2, ...) is equivalent to keeping only the first row
    for each unique combination of (col1, col2, ...) in the specified ORDER BY.

    Example:
    SELECT DISTINCT ON (a, b) a, b, c FROM table ORDER BY a, b, c
    ->
    SELECT a, b, c FROM table QUALIFY ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY a, b, c) = 1
    """
    # Find DISTINCT ON pattern
    distinct_pattern = re.compile(r'DISTINCT\s+ON\s*\(\s*([^)]+)\s*\)', re.IGNORECASE)

    match = distinct_pattern.search(sql)
    if not match:
        return sql  # No DISTINCT ON found, return unchanged

    distinct_cols = match.group(1).strip()
    distinct_start = match.start()
    distinct_end = match.end()

    # Find the ORDER BY clause after the DISTINCT ON
    sql_after_distinct = sql[distinct_end:]
    order_match = re.search(r'ORDER\s+BY\s+(.+?)(?=\s*(?:GROUP\s+BY|HAVING|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|\)|;|$))', sql_after_distinct, re.IGNORECASE | re.DOTALL)

    # Build QUALIFY clause
    qualify_clause = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {distinct_cols}"
    if order_match:
        order_content = order_match.group(1).strip()
        qualify_clause += f" ORDER BY {order_content}"
    qualify_clause += ") = 1"

    # Remove DISTINCT ON
    sql = sql[:distinct_start] + sql[distinct_end:]

    # Insert QUALIFY before ORDER BY if present, else at the end
    if order_match:
        # Find ORDER BY in the modified sql
        order_start = sql.find('ORDER BY', distinct_start)
        if order_start != -1:
            insert_pos = order_start
        else:
            insert_pos = len(sql)
    else:
        insert_pos = len(sql)

    # Insert QUALIFY
    sql = sql[:insert_pos] + f"{qualify_clause}\n" + sql[insert_pos:]

    return sql