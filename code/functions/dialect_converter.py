import sys
import re
import logging
import sqlglot
from sqlglot import exp
from sqlglot.dialects.snowflake import Snowflake

from crosstabs.crosstabs_new import parse_crosstab_sql

# ---- Timezone Abbreviation to IANA Timezone Mapping ----
# Snowflake requires IANA timezone names, not abbreviations
TIMEZONE_ABBREVIATION_MAP = {
    'CEST': 'Europe/Amsterdam',  # Central European Summer Time
    'CET': 'Europe/Amsterdam',    # Central European Time
    'EST': 'America/New_York',    # Eastern Standard Time
    'EDT': 'America/New_York',    # Eastern Daylight Time
    'PST': 'America/Los_Angeles', # Pacific Standard Time
    'PDT': 'America/Los_Angeles', # Pacific Daylight Time
    'MST': 'America/Denver',      # Mountain Standard Time
    'MDT': 'America/Denver',      # Mountain Daylight Time
    'CST': 'America/Chicago',     # Central Standard Time (North America)
    'CDT': 'America/Chicago',     # Central Daylight Time
    'UTC': 'UTC',                 # Coordinated Universal Time
    'GMT': 'UTC',                 # Greenwich Mean Time
}

# ---- Custom Dialect Definition ----
class FixedSnowflake(Snowflake):
    class Generator(Snowflake.Generator):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Snowflake SQL string literals escape apostrophes by doubling them.
            # sqlglot's default Snowflake generator uses the tokenizer escape char,
            # which renders inner quotes as \' in output SQL.
            self._escaped_quote_end = self.dialect.QUOTE_END * 2

        # Override TRANSFORMS to handle EXTRACT(YEAR/MONTH FROM AGE(...)) pattern and ArrayOverlaps
        TRANSFORMS = {
            **Snowflake.Generator.TRANSFORMS,
            exp.Extract: lambda self, e: (
                self.extract_from_age_sql(e)
                if e.this and e.this.this in ("YEAR", "MONTH", "DAY") and isinstance(e.expression, exp.Anonymous) and e.expression.this.upper() == "AGE"
                else Snowflake.Generator.TRANSFORMS[exp.Extract](self, e)
            ),
            exp.ArrayOverlaps: lambda self, e: self.arrayoverlaps_sql(e),
            exp.DPipe: lambda self, e: self.dpipe_sql(e),
            exp.Substring: lambda self, e: self.substring_sql(e),
            exp.AtTimeZone: lambda self, e: self.attimezone_sql(e),
            exp.GenerateSeries: lambda self, e: self.generateseries_sql(e),
            exp.GenerateDateArray: lambda self, e: self.generateseries_sql(e),
            exp.GenerateTimestampArray: lambda self, e: self.generateseries_sql(e),
            # Override CONCAT to avoid sqlglot wrapping args with COALESCE(arg, ''),
            # which breaks numeric columns. PostgreSQL CONCAT ignores NULLs natively;
            # in Snowflake we accept the slight NULL behaviour difference.
            exp.Concat: lambda self, e: f"CONCAT({', '.join(self.sql(arg) for arg in e.expressions)})",

        }

        def extract_from_age_sql(self, expression: exp.Extract) -> str:
            """Handle EXTRACT(YEAR/MONTH/DAY FROM AGE(...)) conversions.

            Emulates Postgres age() decomposition exactly. age() computes the
            field-wise difference and, when day(end) < day(start), borrows one
            month using the number of days in the START date's month (see
            timestamp_age in the PG source). This differs from the previous
            date_diff_exact/DATEADD approach by 1-3 days around unequal month
            lengths, and by a whole month on month-end clamping edges
            (e.g. 2025-01-31 -> 2025-04-30 is '2 mon 30 days' in PG, not '3 mon').

            Emits the pg_age_part(unit, start, end) UDF, which must be deployed
            alongside date_diff_exact. Assumes end >= start, which holds for
            report period columns.
            """
            unit = expression.this.this.upper()
            age_expr = expression.expression
            args = age_expr.expressions

            if len(args) == 2:
                # AGE(end, start)
                start = self.sql(args[1])
                end = self.sql(args[0])
            elif len(args) == 1:
                # AGE(timestamp) -> compared against now
                start = self.sql(args[0])
                end = "CURRENT_DATE()"
            else:
                start = "NULL"
                end = "CURRENT_DATE()"

            return f"{{{{ function('pg_age_part') }}}}('{unit.lower()}', {start}, {end})"
        
        def substring_sql(self, expression: exp.Substring) -> str:
            """Convert PostgreSQL SUBSTRING(value FROM pattern) to Snowflake REGEXP_SUBSTR(value, pattern)"""
            # In sqlglot, SUBSTRING(value FROM pattern) is parsed with:
            # - this: the value
            # - start: the pattern (as a Literal string)
            # When FROM contains a regex pattern, it appears in args['start'] as a string literal
            
            value = self.sql(expression.this)
            args = expression.args
            
            # Check if 'start' exists and is a string literal (indicating regex pattern from FROM clause)
            if 'start' in args and args['start'] is not None:
                start_arg = args['start']
                # If start is a string literal, it's a regex pattern - use REGEXP_SUBSTR
                if isinstance(start_arg, exp.Literal) and start_arg.is_string:
                    pattern = self.sql(start_arg)
                    # PostgreSQL SUBSTRING(val FROM pattern) returns the text matched by the
                    # first parenthesised group. Snowflake equivalent:
                    #   REGEXP_SUBSTR(val, pattern, 1, 1, 'e', 1)
                    #   position=1, occurrence=1, 'e'=extract subgroup, group_num=1
                    return f"REGEXP_SUBSTR({value}, {pattern}, 1, 1, 'e', 1)"
                else:
                    # Numeric start position - use standard SUBSTRING
                    parts = [value, self.sql(start_arg)]
                    if 'length' in args and args['length'] is not None:
                        parts.append(self.sql(args['length']))
                    return f"SUBSTRING({', '.join(parts)})"
            
            # No start argument - just return value
            return f"SUBSTRING({value})"
        
        def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
            """
            Convert PostgreSQL AT TIME ZONE to Snowflake CONVERT_TIMEZONE.
            Maps timezone abbreviations to IANA timezone names.
            """
            # Get the timestamp expression and timezone
            timestamp_expr = self.sql(expression.this)
            zone = expression.args.get('zone')
            
            if zone:
                # Get the timezone string
                if isinstance(zone, exp.Literal):
                    tz_value = zone.this.strip("'\"").upper()
                    # Map abbreviation to IANA timezone if available
                    iana_tz = TIMEZONE_ABBREVIATION_MAP.get(tz_value, tz_value)
                    return f"CONVERT_TIMEZONE('{iana_tz}', {timestamp_expr})"
                else:
                    zone_sql = self.sql(zone)
                    return f"CONVERT_TIMEZONE({zone_sql}, {timestamp_expr})"
            
            # No zone specified - fallback
            return f"CONVERT_TIMEZONE('UTC', {timestamp_expr})"
          
        def array_sql(self, expression: exp.Array) -> str:
            """Convert ARRAY[] syntax to ARRAY_CONSTRUCT() for Snowflake"""
            expressions = self.expressions(expression, flat=True)
            return f"ARRAY_CONSTRUCT({expressions})"
        
        def arrayoverlaps_sql(self, expression: exp.ArrayOverlaps) -> str:
            """Convert array overlap to ARRAYS_OVERLAP function"""
            return f"ARRAYS_OVERLAP({self.sql(expression.this)}, {self.sql(expression.expression)})"
        
        def dpipe_sql(self, expression: exp.DPipe) -> str:
            """
            Convert || operator to appropriate Snowflake function:
            - For arrays: ARRAY_CAT(left, right)
            - For strings: Keep default || or CONCAT behavior
            """
            # Check if either side is an array-related expression
            left = expression.this
            right = expression.expression
            
            # Array-related expression types that indicate array concatenation
            array_types = (
                exp.Array, exp.ArrayAgg, exp.ArrayConcat, exp.ArrayConstructCompact,
                exp.ArrayFilter, exp.ArrayRemove, exp.ArraySlice, exp.ArraySort,
                exp.StringToArray, exp.ToArray
            )
            
            # Array-related function names (for Anonymous functions)
            array_function_names = {
                'ARRAY_CONSTRUCT', 'ARRAY_AGG', 'ARRAY_CONCAT', 'ARRAY_SLICE',
                'ARRAY_SORT', 'ARRAY_FILTER', 'ARRAY_REMOVE', 'STRING_TO_ARRAY',
                'SPLIT', 'FLATTEN'
            }
            
            # Helper function to check if an expression is array-related
            def is_array_expr(e):
                if isinstance(e, array_types):
                    return True
                # Check for Anonymous functions with array-related names
                if isinstance(e, exp.Anonymous) and e.this.upper() in array_function_names:
                    return True
                # Check nested DPipe for arrays
                if isinstance(e, exp.DPipe):
                    return is_array_expr(e.this) or is_array_expr(e.expression)
                return False
            
            # If either side is an array-related expression, use ARRAY_CAT
            if is_array_expr(left) or is_array_expr(right):
                return f"ARRAY_CAT({self.sql(left)}, {self.sql(right)})"
            
            # Default to string concatenation (|| or CONCAT)
            # Use the parent Snowflake dialect's default behavior
            return f"{self.sql(left)} || {self.sql(right)}"

        def generateseries_sql(self, expression: exp.GenerateSeries) -> str:
            """Convert PostgreSQL generate_series to Snowflake TABLE(GENERATOR(...))
            
            Note: This replaces the old duplicate implementation in function_sql().
            Sqlglot parses generate_series as exp.GenerateSeries/GenerateDateArray, handled via TRANSFORMS.
            """
            # Get arguments - try multiple access patterns depending on expression type
            # Different expression types store args differently
            expressions_list = getattr(expression, 'expressions', [])
            
            start_expr = (expression.this or 
                         expression.args.get('start') or
                         (expressions_list[0] if len(expressions_list) > 0 else None))
            end_expr = (expression.args.get('end') or 
                       (expressions_list[1] if len(expressions_list) > 1 else None))
            step_expr = (expression.args.get('step') or
                        (expressions_list[2] if len(expressions_list) > 2 else None))
            
            # Helper function to extract interval value from Cast or Interval expression
            def extract_interval_info(expr):
                """Extract (value, unit) from interval expression"""
                # Check if it's a Cast to INTERVAL type
                if isinstance(expr, exp.Cast):
                    if expr.to and expr.to.this == exp.DataType.Type.INTERVAL:
                        # Extract from the literal: e.g., '2 years' or '1 year'
                        if isinstance(expr.this, exp.Literal):
                            literal_value = expr.this.this.strip("'\"")
                            # Parse "N unit" pattern
                            match = re.match(r'(\d+)\s+(year|month|week|day|hour|minute|second)s?', literal_value, re.IGNORECASE)
                            if match:
                                return (match.group(1), match.group(2).upper())
                # Check if it's a direct Interval type
                elif isinstance(expr, exp.Interval):
                    if expr.this and isinstance(expr.this, exp.Literal):
                        value = expr.this.this.strip("'\"")
                        unit = None
                        if expr.unit:
                            unit = expr.unit.this if hasattr(expr.unit, 'this') else str(expr.unit)
                            unit = unit.upper().rstrip('S')
                        return (value, unit)
                return (None, None)
            
            # Determine if this is a date/interval series
            step_value, step_unit = extract_interval_info(step_expr) if step_expr else (None, None)
            is_interval_step = step_unit is not None
            
            if is_interval_step and step_unit:
                # ---- DATE SERIES ----
                # Try to extract static row count from start/end expressions
                # Pattern: start = base_date - N interval, end = base_date, step = M interval
                # Result: ROWCOUNT = (N / M) + 1
                
                rowcount = None
                base_start_expr = None
                offset_value_int = None
                
                # Check if start_expr is a subtraction with an interval
                if isinstance(start_expr, exp.Sub):
                    base_start_expr = start_expr.this
                    offset_expr = start_expr.expression
                    
                    # Extract interval info from the offset
                    offset_value, offset_unit = extract_interval_info(offset_expr)
                    
                    # Check if the interval unit matches the step unit
                    if offset_value and offset_unit == step_unit:
                        try:
                            offset_value_int = int(offset_value)
                            step_int = int(step_value)
                            # Calculate row count: (offset / step) + 1
                            rowcount = (offset_value_int // step_int) + 1
                        except (ValueError, ZeroDivisionError):
                            pass
                
                # A bare SEQ4() inside an uncorrelated LATERAL subquery does not step
                # per generator row (every row gets the same value), so the series
                # collapses to N identical dates. Assign a gap-free 0..N-1 index with
                # ROW_NUMBER() in a derived table instead.

                # If we successfully calculated a static rowcount, use it
                if rowcount is not None and base_start_expr is not None and offset_value_int is not None:
                    # Use the base expression (without the offset) as the actual start
                    # and adjust using the row index starting from negative offset
                    base_start = self.sql(base_start_expr)

                    return (
                        f"(SELECT DATEADD({step_unit}, -{offset_value_int} + ((ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * {step_value}), {base_start}) AS VALUE "
                        f"FROM TABLE(GENERATOR(ROWCOUNT => {rowcount})))"
                    )
                else:
                    # Fallback: Use dynamic DATEDIFF (may fail if not constant in Snowflake)
                    # Render start and end as SQL strings
                    start = self.sql(start_expr) if start_expr else "0"
                    end = self.sql(end_expr) if end_expr else "0"
                    rowcount_expr = f"DATEDIFF({step_unit}, {start}, {end}) / {step_value} + 1"
                    return (
                        f"(SELECT DATEADD({step_unit}, (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * {step_value}, {start}) AS VALUE "
                        f"FROM TABLE(GENERATOR(ROWCOUNT => {rowcount_expr})))"
                    )
            else:
                # ---- NUMERIC SERIES ----
                # For numeric sequences, generate ARRAY_GENERATE_RANGE
                # Post-processing will convert this to TABLE(FLATTEN(...)) for proper row generation
                start = self.sql(start_expr) if start_expr else "0"
                end = self.sql(end_expr) if end_expr else "0"
                step_sql = self.sql(step_expr) if step_expr else "1"
                # ARRAY_GENERATE_RANGE(start, stop, step) - stop is exclusive, so add step to end
                return f"ARRAY_GENERATE_RANGE({start}, ({end}) + ({step_sql}), {step_sql})"
        
        def anonymous_sql(self, expression: exp.Anonymous) -> str:
            """Handle function conversions (AGE, ARRAYS_OVERLAP, ARRAY_POSITION, CHAR_LENGTH, override_kwartaal, patient_kwartaal, get_hist_kwartaalindicatoren)"""
            func_name = expression.this.upper()
            args = expression.expressions

            if 1 == 2: #func_name == "OVERRIDE_KWARTAAL": 
                # Custom translation for override_kwartaal(arg)
                if len(args) == 1:
                    arg_sql = self.sql(args[0])
                    return f"override_patientenlijst WHERE jaar = YEAR(to_date({arg_sql})) AND kwartaal = QUARTER({arg_sql})"
                # Fallback if wrong number of arguments
                return "override_patientenlijst WHERE jaar = YEAR(to_date(arg)) AND kwartaal = QUARTER(arg)"

            if 1==2: #func_name == "PATIENT_KWARTAAL":
                # TODO: Implement custom translation for patient_kwartaal
                # Example placeholder:
                return "-- TODO: Implement custom translation for patient_kwartaal(arg)"

            if 1==2: #func_name == "GET_HIST_KWARTAALINDICATOREN":
                # TODO: Implement custom translation for get_hist_kwartaalindicatoren
                # Example placeholder:
                return "-- TODO: Implement custom translation for get_hist_kwartaalindicatoren(arg)"

            if func_name == "AGE":
                if len(args) == 2:
                    # AGE(end, start) -> {{ function('date_diff_exact') }}('year', start, end)
                    # Uses the exact age calculation UDF to match PostgreSQL behavior (includes months/days)
                    return f"{{{{ function('date_diff_exact') }}}}('year', {self.sql(args[1])}, {self.sql(args[0])})"
                elif len(args) == 1:
                    # AGE(timestamp) -> {{ function('date_diff_exact') }}('year', timestamp, CURRENT_TIMESTAMP())
                    # Uses the exact age calculation UDF to match PostgreSQL behavior
                    return f"{{{{ function('date_diff_exact') }}}}('year', {self.sql(args[0])}, CURRENT_TIMESTAMP())"

            if func_name == "ARRAYS_OVERLAP":
                return f"ARRAYS_OVERLAP({self.sql(args[0])}, {self.sql(args[1])})"

            if func_name == "ARRAY_POSITION":
                if len(args) == 2:
                    # PostgreSQL: ARRAY_POSITION(array, value) -> Snowflake: ARRAY_POSITION(value, array)
                    return f"ARRAY_POSITION({self.sql(args[1])}, {self.sql(args[0])})"

            if func_name == "CHAR_LENGTH":
                if len(args) == 1:
                    # PostgreSQL: CHAR_LENGTH(string) -> Snowflake: LENGTH(string)
                    return f"LENGTH({self.sql(args[0])})"

            # For other anonymous functions, use default behavior
            return super().anonymous_sql(expression)
        
        def eq_sql(self, expression: exp.EQ) -> str:
            """Convert 'value' = ANY(array) to ARRAY_CONTAINS(TO_VARIANT(value), array)"""
            # Check if right side is ANY
            if isinstance(expression.right, exp.Any):
                array_expr = expression.right.this
                value_expr = expression.left
                result = f"ARRAY_CONTAINS(TO_VARIANT({self.sql(value_expr)}), {self.sql(array_expr)})"
                return result
            
            # Default behavior for other equality expressions
            return super().eq_sql(expression)
        
        def similarto_sql(self, expression: exp.SimilarTo) -> str:
            """Convert PostgreSQL SIMILAR TO to Snowflake RLIKE with pattern translation"""
            def translate_similar_to_pattern(pattern: str) -> str:
                # Replace SIMILAR TO wildcards with regex equivalents
                translated = pattern.replace('%', '.*').replace('_', '.')
                # Add ^ at the beginning if not already present
                if not translated.startswith('^'):
                    translated = '^' + translated
                return translated
            
            pattern_expr = expression.expression
            if isinstance(pattern_expr, exp.Literal):
                pattern = pattern_expr.this
                translated_pattern = translate_similar_to_pattern(pattern)
                return f"{self.sql(expression.this)} RLIKE '{translated_pattern}'"
            else:
                # Fallback for non-literal patterns
                return f"{self.sql(expression.this)} RLIKE {self.sql(expression.expression)}"

        def collate_sql(self, expression: exp.Collate) -> str:
            """Convert PostgreSQL COLLATE "name" to Snowflake COLLATE(expr, 'name').

            PostgreSQL uses double-quoted identifiers for collation names (e.g. COLLATE "C").
            sqlglot parses the collation as an Identifier node and Snowflake's default generator
            re-emits those double quotes, producing COLLATE(col, "C") which is invalid in
            Snowflake. We unwrap the identifier and emit a proper single-quoted string literal.
            """
            collation = expression.args.get("collation") or expression.expression
            col_sql = self.sql(expression.this)
            if isinstance(collation, exp.Identifier):
                collation_name = collation.name
            elif isinstance(collation, exp.Literal):
                collation_name = collation.this
            else:
                collation_name = self.sql(collation).strip('"\'')
            if collation_name.upper() in ('C', 'POSIX'):
                return col_sql
            return f"COLLATE({col_sql}, '{collation_name}')"

        def cast_sql(self, expression: exp.Cast, safe_prefix=None) -> str:
            """
            Override cast_sql to suppress inline comments on the inner expression.
            sqlglot attaches column comments to the inner node and renders them inside
            CAST(col /* comment */ AS TYPE). We strip them here so that alias_sql can
            append them after the full alias: CAST(col AS TYPE) AS alias /* comment */
            """
            inner = expression.this
            comments = getattr(inner, 'comments', None)
            if comments:
                inner.comments = []
                result = super().cast_sql(expression, safe_prefix=safe_prefix)
                inner.comments = comments  # restore for alias_sql to pick up
                return result
            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def alias_sql(self, expression: exp.Alias) -> str:
            """
            Override alias_sql to append inline comments after the alias.
            When the aliased expression is a Cast whose inner node carries comments,
            those comments are stripped by cast_sql and collected here so they appear
            at the end: CAST(col AS TYPE) AS alias /* comment */
            """
            # Collect comments from Cast inner expression if present
            cast_node = expression.this if isinstance(expression.this, exp.Cast) else None
            inner_comments = []
            if cast_node is not None:
                inner = cast_node.this
                inner_comments = getattr(inner, 'comments', None) or []

            if inner_comments:
                # Temporarily clear so cast_sql doesn't re-render them
                cast_node.this.comments = []
                result = super().alias_sql(expression)
                cast_node.this.comments = inner_comments  # restore
                rendered = ' '.join(f'/* {c.strip()} */' for c in inner_comments if c.strip())
                return f"{result} {rendered}"
            return super().alias_sql(expression)

        def select_sql(self, expression: exp.Select) -> str:
            """
            Move inline comments from before the trailing comma to after it.
            alias_sql places comments after the alias but before the separator comma,
            giving "AS alias /* comment */,". This swaps it to "AS alias, /* comment */".
            """
            result = super().select_sql(expression)
            # Single-line block comments only (. does not match \n without DOTALL)
            result = re.sub(r'[ \t]*(/\*.*?\*/),', r', \1', result)
            return result

        def not_sql(self, expression: exp.Not) -> str:
            """Handle NOT expressions, specifically NOT IN -> ... NOT IN ..."""
            # Check if this is NOT (expression IN (...))
            if isinstance(expression.this, exp.In):
                # Generate the IN expression first
                in_sql = self.sql(expression.this)
                # Replace "NOT (expr IN ...)" with "expr NOT IN ..."
                # But since we're in not_sql, we need to generate expr NOT IN ...
                in_expr = expression.this
                left = self.sql(in_expr.this)
                # Get the IN part after the expression
                in_part = in_sql[len(self.sql(in_expr.this)):].strip()
                result = f"{left} NOT {in_part}"
                return result
            
            # Default behavior for other NOT expressions
            return f"NOT {self.sql(expression.this)}"


def _has_top_level_order_by(text: str) -> bool:
    """Return True if *text* contains ORDER BY at paren-depth 0."""
    depth = 0
    in_str = False
    str_char = ''
    n = len(text)
    idx = 0
    while idx < n:
        ch = text[idx]
        if in_str:
            if ch == str_char and (idx == 0 or text[idx - 1] != '\\'):
                in_str = False
        elif ch in ("'", '"'):
            in_str = True
            str_char = ch
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and text[idx:idx + 8].upper() == 'ORDER BY':
            prev = text[idx - 1] if idx > 0 else ' '
            nxt = text[idx + 8] if idx + 8 < n else ' '
            if not (prev.isalnum() or prev == '_') and not (nxt.isalnum() or nxt == '_'):
                return True
        idx += 1
    return False


def add_order_by_to_array_agg(sql: str) -> str:
    """
    Inject ORDER BY into ARRAY_AGG calls that lack one, but only when the
    expression is simple enough that repeating it as an ORDER BY key is safe
    and meaningful:

      Simple column:       ARRAY_AGG(icpc)              → ARRAY_AGG(icpc ORDER BY icpc)
      DISTINCT column:     ARRAY_AGG(DISTINCT icpc)     → ARRAY_AGG(DISTINCT icpc ORDER BY icpc)
      Simple function:     ARRAY_AGG(LEFT(icpc, 3))     → ARRAY_AGG(LEFT(icpc, 3) ORDER BY LEFT(icpc, 3))
      DISTINCT function:   ARRAY_AGG(DISTINCT LEFT(x,3))→ ARRAY_AGG(DISTINCT LEFT(x,3) ORDER BY LEFT(x,3))

    "Simple" means: a plain identifier, or a single function call (possibly
    nested) whose name is NOT one of the SQL control-flow keywords (CASE, IFF,
    COALESCE, NULLIF, etc.).  Complex expressions such as CASE WHEN … END,
    string concatenation, arithmetic, or anything containing a subquery are
    left unchanged.

    Calls that already contain a top-level ORDER BY are also left unchanged.
    """
    # Keywords that indicate a complex expression — do not add ORDER BY
    COMPLEX_KEYWORDS = re.compile(
        r'\b(CASE|IFF|COALESCE|NULLIF|GREATEST|LEAST|IF|NVL|DECODE)\b',
        re.IGNORECASE,
    )

    def _is_simple(expr: str) -> bool:
        """Return True when expr is a plain identifier or a single function call."""
        s = expr.strip()
        # Plain identifier (possibly qualified: schema.table.col)
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', s):
            return True
        # Single function call: name(...)  — must start and end with balanced parens
        func_m = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', s)
        if func_m and s.endswith(')'):
            func_name = func_m.group(1).upper()
            if COMPLEX_KEYWORDS.match(func_name):
                return False
            # Check there are no complex keywords inside the args either
            inner = s[func_m.end():len(s) - 1]
            if COMPLEX_KEYWORDS.search(inner):
                return False
            return True
        return False

    result = []
    i = 0
    n = len(sql)

    while i < n:
        m = re.match(r'\bARRAY_AGG\s*\(', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        inner_start = i + m.end()

        # Balanced-paren scan to locate the closing ')' of ARRAY_AGG(
        depth = 1
        j = inner_start
        in_str = False
        str_char = ''
        while j < n and depth > 0:
            ch = sql[j]
            if in_str:
                if ch == str_char and (j == 0 or sql[j - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            j += 1

        if depth != 0:
            result.append(sql[i])
            i += 1
            continue

        inner = sql[inner_start:j - 1]

        if _has_top_level_order_by(inner):
            result.append(sql[i:j])
        else:
            inner_stripped = inner.strip()
            distinct_m = re.match(r'DISTINCT\s+', inner_stripped, re.IGNORECASE)
            expr = inner_stripped[distinct_m.end():].strip() if distinct_m else inner_stripped

            if _is_simple(expr):
                if distinct_m:
                    result.append(f"ARRAY_AGG(DISTINCT {expr} ORDER BY {expr})")
                else:
                    result.append(f"ARRAY_AGG({expr} ORDER BY {expr})")
            else:
                # Complex expression — leave as-is
                result.append(sql[i:j])

        i = j

    return ''.join(result)


def convert_postgres_regexp_replace(sql: str) -> str:
    """
    Convert PostgreSQL REGEXP_REPLACE(str, pattern, replacement [, flags]) to the
    Snowflake form REGEXP_REPLACE(str, pattern, replacement, position, occurrence [, parameters]).

    PostgreSQL default replaces only the FIRST occurrence; Snowflake default replaces ALL.
    The flags argument drives the mapping:

      No flags  → occurrence=1  (first only)
      'g'       → occurrence=0  (all)
      'i'       → occurrence=1, parameters='i'  (first, case-insensitive)
      'gi'/'ig' → occurrence=0, parameters='i'  (all, case-insensitive)

    Calls that already have 5 or 6 arguments are left untouched (already Snowflake form).
    """
    result = []
    i = 0
    n = len(sql)

    while i < n:
        m = re.match(r'REGEXP_REPLACE\s*\(', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        func_start = i
        paren_start = i + m.end() - 1  # position of opening '('

        # Parse top-level comma-separated arguments, respecting nested parens/strings
        args = []
        arg_start = paren_start + 1
        depth = 1
        j = paren_start + 1
        in_str = False
        str_char = ''

        while j < n and depth > 0:
            ch = sql[j]
            if in_str:
                if ch == str_char and (j == 0 or sql[j - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    args.append(sql[arg_start:j].strip())
                    break
            elif ch == ',' and depth == 1:
                args.append(sql[arg_start:j].strip())
                arg_start = j + 1
            j += 1

        if depth != 0:
            # Unbalanced parentheses – leave as-is
            result.append(sql[i])
            i += 1
            continue

        end = j + 1  # one past the closing ')'

        if len(args) == 3:
            # No flags argument → replace first occurrence only
            # Recurse into s to handle nested REGEXP_REPLACE calls
            s, p, r = args
            s = convert_postgres_regexp_replace(s)
            result.append(f"REGEXP_REPLACE({s}, {p}, {r}, 1, 1)")
            i = end
        elif len(args) == 4:
            s, p, r, flags_arg = args
            # Recurse into s to handle nested REGEXP_REPLACE calls
            s = convert_postgres_regexp_replace(s)
            flags_inner = flags_arg.strip().strip("'\"").lower()
            has_g = 'g' in flags_inner
            has_i = 'i' in flags_inner
            occurrence = 0 if has_g else 1
            if has_i:
                result.append(f"REGEXP_REPLACE({s}, {p}, {r}, 1, {occurrence}, 'i')")
            else:
                result.append(f"REGEXP_REPLACE({s}, {p}, {r}, 1, {occurrence})")
            i = end
        else:
            # 1, 2, or 5+ args – already Snowflake form; recurse into args to handle
            # any nested REGEXP_REPLACE calls (e.g. in the first/subject argument).
            converted_args = [convert_postgres_regexp_replace(a) for a in args]
            result.append(f"REGEXP_REPLACE({', '.join(converted_args)})")
            i = end

    return ''.join(result)


def convert_postgres_regex_to_rlike(sql: str) -> str:
    """
    Convert PostgreSQL regex match operators ~ and ~* to Snowflake equivalents.
    Translates regex patterns from PostgreSQL to Snowflake format:
    - Replace \\d with [0-9] (digit character class)
    - ~ (case-sensitive)   → expr RLIKE 'pattern'
    - ~* (case-insensitive) → REGEXP_LIKE(expr, 'pattern', 'i')
    - !~ (negated, case-sensitive)   → NOT expr RLIKE 'pattern'
    - !~* (negated, case-insensitive) → NOT REGEXP_LIKE(expr, 'pattern', 'i')

    Anchoring:
    PostgreSQL ~ / ~* are partial-match operators (like re.search).
    Snowflake RLIKE / REGEXP_LIKE are full-match operators (like re.fullmatch).
    To preserve semantics:
    - If pattern has no leading ^, prepend .*
    - If pattern has no trailing $, append .*

    Examples:
        column ~ '^\\d{2}[^a-zA-Z0-9]{1}\\d{4}$'
        → column RLIKE '^[0-9]{2}[^a-zA-Z0-9]{1}[0-9]{4}$'

        icpc ~ '^[a-zA-Z]{1}[0-9]{2}'
        → icpc RLIKE '^[a-zA-Z]{1}[0-9]{2}.*'

        COALESCE(a,b) ~* 'vaxigrip tetra'
        → REGEXP_LIKE(COALESCE(a,b), '.*vaxigrip tetra.*', 'i')
    """
    def translate_postgres_regex_pattern(pattern_str: str) -> str:
        """Translate PostgreSQL regex syntax to Snowflake compatible format"""
        translated = pattern_str.replace(r'\d', '[0-9]')
        return translated

    def anchor_pattern(pattern: str) -> str:
        """Wrap pattern with .* on either side to preserve partial-match semantics."""
        if not pattern.startswith('^') and not pattern.startswith('.*'):
            pattern = '.*' + pattern
        if not pattern.endswith('$') and not pattern.endswith('.*'):
            pattern = pattern + '.*'
        return pattern

    def _peel_leading_parens(raw: str):
        """Split off any leading '(' grouping chars from a captured \S+ token.

        When the regex captures e.g. '((tekst' the leading '((' are SQL grouping
        parentheses that belong *before* the NOT keyword, not inside the function call.
        Function-call parens like 'COALESCE(' start with a letter so they are unaffected.
        Returns (prefix, expression) where prefix is the stripped leading parens.
        """
        m = re.match(r'^(\(+)', raw)
        if m:
            return m.group(1), raw[m.end():]
        return '', raw

    # Match: expression !~* 'pattern' (negated, case-insensitive) — must come before ~* / ~
    def repl_not_tilde_star(match):
        prefix, expression = _peel_leading_parens(match.group(1))
        quote = match.group(2)
        pattern = anchor_pattern(translate_postgres_regex_pattern(match.group(3)))
        return f"{prefix}NOT REGEXP_LIKE({expression}, {quote}{pattern}{quote}, 'i')"

    sql = re.sub(
        r'(\S+)[^\S\n]+!~\*[^\S\n]+([\'"])([^\2]*?)\2',
        repl_not_tilde_star,
        sql,
        flags=re.IGNORECASE
    )

    # Match: expression !~ 'pattern' (negated, case-sensitive)
    def repl_not_tilde(match):
        prefix, expression = _peel_leading_parens(match.group(1))
        quote = match.group(2)
        pattern = anchor_pattern(translate_postgres_regex_pattern(match.group(3)))
        return f"{prefix}NOT {expression} RLIKE {quote}{pattern}{quote}"

    sql = re.sub(
        r'(\S+)[^\S\n]+!~[^\S\n]+([\'"])([^\2]*?)\2',
        repl_not_tilde,
        sql,
        flags=re.IGNORECASE
    )

    # Match: expression ~* 'pattern' (case-insensitive) — must come before ~ to avoid mis-match
    def repl_tilde_star(match):
        expression = match.group(1)
        quote = match.group(2)
        pattern = anchor_pattern(translate_postgres_regex_pattern(match.group(3)))
        return f"REGEXP_LIKE({expression}, {quote}{pattern}{quote}, 'i')"

    sql = re.sub(
        r'(\S+)[^\S\n]+~\*[^\S\n]+([\'"])([^\2]*?)\2',
        repl_tilde_star,
        sql,
        flags=re.IGNORECASE
    )

    # Match: expression ~ 'pattern' (case-sensitive)
    def repl_tilde(match):
        expression = match.group(1)
        quote = match.group(2)
        pattern = anchor_pattern(translate_postgres_regex_pattern(match.group(3)))
        return f"{expression} RLIKE {quote}{pattern}{quote}"

    sql = re.sub(
        r'(\S+)[^\S\n]+~[^\S\n]+([\'"])([^\2]*?)\2',
        repl_tilde,
        sql,
        flags=re.IGNORECASE
    )

    return sql


def convert_ja_nee_ilike_to_equals(sql: str) -> str:
        """
        Convert boolean-like Dutch text comparisons to deterministic equality checks.

        Rewrites:
            col ILIKE 'ja'  -> col = 'ja'
            col ILIKE 'nee' -> col = 'nee'

        Also handles NOT ILIKE variants safely:
            col NOT ILIKE 'ja'  -> col <> 'ja'
            col NOT ILIKE 'nee' -> col <> 'nee'
        """
        # Handle NOT ILIKE first so it is not partially rewritten by the ILIKE rule.
        sql = re.sub(
                r"\bNOT\s+ILIKE\s*('(?:ja|nee)')",
                r"<> \1",
                sql,
                flags=re.IGNORECASE,
        )
        sql = re.sub(
                r"\bILIKE\s*('(?:ja|nee)')",
                r"= \1",
                sql,
                flags=re.IGNORECASE,
        )
        return sql


def add_varchar_cast_to_untyped_array_access(sql: str) -> str:
    """
    Add ::varchar cast to array subscript accesses that have no explicit type cast.

    In Snowflake, array elements are VARIANT by default. Without a cast, the value
    keeps the VARIANT type instead of being coerced to text.  Any access of the form
    identifier[N] (where N is a numeric literal) that is NOT immediately followed by
    a PostgreSQL cast (::type) is wrapped in ::varchar so sqlglot converts it to
    CAST(identifier[N-1] AS VARCHAR).

    Examples:
        coex_rq[3]          -> coex_rq[3]::varchar
        coex_rq[3]::int     -> coex_rq[3]::int          (unchanged)
        coex_rq[2]::date    -> coex_rq[2]::date          (unchanged)
        coex_rq[2][1]       -> coex_rq[2][1]::varchar
        coex_rq[2][1]::numeric -> coex_rq[2][1]::numeric (unchanged)
    """
    # Match identifier[digit(s)][digit(s)]... and check if it has a type cast following
    # Use a replacement function to avoid regex backtracking issues with lookaheads
    def replacer(match):
        full_match = match.group(0)
        # Check if the match is followed by a PostgreSQL-style type cast
        start_pos = match.end()
        if start_pos < len(sql) and sql[start_pos:start_pos+2] == '::':
            # Already has a type cast, don't add ::varchar
            return full_match
        else:
            # No type cast, add ::varchar
            return full_match + '::varchar'
    
    # Match identifier followed by one or more [digit] patterns
    # but exclude ARRAY keyword to avoid matching ARRAY[...] construction
    pattern = r'\b(?!ARRAY\b)[a-zA-Z_][a-zA-Z0-9_]*(?:\s*\[\s*\d+\s*\])+'
    return re.sub(pattern, replacer, sql, flags=re.IGNORECASE)


def convert_array_indices_postgres_to_snowflake(sql: str) -> str:
    """
    Convert PostgreSQL array indices (1-based) to Snowflake array indices (0-based).
    PostgreSQL uses 1-based indexing, Snowflake uses 0-based indexing.
    This function finds all array[index] patterns and subtracts 1 from the index.
    
    Examples:
        array[1] -> array[0]
        column[3] -> column[2]
        array_col[variable] -> array_col[variable - 1]
    """
    result = []
    i = 0
    
    while i < len(sql):
        # Look for array access pattern: identifier[
        # Match identifiers (including qualified names like table.column)
        match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*\[', sql[i:])
        if not match:
            result.append(sql[i])
            i += 1
            continue
        
        # Found potential array access
        identifier = match.group(1)
        bracket_start = i + match.end() - 1  # Position of '['
        
        # Find matching closing bracket
        bracket_depth = 1
        paren_depth = 0
        in_string = False
        string_char = None
        j = bracket_start + 1
        
        while j < len(sql) and bracket_depth > 0:
            char = sql[j]
            
            # Handle strings
            if char in ('"', "'") and (j == 0 or sql[j-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            # Count brackets and parens outside strings
            if not in_string:
                if char == '[':
                    bracket_depth += 1
                elif char == ']':
                    bracket_depth -= 1
                elif char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
            
            j += 1
        
        if bracket_depth != 0:
            # Malformed bracket, skip
            result.append(sql[i])
            i += 1
            continue
        
        # Extract the index expression
        index_expr = sql[bracket_start + 1:j - 1].strip()
        
        # Check if it's a simple numeric literal
        if index_expr.isdigit():
            # Simple numeric index - subtract 1
            new_index = max(0, int(index_expr) - 1)
            result.append(sql[i:bracket_start + 1])
            result.append(str(new_index))
            result.append(']')
            i = j
        elif re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', index_expr):
            # Simple variable name - subtract 1
            result.append(sql[i:bracket_start + 1])
            result.append(f"({index_expr}) - 1")
            result.append(']')
            i = j
        elif re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+$', index_expr):
            # Qualified column name (e.g., table.column) - subtract 1
            result.append(sql[i:bracket_start + 1])
            result.append(f"({index_expr}) - 1")
            result.append(']')
            i = j
        else:
            # Complex expression - wrap in parens and subtract 1
            result.append(sql[i:bracket_start + 1])
            result.append(f"({index_expr}) - 1")
            result.append(']')
            i = j
    
    return ''.join(result)


def convert_array_remove_to_variant(sql: str) -> str:
    """
    Convert ARRAY_REMOVE(array, value) to {{ function('pg_array_remove') }}(array, TO_VARIANT(value)).

    The dbt adapter function preserves PostgreSQL semantics:
    - NULL input array stays NULL
    - removing NULL from a non-NULL array yields an empty array when appropriate

    The {{ function('pg_array_remove') }} Jinja call resolves to the correct UDF at compile time.
    """
    max_iterations = 20  # Safety limit
    
    for iteration in range(max_iterations):
        # Find the first array_remove with unwrapped value
        i = 0
        found = False
        
        while i < len(sql):
            # Look for array_remove(
            match = re.match(r'\barray_remove\s*\(', sql[i:], re.IGNORECASE)
            if not match:
                i += 1
                continue
            
            # Found array_remove at position i
            func_start = i
            func_end = i + match.end()
            i = func_end
            
            # Find comma separating array from value
            paren_count = 1
            bracket_count = 0
            in_string = False
            string_char = None
            comma_pos = None
            j = i
            
            while j < len(sql) and paren_count > 0:
                char = sql[j]
                
                if char in ('"', "'") and (j == 0 or sql[j-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                
                if not in_string:
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            break
                    elif char == '[':
                        bracket_count += 1
                    elif char == ']':
                        bracket_count -= 1
                    elif char == ',' and paren_count == 1 and bracket_count == 0 and comma_pos is None:
                        comma_pos = j
                
                j += 1
            
            if comma_pos is None:
                i = func_end
                continue
            
            # Check if value is already wrapped
            value_start = comma_pos + 1
            while value_start < len(sql) and sql[value_start].isspace():
                value_start += 1
            
            if sql[value_start:value_start+11].upper() == 'TO_VARIANT(':
                # Already wrapped, skip this one
                i = func_end
                continue
            
            # Find end of value (closing paren of array_remove)
            paren_count = 1
            value_end = value_start
            in_string = False
            string_char = None
            
            while value_end < len(sql) and paren_count > 0:
                char = sql[value_end]
                
                if char in ('"', "'") and (value_end == 0 or sql[value_end-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                
                if not in_string:
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                
                value_end += 1
            
            # value_end is now pointing right after the closing paren
            array_expr = sql[func_end:comma_pos].strip()
            value_expr = sql[value_start:value_end-1].strip()

            # ARRAY_REMOVE(arr, NULL) is equivalent to ARRAY_COMPACT(arr) in Snowflake
            if value_expr.upper() == 'NULL':
                replacement = f"ARRAY_COMPACT({array_expr})"
            else:
                if re.match(r'TO_VARIANT\s*\(', value_expr, re.IGNORECASE):
                    wrapped_value_expr = value_expr
                else:
                    wrapped_value_expr = f"TO_VARIANT({value_expr})"
                replacement = f"{{{{ function('pg_array_remove') }}}}({array_expr}, {wrapped_value_expr})"

            sql = sql[:func_start] + replacement + sql[value_end:]
            found = True
            break  # Process one at a time, restart from beginning
        
        if not found:
            break  # No more unwrapped calls
    
    return sql


def replace_uitslag_num_for_bepaling(sql: str) -> str: #TEMPFIX
    """
    Post-processing: in any query block whose FROM clause references the 'bepaling'
    table, replace uitslag_num with {{ function('get_number') }}(uitslag).

    bepaling.uitslag stores raw text; get_number() coerces it to a numeric value.
    """

    def _paren_depth_at(text: str, pos: int) -> int:
        depth = 0
        in_str = False
        str_char = None
        for i in range(pos):
            ch = text[i]
            if in_str:
                if ch == str_char and (i == 0 or text[i - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
        return depth

    def _collect_keywords(text: str):
        """Yield (kind, pos, depth) for each SELECT and FROM keyword."""
        n = len(text)
        depth = 0
        in_str = False
        str_char = None
        i = 0
        while i < n:
            ch = text[i]
            if in_str:
                if ch == str_char and (i == 0 or text[i - 1] != '\\'):
                    in_str = False
                i += 1
                continue
            if ch in ("'", '"'):
                in_str = True
                str_char = ch
                i += 1
                continue
            if ch == '(':
                depth += 1
                i += 1
                continue
            if ch == ')':
                depth -= 1
                i += 1
                continue
            for kw in ('SELECT', 'FROM'):
                klen = len(kw)
                if text[i:i + klen].upper() == kw:
                    before = text[i - 1] if i > 0 else ' '
                    after = text[i + klen] if i + klen < n else ' '
                    if not (before.isalnum() or before == '_') and not (after.isalnum() or after == '_'):
                        yield (kw, i, depth)
                        break
            i += 1

    def _from_has_bepaling(text: str, from_pos: int, from_depth: int) -> bool:
        """Return True if the FROM clause at from_pos references 'bepaling'."""
        n = len(text)
        i = from_pos + 4  # skip 'FROM'
        depth = from_depth
        in_str = False
        str_char = None
        clause_end = re.compile(
            r'\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|INTERSECT|EXCEPT|SELECT)\b',
            re.IGNORECASE,
        )
        start = i
        while i < n:
            ch = text[i]
            if in_str:
                if ch == str_char and (i == 0 or text[i - 1] != '\\'):
                    in_str = False
                i += 1
                continue
            if ch in ("'", '"'):
                in_str = True
                str_char = ch
                i += 1
                continue
            if ch == '(':
                depth += 1
                i += 1
                continue
            if ch == ')':
                if depth == from_depth:
                    break  # exited enclosing scope
                depth -= 1
                i += 1
                continue
            if depth == from_depth:
                m = clause_end.match(text, i)
                if m:
                    before = text[i - 1] if i > 0 else ' '
                    if not (before.isalnum() or before == '_'):
                        break
            i += 1
        from_clause = text[start:i]
        # Only match bepaling as a direct table reference (FROM bepaling / JOIN bepaling),
        # not when it appears as a subquery alias (") AS bepaling").
        return bool(re.search(
            r'(?:^|,|\bJOIN\b)\s*bepaling\b',
            from_clause,
            re.IGNORECASE,
        ))

    # Collect SELECT and FROM positions with their paren depths
    selects = []  # (pos, depth)
    froms = []    # (pos, depth)
    for kind, pos, depth in _collect_keywords(sql):
        if kind == 'SELECT':
            selects.append((pos, depth))
        else:
            froms.append((pos, depth))

    uitslag_re = re.compile(r'\buitslag_num\b', re.IGNORECASE)
    matches = list(uitslag_re.finditer(sql))
    if not matches:
        return sql

    to_replace = []
    for match in matches:
        match_pos = match.start()

        # Skip aliases: occurrences directly preceded by AS (e.g. "... as uitslag_num")
        preceding = sql[:match_pos].rstrip()
        if re.search(r'\bAS$', preceding, re.IGNORECASE):
            continue

        # Skip ORDER BY references: the uitslag_num here refers to the already-computed
        # alias, not the raw bepaling column. Detect by checking whether the last
        # ORDER BY before this position comes after the last SELECT at the same depth.
        if re.search(r'\bORDER\s+BY\b', sql[:match_pos], re.IGNORECASE):
            last_ob = max((m.start() for m in re.finditer(r'\bORDER\s+BY\b', sql[:match_pos], re.IGNORECASE)), default=-1)
            last_select = max((m.start() for m in re.finditer(r'\bSELECT\b', sql[:match_pos], re.IGNORECASE)), default=-1)
            if last_ob > last_select:
                continue

        match_depth = _paren_depth_at(sql, match_pos)

        # Find the innermost SELECT that precedes this position
        governing_select = None
        for sel_pos, sel_depth in reversed(selects):
            if sel_pos < match_pos and sel_depth <= match_depth:
                governing_select = (sel_pos, sel_depth)
                break

        if governing_select is None:
            continue

        sel_pos, sel_depth = governing_select

        # Find the FROM that belongs to this SELECT at the same depth,
        # occurring after uitslag_num (column list comes before FROM)
        governing_from = None
        for from_pos, from_depth in froms:
            if from_pos > match_pos and from_depth == sel_depth:
                governing_from = (from_pos, from_depth)
                break

        if governing_from is None:
            continue

        if _from_has_bepaling(sql, *governing_from):
            # Keep SELECT depth so alias injection can be scope-aware.
            to_replace.append((match, sel_depth))

    func_call = "{{ function('get_number') }}(uitslag)"
    for match, sel_depth in sorted(to_replace, key=lambda item: item[0].start(), reverse=True):
        after = sql[match.end():]
        already_aliased = re.match(r'\s+AS\s+\w', after, re.IGNORECASE)
        match_depth = _paren_depth_at(sql, match.start())
        # Top-level item in this SELECT list if depth equals the governing SELECT depth.
        # Deeper means nested expression/function argument and should not receive alias.
        is_top_level_select_item = match_depth == sel_depth
        if is_top_level_select_item and not already_aliased:
            replacement = f"{func_call} AS uitslag_num"
        else:
            replacement = func_call
        sql = sql[:match.start()] + replacement + sql[match.end():]

    return sql


def replace_numeric_cast_with_float_in_nan_cases(sql: str) -> str:
    """
    Pre-processing: in any CASE...END expression that can return the literal 'NaN',
    replace every ::numeric cast with ::float.

    Snowflake only allows NaN in FLOAT columns, not in NUMBER/DECIMAL columns.
    Changing ::numeric -> ::float here means sqlglot will emit CAST(... AS FLOAT)
    and convert_cast_to_try_cast will leave it untouched (it only rewrites
    NUMERIC/DECIMAL/NUMBER to TO_DECFLOAT).

    Pattern recognised:
        CASE WHEN ... THEN 'NaN' ... ELSE expr::numeric END
    """
    result = []
    i = 0
    n = len(sql)

    while i < n:
        m = re.match(r'\bCASE\b', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        case_start = i
        i += m.end()

        # Find the matching END keyword by tracking CASE/END nesting
        depth = 1
        in_str = False
        str_char = None
        while i < n and depth > 0:
            ch = sql[i]
            if in_str:
                if ch == str_char and (i == 0 or sql[i - 1] != '\\'):
                    in_str = False
                i += 1
                continue
            if ch in ("'", '"'):
                in_str = True
                str_char = ch
                i += 1
                continue
            kw_m = re.match(r'\b(CASE|END)\b', sql[i:], re.IGNORECASE)
            if kw_m:
                kw = kw_m.group(1).upper()
                if kw == 'CASE':
                    depth += 1
                else:
                    depth -= 1
                i += kw_m.end()
                continue
            i += 1

        case_block = sql[case_start:i]

        # Only modify blocks that return 'NaN' as a THEN or ELSE value
        if re.search(r"\bTHEN\s+'NaN'", case_block, re.IGNORECASE):
            case_block = re.sub(r'::numeric\b', '::float', case_block, flags=re.IGNORECASE)

        result.append(case_block)

    return ''.join(result)


def remove_redundant_literal_and_null_casts(sql: str) -> str:
    """
    Remove unnecessary casts on numeric literals and NULL values.

    Examples:
        1::float              -> 1
        -2.5::double precision -> -2.5
        CAST(3 AS INTEGER)    -> 3
        NULL::varchar         -> NULL
        CAST(NULL AS DATE)    -> NULL

    NUMERIC/DECIMAL/NUMBER casts on literals are intentionally left intact so that
    convert_cast_to_try_cast can convert them to TO_DECFLOAT(expr).
    This intentionally leaves casts on non-literal expressions untouched.
    """
    # Excludes NUMERIC/DECIMAL/NUMBER so that 14.1::numeric -> TO_DECFLOAT(14.1)
    # via convert_cast_to_try_cast rather than being silently dropped here.
    non_decimal_numeric_types = (
        r'REAL|FLOAT|FLOAT4|FLOAT8|DOUBLE\s+PRECISION|'
        r'SMALLINT|INTEGER|INT|BIGINT'
    )

    # Remove PostgreSQL-style casts on numeric literals (non-decimal types only).
    sql = re.sub(
        rf'(?<![\w\]])([+-]?\d+(?:\.\d+)?)\s*::\s*(?:{non_decimal_numeric_types})(?:\s*\([^)]*\))?\b',
        r'\1',
        sql,
        flags=re.IGNORECASE,
    )

    # Remove CAST(numeric_literal AS <non-decimal numeric type>).
    sql = re.sub(
        rf'\bCAST\s*\(\s*([+-]?\d+(?:\.\d+)?)\s+AS\s+(?:{non_decimal_numeric_types})(?:\s*\([^)]*\))?\s*\)',
        r'\1',
        sql,
        flags=re.IGNORECASE,
    )

    # Remove PostgreSQL-style casts on NULL.
    sql = re.sub(
        r'\bNULL\s*::\s*[a-zA-Z_][a-zA-Z0-9_]*(?:\s+precision|\s+varying)?(?:\s*\([^)]*\))?(?:\s*\[\s*\])?',
        'NULL',
        sql,
        flags=re.IGNORECASE,
    )

    # Remove CAST(NULL AS <type>). Keep nested parentheses around NULL as-is.
    sql = re.sub(
        r'\bCAST\s*\(\s*NULL\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\s+precision|\s+varying)?(?:\s*\([^)]*\))?(?:\s*\[\s*\])?\s*\)',
        'NULL',
        sql,
        flags=re.IGNORECASE,
    )

    return sql

def convert_cast_to_try_cast(sql: str) -> str:
    """
    Convert CAST(... AS DATE) to TO_DATE(expr), and CAST(... AS DECIMAL/NUMBER/NUMERIC)
    to TO_DECFLOAT(expr).
    """
    # Numeric types → TO_DECFLOAT(expr)
    numeric_types = {'DECIMAL', 'NUMBER', 'NUMERIC'}
    # Date type → TO_DATE(expr)
    date_types = {'DATE': 'TO_DATE'}

    # Order matters: numeric rewrites must run before DATE so the loop is deterministic.
    # A set would give random iteration order across Python runs.
    target_types = ['DECIMAL', 'NUMBER', 'NUMERIC', 'DATE']

    def _is_array_access_expression(expr: str) -> bool:
        """Return True for identifier[index] or identifier[index][index] style access."""
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*(?:\s*\[\s*[^\]]+\s*\])+$', expr.strip()))

    def _ensure_varchar_for_array_access(expr: str) -> str:
        """TO_DECFLOAT requires array element values to be coerced to text first."""
        stripped = expr.strip()
        if not _is_array_access_expression(stripped):
            return stripped
        # Avoid double-wrapping when already string-cast.
        if re.search(r'\bCAST\s*\(\s*.+\s+AS\s+VARCHAR\s*\)', stripped, re.IGNORECASE):
            return stripped
        if re.search(r'::\s*VARCHAR\b', stripped, re.IGNORECASE):
            return stripped
        return f'CAST({stripped} AS VARCHAR)'

    def _rewrite_for_dtype(s: str, dtype: str) -> str:
        """Single-type pass: rewrite CAST(... AS dtype) in s, recursing into non-matching CASTs."""
        result = []
        i = 0
        while i < len(s):
            match = re.match(r'\bCAST\s*\(', s[i:], re.IGNORECASE)
            if match:
                start = i
                cast_prefix = s[start : start + match.end()]  # e.g. "CAST("
                i += match.end()
                paren_count = 1
                expr_start = i
                while i < len(s) and paren_count > 0:
                    if s[i] == '(':
                        paren_count += 1
                    elif s[i] == ')':
                        paren_count -= 1
                    i += 1
                if paren_count == 0:
                    cast_content = s[expr_start:i-1]
                    as_pattern = rf'\s+AS\s+{dtype}(?:\s*\([^)]*\))?\s*$'
                    if re.search(as_pattern, cast_content, re.IGNORECASE):
                        as_match = re.search(as_pattern, cast_content, re.IGNORECASE)
                        expr = cast_content[:as_match.start()].strip()
                        if dtype in numeric_types:
                            # If precision/scale is specified (e.g. NUMERIC(18,3)), keep as
                            # CAST(expr AS NUMBER(p,s)) so the scale is preserved in Snowflake.
                            # Only bare NUMERIC/DECIMAL/NUMBER without precision falls back to
                            # TO_DECFLOAT.
                            prec_match = re.search(
                                rf'\s+AS\s+{dtype}\s*(\(\s*\d+\s*(?:,\s*\d+\s*)?\))\s*$',
                                cast_content, re.IGNORECASE
                            )
                            if prec_match:
                                prec_spec = prec_match.group(1)
                                result.append(f'CAST({expr} AS NUMBER{prec_spec})')
                            else:
                                expr_for_numeric = _ensure_varchar_for_array_access(expr)
                                result.append(f'TO_DECFLOAT({expr_for_numeric})')
                        else:
                            func_name = date_types[dtype]
                            result.append(f'{func_name}({expr})')
                    else:
                        # Outer CAST doesn't match – recurse into its inner content
                        inner_processed = _rewrite_for_dtype(cast_content, dtype)
                        result.append(f'{cast_prefix}{inner_processed})')
                else:
                    # Malformed, keep original
                    result.append(s[start:i])
            else:
                result.append(s[i])
                i += 1
        return ''.join(result)

    # Process each target type
    for dtype in target_types:
        sql = _rewrite_for_dtype(sql, dtype)
    
    return sql

def wrap_round_with_number_scale(sql: str) -> str:
    """
    Post-process: Wrap ROUND(expr, n) as CAST(ROUND(expr, n) AS NUMBER(36, n)).
    This ensures Snowflake returns the exact scale specified by the ROUND function.
    Only wraps when the second argument is a numeric literal.
    Applies multiple passes to catch nested ROUND calls.
    """
    # Keep processing until no more ROUND calls are wrapped
    max_iterations = 100  # Prevent infinite loops
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        result = []
        i = 0
        found_any = False

        while i < len(sql):
            # Find next ROUND(
            match = re.search(r'\bROUND\s*\(', sql[i:], re.IGNORECASE)
            if not match:
                result.append(sql[i:])
                break

            round_start = i + match.start()
            
            # Skip this ROUND only if it's already wrapped with a NUMBER cast:
            # CAST(ROUND(...) AS NUMBER(...))
            # A surrounding CAST to a different type (e.g. VARCHAR) should still be wrapped.
            if round_start >= 5:
                before_text = sql[max(0, round_start - 5):round_start].rstrip()
                if before_text.endswith('CAST('):
                    # Scan forward to find ROUND's closing paren
                    scan_pos = i + match.end()  # position just after "ROUND("
                    scan_depth = 1
                    while scan_pos < len(sql) and scan_depth > 0:
                        ch = sql[scan_pos]
                        if ch == '(':
                            scan_depth += 1
                        elif ch == ')':
                            scan_depth -= 1
                        scan_pos += 1
                    after_round = sql[scan_pos:scan_pos + 20].lstrip()
                    if re.match(r'AS\s+NUMBER', after_round, re.IGNORECASE):
                        # Already wrapped with CAST(... AS NUMBER), skip it
                        result.append(sql[i:i + match.end()])
                        i = i + match.end()
                        continue
            
            # Keep everything before "ROUND("
            result.append(sql[i:round_start])
            
            # Find matching closing parenthesis
            open_paren_pos = i + match.end() - 1
            j = open_paren_pos + 1
            depth = 1
            in_string = False
            string_char = None

            while j < len(sql) and depth > 0:
                ch = sql[j]
                prev_ch = sql[j - 1] if j > 0 else ''
                
                # Track string boundaries
                if ch in ('"', "'") and prev_ch != '\\':
                    if not in_string:
                        in_string = True
                        string_char = ch
                    elif ch == string_char:
                        in_string = False
                # Track parentheses only outside strings
                elif not in_string:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                j += 1

            if depth != 0:
                # Unbalanced parentheses, keep remainder as-is
                result.append(sql[round_start:])
                break

            # Extract the full ROUND(...) expression
            full_round_expr = sql[round_start:j]
            # Extract arguments: everything between ROUND( and )
            args_content = sql[open_paren_pos + 1:j - 1]
            
            # Parse arguments by top-level commas
            args = []
            start = 0
            depth = 0
            in_string = False
            string_char = None
            
            for idx, ch in enumerate(args_content):
                prev_ch = args_content[idx - 1] if idx > 0 else ''
                
                if ch in ('"', "'") and prev_ch != '\\':
                    if not in_string:
                        in_string = True
                        string_char = ch
                    elif ch == string_char:
                        in_string = False
                elif not in_string:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                    elif ch == ',' and depth == 0:
                        args.append(args_content[start:idx].strip())
                        start = idx + 1
            
            args.append(args_content[start:].strip())
            
            # If we have 2 args and the second is a numeric literal, wrap with CAST
            if len(args) == 2 and re.match(r'^\d+$', args[1]):
                scale = args[1]
                result.append(f'CAST({full_round_expr} AS NUMBER(36, {scale}))')
                found_any = True
            else:
                # Not a ROUND with numeric scale, keep original
                result.append(full_round_expr)
            
            i = j

        sql = ''.join(result)
        if not found_any:
            break  # No more ROUND calls to wrap, exit loop
    
    return sql


def _find_left_expr_before_operator(text: str, op_start_pos: int) -> tuple:
    """
    Extract the complete left expression before an operator by working backwards.
    Handles nested parentheses, array indexing [n], type casts ::type, and unary -/+ operators.
    
    Returns:
        Tuple of (expression_text, start_position)
    """
    i = op_start_pos - 1
    
    # Skip trailing whitespace
    while i >= 0 and text[i].isspace():
        i -= 1
    
    if i < 0:
        return "", 0
    
    end_pos = i + 1
    
    # Work backwards through the complete expression
    while i >= 0:
        if text[i] == ')':
            # Closing paren - find matching opening paren
            paren_count = 1
            i -= 1
            while i >= 0 and paren_count > 0:
                if text[i] == ')':
                    paren_count += 1
                elif text[i] == '(':
                    paren_count -= 1
                i -= 1
            i += 1
            # Continue backwards to get function name
            i -= 1
            while i >= 0 and (text[i].isalnum() or text[i] == '_'):
                i -= 1
        elif text[i] == ']':
            # Closing bracket - find matching opening bracket
            bracket_count = 1
            i -= 1
            while i >= 0 and bracket_count > 0:
                if text[i] == ']':
                    bracket_count += 1
                elif text[i] == '[':
                    bracket_count -= 1
                i -= 1
        elif i > 0 and text[i-1:i+1] == '::':
            # Type cast - skip both colons
            i -= 2
        elif text[i].isalnum() or text[i] in ('_', '.', '$', '{', '}', "'", '"', ':', ','):
            i -= 1
        elif text[i] in ('-', '+') and i > 0:
            # Potential unary operator
            peek = i + 1
            while peek < end_pos and text[peek].isspace():
                peek += 1
            if peek < end_pos and (text[peek].isdigit() or text[peek] == '.'):
                i -= 1
            else:
                break
        else:
            break
    
    start_pos = i + 1
    return text[start_pos:end_pos].strip(), start_pos

def convert_all_to_snowflake_post(sql: str) -> str:
    """
    Convert PostgreSQL ALL expressions to Snowflake equivalents.
    - element <= ALL(array) -> element <= ARRAY_MIN(array) AND NOT COALESCE(ARRAY_CONTAINS(NULL, array), FALSE)
    - element < ALL(array) -> element < ARRAY_MIN(array) AND NOT COALESCE(ARRAY_CONTAINS(NULL, array), FALSE)
    - element >= ALL(array) -> element >= ARRAY_MAX(array) AND NOT COALESCE(ARRAY_CONTAINS(NULL, array), FALSE)
    - element > ALL(array) -> element > ARRAY_MAX(array) AND NOT COALESCE(ARRAY_CONTAINS(NULL, array), FALSE)
    - element = ALL(array) -> ARRAY_SIZE(ARRAY_DISTINCT(array)) = 1 AND ARRAY_CONTAINS(element, array)
    - element <> ALL(array) -> NOT ARRAY_CONTAINS(element, array)
    - element != ALL(array) -> NOT ARRAY_CONTAINS(element, array)
    """

    def comparison_with_null_guard(comparison_sql: str, array_expr: str) -> str:
        return (
            f"{comparison_sql} AND "
            f"NOT COALESCE(ARRAY_CONTAINS(NULL, {array_expr}), FALSE)"
        )
    
    # Process each operator type
    operators = [
        ('<=', lambda l, a: comparison_with_null_guard(f"{l} <= ARRAY_MIN({a})", a)),
        ('>=', lambda l, a: comparison_with_null_guard(f"{l} >= ARRAY_MAX({a})", a)),
        ('<>', lambda l, a: f"NOT ARRAY_CONTAINS(TO_VARIANT({l}), {a})"),
        ('!=', lambda l, a: f"NOT ARRAY_CONTAINS(TO_VARIANT({l}), {a})"),
        ('<', lambda l, a: comparison_with_null_guard(f"{l} < ARRAY_MIN({a})", a)),
        ('>', lambda l, a: comparison_with_null_guard(f"{l} > ARRAY_MAX({a})", a)),
        ('=', lambda l, a: f"ARRAY_SIZE(ARRAY_DISTINCT({a})) = 1 AND ARRAY_CONTAINS(TO_VARIANT({l}), {a})"),
    ]
    
    for op, replacement_func in operators:
        # Find all occurrences of "operator ALL("
        pattern = re.escape(op) + r'\s*ALL\s*\('
        
        while True:
            match = re.search(pattern, sql, re.IGNORECASE)
            if not match:
                break
            
            op_start = match.start()
            
            # Extract left expression
            left_expr, left_start = _find_left_expr_before_operator(sql, op_start)
            
            if not left_expr:
                break
            
            # Find the array argument inside ALL(...)
            all_paren_start = match.end()
            paren_count = 1
            i = all_paren_start
            while i < len(sql) and paren_count > 0:
                if sql[i] == '(':
                    paren_count += 1
                elif sql[i] == ')':
                    paren_count -= 1
                i += 1
            
            if paren_count != 0:
                break
            
            array_expr = sql[all_paren_start:i-1].strip()
            
            # Build replacement
            replacement = replacement_func(left_expr, array_expr)
            
            # Replace in sql
            sql = sql[:left_start] + replacement + sql[i:]
    
    return sql

def convert_any_to_snowflake_post(sql: str) -> str:
    """
    Convert remaining PostgreSQL ANY expressions to Snowflake equivalents after sqlglot conversion.
    - element = ANY(array) -> ARRAY_CONTAINS(element, array)
    - element >= ANY(array) -> COALESCE(ARRAY_MIN(array) <= element, FALSE)
    - element <= ANY(array) -> COALESCE(ARRAY_MAX(array) >= element, FALSE)
    - element > ANY(array) -> COALESCE(ARRAY_MIN(array) < element, FALSE)
    - element < ANY(array) -> COALESCE(ARRAY_MAX(array) > element, FALSE)
    - element op ANY(array) -> EXISTS(SELECT 1 FROM TABLE(FLATTEN(input => array)) f WHERE element op f.value)
    """
    
    # Process each operator type, starting with multi-char operators first
    operators = [
        ('>=', lambda l, a: f"COALESCE(ARRAY_MIN({a}) <= {l}, FALSE)"),
        ('<=', lambda l, a: f"COALESCE(ARRAY_MAX({a}) >= {l}, FALSE)"),
        ('<>', lambda l, a: f"EXISTS (SELECT 1 FROM TABLE(FLATTEN(input => {a})) f WHERE {l} <> f.value)"),
        ('!=', lambda l, a: f"EXISTS (SELECT 1 FROM TABLE(FLATTEN(input => {a})) f WHERE {l} != f.value)"),
        ('>', lambda l, a: f"COALESCE(ARRAY_MIN({a}) < {l}, FALSE)"),
        ('<', lambda l, a: f"COALESCE(ARRAY_MAX({a}) > {l}, FALSE)"),
        ('=', lambda l, a: f"ARRAY_CONTAINS(TO_VARIANT({l}), {a})"),
    ]
    
    for op, replacement_func in operators:
        # Find all occurrences of "operator ANY("
        pattern = re.escape(op) + r'\s*ANY\s*\('
        
        while True:
            match = re.search(pattern, sql, re.IGNORECASE)
            if not match:
                break
            
            op_start = match.start()
            
            # Extract left expression
            left_expr, left_start = _find_left_expr_before_operator(sql, op_start)
            
            if not left_expr:
                # Couldn't extract left expression, skip this match
                break
            
            # Find the array argument inside ANY(...)
            any_paren_start = match.end()
            paren_count = 1
            i = any_paren_start
            while i < len(sql) and paren_count > 0:
                if sql[i] == '(':
                    paren_count += 1
                elif sql[i] == ')':
                    paren_count -= 1
                i += 1
            
            if paren_count != 0:
                # Malformed, skip
                break
            
            array_expr = sql[any_paren_start:i-1].strip()
            
            # Build replacement
            replacement = replacement_func(left_expr, array_expr)
            
            # Replace in sql
            sql = sql[:left_start] + replacement + sql[i:]
    
    return sql

def convert_postgres_escape_strings(sql: str) -> str:
    """
    Convert PostgreSQL escape strings E'...' to regular strings.
    In PostgreSQL E-strings:
    - E'\\n' means newline, E'\\t' means tab
    - E'\\\\' means one backslash
    - E'\\X' means just X (backslash escapes the character)
    
    For Snowflake REGEXP_REPLACE: E'\.0\\1' -> '.0\\1' (dot is unescaped, \\1 stays for backreference)
    """
    result = []
    i = 0
    
    while i < len(sql):
        # Look for E' or E" - but only if E is not part of an identifier
        # Check that E is preceded by whitespace, operator, or start of string
        if (i < len(sql) - 1 and 
            sql[i].upper() == 'E' and 
            sql[i+1] in ("'", '"') and
            (i == 0 or (not sql[i-1].isalnum() and sql[i-1] != '_' and sql[i-1] not in ("'", '"')))):
            quote_char = sql[i+1]
            result.append(quote_char)  # Remove the E, keep the quote
            i += 2  # Skip past E'
            
            # Process the string content
            while i < len(sql):
                if sql[i] == '\\' and i + 1 < len(sql):
                    next_char = sql[i+1]
                    if next_char == '\\':
                        # E'\\' means one literal backslash. Emit a single backslash
                        # so that sqlglot's subsequent string escaping produces the
                        # correct two-backslash representation in the Snowflake output
                        # (e.g. E'\.0\\1' -> '.0\1' here -> '.0\\1' after sqlglot).
                        result.append('\\')
                        i += 2
                    elif next_char in ('n', 't', 'r'):
                        # Special escape sequences
                        result.append('\\')
                        result.append(next_char)
                        i += 2
                    else:
                        # E'\X' -> just X (backslash escapes the character in E-string)
                        # This handles E'\.' -> '.' and similar cases
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

def convert_join_like_any_to_array_to_string(sql: str) -> str:
    """Convert JOIN table ON col LIKE ANY(array) to use ARRAY_TO_STRING.
    
    Snowflake supports LIKE ANY with ARRAY_TO_STRING:
    JOIN table ON col LIKE ANY(array_column)
    
    Becomes:
    JOIN table ON col LIKE ANY ARRAY_TO_STRING(array_column, ',')
    
    Handles LEFT/RIGHT/INNER JOIN types and AND conditions.
    """
    # Pattern to match [LEFT|RIGHT|INNER] JOIN ... ON ... LIKE ANY (...)
    pattern = re.compile(
        r'(\b(?:LEFT|RIGHT|INNER)?\s*JOIN\s+[\w\{\}\.\'\']+(?:\s+(?:AS\s+)?\w+)?\s+ON\s+[\w\.]+\s+(?:LIKE|ILIKE)\s+ANY\s*\(\s*)([^\)]+?)(\s*\))',
        re.IGNORECASE
    )
    
    def replace_join(match):
        prefix = match.group(1)  # Everything up to and including "ANY("
        array_ref = match.group(2).strip()
        suffix = match.group(3)  # Closing paren
        
        # Wrap the array reference with ARRAY_TO_STRING
        return f"{prefix}ARRAY_TO_STRING({array_ref}, ','){suffix}"
    
    return pattern.sub(replace_join, sql)


def wrap_array_to_string_with_compact(sql: str) -> str:
    """
    Wrap the array argument of every ARRAY_TO_STRING call with ARRAY_COMPACT()
    to mimic PostgreSQL behaviour where array_to_string() silently skips NULLs.

    ARRAY_TO_STRING(arr, ',')  ->  ARRAY_TO_STRING(ARRAY_COMPACT(arr), ',')

    Already-wrapped calls (ARRAY_COMPACT already present) are left untouched.
    """
    result = []
    i = 0

    while i < len(sql):
        match = re.match(r'ARRAY_TO_STRING\s*\(', sql[i:], re.IGNORECASE)
        if not match:
            result.append(sql[i])
            i += 1
            continue

        func_prefix = sql[i:i + match.end()]   # e.g. "ARRAY_TO_STRING("
        args_start = i + match.end()

        # Walk forward to find the comma that separates the first argument
        # from the delimiter, staying at paren depth 1.
        paren_depth = 1
        in_string = False
        string_char = None
        first_arg_end = None
        j = args_start

        while j < len(sql) and paren_depth > 0:
            char = sql[j]

            if char in ('"', "'") and (j == 0 or sql[j - 1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if not in_string:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 1 and first_arg_end is None:
                    first_arg_end = j

            j += 1

        # j now points one past the closing ')'
        if first_arg_end is None:
            # No comma found — not a standard ARRAY_TO_STRING call, keep as-is
            result.append(sql[i:j])
            i = j
            continue

        first_arg = sql[args_start:first_arg_end].strip()

        # Skip if the array argument is already wrapped with ARRAY_COMPACT,
        # but continue scanning from *inside* so nested ARRAY_TO_STRING calls
        # further down in the expression are still processed.
        if re.match(r'ARRAY_COMPACT\s*\(', first_arg, re.IGNORECASE):
            result.append(func_prefix)
            i = args_start
            continue

        # Reconstruct with ARRAY_COMPACT wrapping the first argument.
        # Recurse into first_arg so nested ARRAY_TO_STRING calls are also wrapped.
        rest = sql[first_arg_end:j]  # ", delimiter)"
        processed_first_arg = wrap_array_to_string_with_compact(first_arg)
        result.append(f"{func_prefix}ARRAY_COMPACT({processed_first_arg}){rest}")
        i = j

    return ''.join(result)


def _has_negated_like_operator(sql: str, left_start: int) -> bool:
    """
    Detect whether the LIKE/ILIKE ANY/ALL expression whose left operand starts at
    left_start came from a Postgres negated operator ("x NOT ILIKE ANY/ALL(arr)").

    sqlglot renders that form as "NOT x ILIKE ANY/ALL(arr)" without parentheses,
    while an explicit "NOT (x ILIKE ...)" keeps its parentheses. So a bare NOT
    keyword immediately before the left expression identifies the negated operator,
    which requires flipping the ANY/ALL quantifier when the NOT is left outside
    the UDF call (De Morgan).
    """
    return re.search(r'\bNOT\s*$', sql[:left_start], re.IGNORECASE) is not None


def convert_like_any_to_regexp_like(sql: str) -> str:
    """
    Convert LIKE/ILIKE ANY(dynamic_expr) to ILIKE_ANY/LIKE_ANY UDF calls for Snowflake.

    Snowflake's LIKE ANY only accepts literal string patterns — not array column references
    or subqueries. The ILIKE_ANY / LIKE_ANY UDFs (defined below) accept a pipe-separated
    VARCHAR built with ARRAY_TO_STRING or LISTAGG and handle the REGEXP_LIKE logic internally.

    Handles:
      col ILIKE ANY(array_col)
        → {{ function('ILIKE_ANY') }}(col, ARRAY_TO_STRING(array_col, '|'))
      col LIKE ANY(array_col)
        → {{ function('LIKE_ANY') }}(col, ARRAY_TO_STRING(array_col, '|'))
      col ILIKE ANY((SELECT pattern_col FROM T))
        → {{ function('ILIKE_ANY') }}(col, (SELECT LISTAGG(pattern_col, '|') FROM T))
      col LIKE ANY((SELECT pattern_col FROM T))
        → {{ function('LIKE_ANY') }}(col, (SELECT LISTAGG(pattern_col, '|') FROM T))

    Companion UDFs to create in Snowflake:
      CREATE OR REPLACE FUNCTION ILIKE_ANY(col VARCHAR, pipe_separated_patterns VARCHAR)
        RETURNS BOOLEAN AS $$
          REGEXP_LIKE(col, '^(' || REPLACE(pipe_separated_patterns, '%', '.*') || ')$', 'i')
        $$;

      CREATE OR REPLACE FUNCTION LIKE_ANY(col VARCHAR, pipe_separated_patterns VARCHAR)
        RETURNS BOOLEAN AS $$
          REGEXP_LIKE(col, '^(' || REPLACE(pipe_separated_patterns, '%', '.*') || ')$')
        $$;

    Notes:
      - ILIKE maps to ILIKE_ANY (case-insensitive); LIKE maps to LIKE_ANY (case-sensitive).
      - LIKE wildcard % is converted to regex .* inside the UDF; _ is not currently translated.
      - LIKE ANY(ARRAY_CONSTRUCT(...)) with literal strings is handled separately upstream
        and is NOT processed by this function.
      - Negated-operator form: Postgres "x NOT ILIKE ANY(arr)" means "at least one pattern
        does not match" (NOT ILIKE is the operator, ANY the quantifier). sqlglot renders it
        as "NOT x ILIKE ANY(arr)" (no parens), which would wrongly become
        NOT ILIKE_ANY(x, arr) = "no pattern matches". De Morgan requires flipping the
        quantifier: NOT ILIKE_ALL(x, arr). An explicit "NOT (x ILIKE ANY(arr))" keeps its
        parens through sqlglot, so the two forms are distinguishable here: a bare NOT
        directly before the left expression signals the negated operator.
    """
    max_iterations = 200

    # ── Subquery form: LIKE/ILIKE ANY((SELECT column FROM ...)) ──────────────
    select_pattern = re.compile(
        r"(I?LIKE)\s+ANY\s*\(\s*\(\s*SELECT\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(FROM\s+.*?)\)\s*\)",
        re.IGNORECASE | re.DOTALL,
    )
    for _ in range(max_iterations):
        match = select_pattern.search(sql)
        if not match:
            break
        like_op = match.group(1).upper()
        column = match.group(2).strip()
        rest_of_query = ' '.join(match.group(3).strip().split())
        left_result = _find_left_expr_before_operator(sql, match.start())
        if not left_result:
            break
        left_expr, left_start = left_result
        if _has_negated_like_operator(sql, left_start):
            # "x NOT ILIKE ANY(arr)": the NOT stays outside, so flip ANY -> ALL.
            udf_name = "ILIKE_ALL" if like_op == 'ILIKE' else "LIKE_ALL"
        else:
            udf_name = "ILIKE_ANY" if like_op == 'ILIKE' else "LIKE_ANY"
        replacement = (
            f"{{{{ function('{udf_name}') }}}}({left_expr}, "
            f"(SELECT LISTAGG(ARRAY_TO_STRING({column}, '|'), '|') {rest_of_query}))"
        )
        sql = sql[:left_start] + replacement + sql[match.end():]

    # ── Column-reference form: LIKE/ILIKE ANY(array_column) ──────────────────
    # Skips ARRAY_CONSTRUCT (already unwrapped to literal list by upstream step).
    col_pattern = re.compile(
        r"(I?LIKE)\s+ANY\s*\(\s*(?!ARRAY_CONSTRUCT\b)([a-zA-Z_][a-zA-Z0-9_\.]*)\s*\)",
        re.IGNORECASE,
    )
    for _ in range(max_iterations):
        match = col_pattern.search(sql)
        if not match:
            break
        like_op = match.group(1).upper()
        array_col = match.group(2)
        left_result = _find_left_expr_before_operator(sql, match.start())
        if not left_result:
            break
        left_expr, left_start = left_result
        if _has_negated_like_operator(sql, left_start):
            # "x NOT ILIKE ANY(arr)": the NOT stays outside, so flip ANY -> ALL.
            udf_name = "ILIKE_ALL" if like_op == 'ILIKE' else "LIKE_ALL"
        else:
            udf_name = "ILIKE_ANY" if like_op == 'ILIKE' else "LIKE_ANY"
        replacement = f"{{{{ function('{udf_name}') }}}}({left_expr}, ARRAY_TO_STRING({array_col}, '|'))"
        sql = sql[:left_start] + replacement + sql[match.end():]

    return sql


def convert_like_all_to_regexp_like(sql: str) -> str:
    """
    Convert LIKE/ILIKE ALL(dynamic_expr) to ILIKE_ALL/LIKE_ALL UDF calls for Snowflake.

    Snowflake has no native ILIKE ALL with dynamic array arguments.  The ILIKE_ALL / LIKE_ALL
    UDFs accept a pipe-separated VARCHAR built with ARRAY_TO_STRING or LISTAGG and check
    that the column matches EVERY pattern (AND logic), unlike ILIKE_ANY which uses OR.

    Handles:
      col ILIKE ALL(array_col)
        → {{ function('ILIKE_ALL') }}(col, ARRAY_TO_STRING(array_col, '|'))
      col LIKE ALL(array_col)
        → {{ function('LIKE_ALL') }}(col, ARRAY_TO_STRING(array_col, '|'))
      col ILIKE ALL((SELECT pattern_col FROM T))
        → {{ function('ILIKE_ALL') }}(col, (SELECT LISTAGG(ARRAY_TO_STRING(pattern_col, '|'), '|') FROM T))
      col LIKE ALL((SELECT pattern_col FROM T))
        → {{ function('LIKE_ALL') }}(col, (SELECT LISTAGG(ARRAY_TO_STRING(pattern_col, '|'), '|') FROM T))

    Companion UDFs to create in Snowflake (JavaScript, because ALL requires iterating every pattern):
      CREATE OR REPLACE FUNCTION ILIKE_ALL(col VARCHAR, pipe_separated_patterns VARCHAR)
        RETURNS BOOLEAN LANGUAGE JAVASCRIPT AS
        $$
          if (PIPE_SEPARATED_PATTERNS === null || COL === null) return null;
          const patterns = PIPE_SEPARATED_PATTERNS.split('|').filter(p => p.length > 0);
          if (patterns.length === 0) return null;
          return patterns.every(p => {
            const rx = new RegExp('^' + p.replace(/%/g, '.*').replace(/_/g, '.') + '$', 'i');
            return rx.test(COL);
          });
        $$;

      CREATE OR REPLACE FUNCTION LIKE_ALL(col VARCHAR, pipe_separated_patterns VARCHAR)
        RETURNS BOOLEAN LANGUAGE JAVASCRIPT AS
        $$
          if (PIPE_SEPARATED_PATTERNS === null || COL === null) return null;
          const patterns = PIPE_SEPARATED_PATTERNS.split('|').filter(p => p.length > 0);
          if (patterns.length === 0) return null;
          return patterns.every(p => {
            const rx = new RegExp('^' + p.replace(/%/g, '.*').replace(/_/g, '.') + '$');
            return rx.test(COL);
          });
        $$;

    Notes:
      - ILIKE maps to ILIKE_ALL (case-insensitive); LIKE maps to LIKE_ALL (case-sensitive).
      - Negated-operator form: Postgres "x NOT ILIKE ALL(arr)" means "no pattern matches"
        (NOT ILIKE is the operator, ALL the quantifier), which sqlglot renders as
        "NOT x ILIKE ALL(arr)" without parens. Keeping the NOT outside therefore requires
        flipping the quantifier: NOT {{ function('ILIKE_ANY') }}(x, arr). An explicit
        "NOT (x ILIKE ALL(arr))" keeps its parens through sqlglot and correctly stays
        NOT {{ function('ILIKE_ALL') }}(x, arr).
    """
    max_iterations = 200
    op_pattern = re.compile(r'(I?LIKE)\s+ALL\s*\(', re.IGNORECASE)

    for _ in range(max_iterations):
        match = op_pattern.search(sql)
        if not match:
            break

        like_op = match.group(1).upper()
        udf_name = "ILIKE_ALL" if like_op == 'ILIKE' else "LIKE_ALL"

        # Use paren counting to extract the full argument of ALL(...)
        arg_start = match.end()  # position right after the opening '('
        paren_depth = 1
        i = arg_start
        while i < len(sql) and paren_depth > 0:
            if sql[i] == '(':
                paren_depth += 1
            elif sql[i] == ')':
                paren_depth -= 1
            i += 1
        # sql[arg_start:i-1] is the raw argument (may have leading/trailing whitespace/parens)
        raw_arg = sql[arg_start:i - 1].strip()
        all_end = i  # position after the closing ')' of ALL(...)

        # Strip redundant outer parentheses to reach the core expression
        core = raw_arg
        while core.startswith('(') and core.endswith(')'):
            # Verify the parens are balanced around the whole string
            depth = 0
            balanced_at_start = True
            for ch_i, ch in enumerate(core):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                if depth == 0 and ch_i < len(core) - 1:
                    balanced_at_start = False
                    break
            if balanced_at_start and depth == 0:
                core = core[1:-1].strip()
            else:
                break

        # Find the left-hand column expression
        left_result = _find_left_expr_before_operator(sql, match.start())
        if not left_result:
            break
        left_expr, left_start = left_result

        if _has_negated_like_operator(sql, left_start):
            # "x NOT ILIKE ALL(arr)" (Postgres: no pattern matches): the NOT stays
            # outside the UDF call, so flip ALL -> ANY (De Morgan).
            udf_name = "ILIKE_ANY" if like_op == 'ILIKE' else "LIKE_ANY"

        # Build replacement based on whether the argument is a subquery or a column ref
        select_m = re.match(
            r'SELECT\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(FROM\s+.*)',
            core,
            re.IGNORECASE | re.DOTALL,
        )
        if select_m:
            column = select_m.group(1).strip()
            rest_of_query = ' '.join(select_m.group(2).strip().split())
            replacement = (
                f"{{{{ function('{udf_name}') }}}}({left_expr}, "
                f"(SELECT LISTAGG(ARRAY_TO_STRING({column}, '|'), '|') {rest_of_query}))"
            )
        elif re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', core):
            # Simple column reference
            replacement = f"{{{{ function('{udf_name}') }}}}({left_expr}, ARRAY_TO_STRING({core}, '|'))"
        else:
            # Unknown form — leave as-is to avoid mangling unrecognised patterns
            break

        sql = sql[:left_start] + replacement + sql[all_end:]

    return sql


def remove_date_part_day(sql: str) -> str:
    """
    Remove DATE_PART('day', ...) or DATE_PART(day, ...) wrappers.
    Properly handles nested parentheses and only removes when the content is a date subtraction.
    
    In Snowflake, date subtraction returns days directly, so DATE_PART(day, ...) is redundant.
    """
    result = []
    i = 0
    
    while i < len(sql):
        # Look for DATE_PART pattern (case-insensitive)
        match = re.match(r'DATE_PART\s*\(\s*["\']?day["\']?\s*,\s*', sql[i:], re.IGNORECASE)
        if match:
            # Found DATE_PART(day,
            start_pos = i
            i += match.end()
            
            # Now find the matching closing parenthesis by counting parens
            paren_count = 1
            content_start = i
            
            while i < len(sql) and paren_count > 0:
                if sql[i] == '(':
                    paren_count += 1
                elif sql[i] == ')':
                    paren_count -= 1
                i += 1
            
            if paren_count == 0:
                # Found the matching closing paren
                content = sql[content_start:i-1]
                # Remove the DATE_PART wrapper and just keep the content
                result.append(content)
            else:
                # Unmatched parentheses, keep original
                result.append(sql[start_pos:i])
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def fix_flatten_column_qualifiers(sql: str) -> str:
    """
    Fix incorrect table alias prefixes in FLATTEN subqueries.
    
    Sqlglot incorrectly adds the alias prefix to ALL column references,
    but only columns in the explicit FLATTEN column list should have it.
    
    Example: d.datum is valid, but d.controledatum (from outer context) is not.
    """
    pattern = r'TABLE\s*\(\s*FLATTEN\s*\(\s*INPUT\s*=>\s*[^)]+\)\s*\)\s*AS\s+(\w+)\s*\(\s*([^)]+)\s*\)'
    
    for match in re.finditer(pattern, sql, re.IGNORECASE):
        table_alias = match.group(1)
        column_list_str = match.group(2)
        valid_columns = set(col.strip() for col in column_list_str.split(','))
        
        # Replace alias.column with just column if column is not in the FLATTEN result
        ref_pattern = rf'\b{re.escape(table_alias)}\.(\w+)\b'
        
        def replace_func(m):
            column_name = m.group(1)
            if column_name not in valid_columns:
                return column_name  # Remove invalid prefix
            return m.group(0)  # Keep valid prefix
        
        sql = re.sub(ref_pattern, replace_func, sql)
    
    return sql

def postprocess_date_arithmetic_snowflake(sql: str, month_columns: list = None) -> str:
    """
    Post-process date arithmetic after SQLglot conversion.
    SQLglot converts PostgreSQL date arithmetic to patterns like:
    - CAST(expr AS DATE) - CAST('N unit' AS INTERVAL) 
    - CAST(expr AS DATE) + CAST('N unit' AS INTERVAL)
    - expr - INTERVAL 'N' UNIT
    - expr + INTERVAL 'N' UNIT
    
    These need to be converted to Snowflake DATEADD:
    - DATEADD(UNIT, -N, CAST(expr AS DATE))
    - DATEADD(UNIT, N, CAST(expr AS DATE))
    
    Args:
        sql: SQL string to process
        month_columns: List of column names (unqualified) that represent months
                       and should be converted to DATEADD(month, ...) when used
                       in date arithmetic. Example: ['validtime', 'retention_months']
    
    This handles:
    - Simple date arithmetic
    - Nested cases (e.g., within DATE_TRUNC, DATE_PART, etc.)
    - Chained DATEADD expressions
    - Complex expressions with arithmetic operations
    - Dynamic intervals: concat(expr, ' unit')::interval or (expr || ' unit')::interval
    - Month-based columns: date +/- month_column (when month_columns specified)
    """
    
    # Default month columns if none specified
    if month_columns is None:
        month_columns = ['validtime']  # Default list

    # ============================================================================
    # STEP 0: Direct rewrite for common CAST(date + dynamic interval AS DATE)
    # ============================================================================
    # Restrict to simple identifier expressions to avoid accidental wide matches.
    direct_dynamic_date_cast_pattern = re.compile(
        r"CAST\s*\(\s*\(\s*([A-Za-z_][\w\.]*)\s*([+-])\s*CAST\s*\(\s*\(\s*([A-Za-z_][\w\.]*)\s*\|\|\s*'([^']+)'\s*\)\s*AS\s+INTERVAL\s*\)\s*\)\s*AS\s+DATE\s*\)",
        re.IGNORECASE,
    )

    def _rewrite_direct_dynamic_date_cast(match):
        date_expr = match.group(1).strip()
        operator = match.group(2)
        amount_expr = match.group(3).strip()
        unit_str = match.group(4).strip()

        unit_only_match = re.match(r'^\s*(year|month|week|day|hour|minute|second)s?\s*$', unit_str, re.IGNORECASE)
        if not unit_only_match:
            return match.group(0)

        unit = unit_only_match.group(1).upper()
        if operator == '-':
            return f"DATEADD({unit}, -({amount_expr}), {date_expr})"
        return f"DATEADD({unit}, {amount_expr}, {date_expr})"

    sql = direct_dynamic_date_cast_pattern.sub(_rewrite_direct_dynamic_date_cast, sql)

    # Also handle direct arithmetic without outer CAST(... AS DATE):
    #   date_expr +/- CAST((amount_expr || ' day') AS INTERVAL)
    dynamic_interval_arith_pattern = re.compile(
        r"([A-Za-z_][\w\.]*)\s*([+-])\s*CAST\s*\(\s*\(\s*(.+?)\s*\|\|\s*'([^']+)'\s*\)\s*AS\s+INTERVAL\s*\)",
        re.IGNORECASE | re.DOTALL,
    )

    def _rewrite_dynamic_interval_arith(match):
        date_expr = match.group(1).strip()
        operator = match.group(2)
        amount_expr = match.group(3).strip()
        unit_str = match.group(4).strip()

        unit_only_match = re.match(r'^\s*(year|month|week|day|hour|minute|second)s?\s*$', unit_str, re.IGNORECASE)
        if not unit_only_match:
            return match.group(0)

        unit = unit_only_match.group(1).upper()
        if operator == '-':
            return f"DATEADD({unit}, -({amount_expr}), {date_expr})"
        return f"DATEADD({unit}, {amount_expr}, {date_expr})"

    sql = dynamic_interval_arith_pattern.sub(_rewrite_dynamic_interval_arith, sql)
    
    # ============================================================================
    # STEP 1: Handle dynamic interval patterns (concat/|| with ::interval)
    # ============================================================================
    # Pattern: concat(expr, ' unit')::interval or (expr || ' unit')::interval
    # Convert to INTERVAL patterns that can be handled by subsequent processing
    
    dynamic_interval_pattern = re.compile(
        r"concat\s*\(\s*([^,]+?)\s*,\s*'([^']+)'\s*\)\s*::\s*interval|"
        r"\(\s*([^)]+?)\s*\|\|\s*'([^']+)'\s*\)\s*::\s*interval",
        re.IGNORECASE
    )
    
    max_dynamic_iterations = 5
    for iteration in range(max_dynamic_iterations):
        original_sql = sql
        matches = list(dynamic_interval_pattern.finditer(sql))
        
        if not matches:
            break
        
        # Process matches in reverse order to preserve positions
        for match in reversed(matches):
            # Extract the expression and unit from either concat or || pattern
            if match.group(1):  # concat pattern
                expr = match.group(1).strip()
                unit_str = match.group(2).strip()
            elif match.group(3):  # || pattern (raw PostgreSQL form)
                expr = match.group(3).strip()
                unit_str = match.group(4).strip()
            else:  # CAST((expr || ' ' || 'unit') AS INTERVAL) — sqlglot output form
                expr = match.group(5).strip()
                unit_str = match.group(6).strip()
            
            # Parse the unit string (e.g., "5 day" -> fixed amount, or "day" -> dynamic amount from expr)
            # For concat(expr, ' day')::interval, the expr IS the amount variable
            unit_match = re.match(r'^\s*(\d+)\s+(year|month|week|day|hour|minute|second)s?\s*$', unit_str, re.IGNORECASE)
            unit_only_match = re.match(r'^\s*(year|month|week|day|hour|minute|second)s?\s*$', unit_str, re.IGNORECASE)
            
            if unit_match:
                # Fixed interval (e.g., concat(something, '5 day')) - rare but possible
                amount = unit_match.group(1)
                unit = unit_match.group(2).upper()
                replacement = f"INTERVAL '{amount}' {unit}"
            elif unit_only_match:
                # Dynamic interval: concat(expr, ' day') means expr days
                unit = unit_only_match.group(1).upper()

                # Mark it specially so we can handle it in date arithmetic
                replacement = f"__DYNAMIC_INTERVAL__({expr}, {unit})"
            else:
                # Can't parse the unit string, skip
                continue
            
            # Replace in SQL
            sql = sql[:match.start()] + replacement + sql[match.end():]
        
        if sql == original_sql:
            break
    
    # ============================================================================
    # STEP 1b: Handle dynamic intervals in date arithmetic
    # ============================================================================
    # Now handle __DYNAMIC_INTERVAL__(expr, UNIT) when used in date arithmetic
    # Pattern: date_expr +/- __DYNAMIC_INTERVAL__(amount_expr, UNIT)
    
    def find_date_expression_start(text, end_pos):
        """Find start of date expression, handling quotes, parens, casts"""
        if end_pos <= 0:
            return 0
        
        pos = end_pos - 1
        
        # Handle closing paren
        if text[pos] == ')':
            paren_count = 1
            pos -= 1
            while pos >= 0 and paren_count > 0:
                if text[pos] == ')':
                    paren_count += 1
                elif text[pos] == '(':
                    paren_count -= 1
                pos -= 1
            pos += 1
            
            # Check for keyword/function before paren
            temp = pos - 1
            while temp > 0 and text[temp].isspace():
                temp -= 1
            if temp >= 0 and (text[temp].isalnum() or text[temp] == '_'):
                while temp >= 0 and (text[temp].isalnum() or text[temp] == '_'):
                    temp -= 1
                return temp + 1
            return pos
        
        # Handle quoted string with optional ::cast
        if text[pos] == "'":
            # Find opening quote
            pos -= 1
            while pos >= 0:
                if text[pos] == "'" and (pos == 0 or text[pos-1] != '\\'):
                    break
                pos -= 1
            return pos if pos >= 0 else 0
        
        # Handle simple identifiers, keeping :: for casts
        # Work backwards through alphanumeric, _, ., ::, quotes
        while pos >= 0:
            if text[pos] == "'":
                # Hit a quote going backwards, find its start
                pos -= 1
                while pos >= 0:
                    if text[pos] == "'" and (pos == 0 or text[pos-1] != '\\'):
                        break
                    pos -= 1
                pos -= 1  # Move before the opening quote
            elif text[pos] in (' ', '\t', '\n'):
                # Whitespace - check if we should continue (for :: patterns)
                temp = pos - 1
                while temp >= 0 and text[temp] in (' ', '\t', '\n'):
                    temp -= 1
                if temp >= 1 and text[temp] == ':' and text[temp-1] == ':':
                    pos = temp - 1
                else:
                    break
            elif text[pos].isalnum() or text[pos] in ('_', '.', ':', '-'):
                pos -= 1
            else:
                break
        
        return pos + 1
    
    for iteration in range(5):
        original_sql = sql
        
        # Find all __DYNAMIC_INTERVAL__ markers
        marker_pattern = re.compile(r'__DYNAMIC_INTERVAL__\(\s*([^,]+?)\s*,\s*(\w+)\s*\)')
        matches = []
        
        for match in marker_pattern.finditer(sql):
            matches.append((match.start(), match.end(), match.group(1), match.group(2)))
        
        if not matches:
            break
        
        # Process matches in reverse order to preserve positions
        for match_start, match_end, amount_expr, unit in reversed(matches):
            # Search backwards from match_start to find the operator (+/-)
            search_pos = match_start - 1
            while search_pos >= 0 and sql[search_pos].isspace():
                search_pos -= 1
            
            if search_pos < 0 or sql[search_pos] not in ('+', '-'):
                # No operator found, skip
                continue
            
            operator = sql[search_pos]
            
            # Now find the date expression before the operator
            # Need to handle quoted strings, parentheses, casts, etc.
            expr_end = search_pos
            
            # Skip whitespace before operator
            while expr_end > 0 and sql[expr_end - 1].isspace():
                expr_end -= 1
            
            # Find the start of the expression
            expr_start = find_date_expression_start(sql, expr_end)
            
            if expr_start is None or expr_start >= expr_end:
                continue
            
            date_expr = sql[expr_start:expr_end].strip()
            
            if not date_expr:
                continue
            
            # Build DATEADD
            if operator == '-':
                dateadd_expr = f"DATEADD({unit}, -({amount_expr}), {date_expr})"
            else:
                dateadd_expr = f"DATEADD({unit}, {amount_expr}, {date_expr})"
            
            # Replace in SQL
            sql = sql[:expr_start] + dateadd_expr + sql[match_end:]
        
        if sql == original_sql:
            break

    # ============================================================================
    # STEP 1b2: Safeguard for malformed leaked markers
    # ============================================================================
    # Rare malformed output can look like:
    # __DYNAMIC_INTERVAL__(date_expr + CAST((amount, DAY)) AS DATE)
    # Convert it directly to DATEADD(DAY, amount, date_expr)
    malformed_marker_pattern = re.compile(
        r"__DYNAMIC_INTERVAL__\(\s*(.+?)\s*\+\s*CAST\s*\(\s*\(\s*([^,]+?)\s*,\s*(\w+)\s*\)\s*\)\s*AS\s+DATE\s*\)",
        re.IGNORECASE | re.DOTALL,
    )

    def _replace_malformed_marker(match):
        date_expr = match.group(1).strip()
        amount_expr = match.group(2).strip()
        unit = match.group(3).strip().upper()
        return f"DATEADD({unit}, {amount_expr}, {date_expr})"

    if '__DYNAMIC_INTERVAL__(' in sql:
        sql = malformed_marker_pattern.sub(_replace_malformed_marker, sql)
    
    # ============================================================================
    # STEP 1c: Handle compound intervals (e.g., '3 months 1 day')
    # ============================================================================
    # Pattern: CAST('N unit1 M unit2 ...' AS INTERVAL) or '...'::interval
    # Convert to nested DATEADD: DATEADD(unit2, -M, DATEADD(unit1, -N, date))
    # NOTE: Only match intervals with 2+ units (compound intervals)
    
    compound_interval_pattern = re.compile(
        r"CAST\s*\(\s*'(\d+\s+(?:year|month|week|day|hour|minute|second)s?(?:\s+\d+\s+(?:year|month|week|day|hour|minute|second)s?)+)'\s+AS\s+INTERVAL\s*\)|"
        r"'(\d+\s+(?:year|month|week|day|hour|minute|second)s?(?:\s+\d+\s+(?:year|month|week|day|hour|minute|second)s?)+)'::\s*interval",
        re.IGNORECASE
    )
    
    max_compound_iterations = 10
    for iteration in range(max_compound_iterations):
        original_sql = sql
        
        matches = list(compound_interval_pattern.finditer(sql))
        if not matches:
            break
        
        # Process in reverse order to preserve positions
        for match in reversed(matches):
            interval_str = match.group(1) or match.group(2)
            interval_start = match.start()
            interval_end = match.end()
            
            # Parse the compound interval into components
            # Match all "N unit" pairs
            component_pattern = re.compile(r'(\d+)\s+(year|month|week|day|hour|minute|second)s?', re.IGNORECASE)
            components = component_pattern.findall(interval_str)
            
            if not components:
                continue
            
            # Find the operator before the interval (+ or -)
            search_pos = interval_start - 1
            while search_pos >= 0 and sql[search_pos].isspace():
                search_pos -= 1
            
            if search_pos < 0 or sql[search_pos] not in ('+', '-'):
                continue
            
            operator = sql[search_pos]
            
            # Find the date expression before the operator
            expr_end = search_pos
            while expr_end > 0 and sql[expr_end - 1].isspace():
                expr_end -= 1
            
            expr_start = find_date_expression_start(sql, expr_end)
            if expr_start is None or expr_start >= expr_end:
                continue
            
            date_expr = sql[expr_start:expr_end].strip()
            if not date_expr:
                continue
            
            # Build nested DATEADD calls
            # Start from innermost (first component) and work outward
            result_expr = date_expr
            for amount, unit in components:
                unit = unit.upper().rstrip('S')
                if operator == '-':
                    result_expr = f"DATEADD({unit}, -{amount}, {result_expr})"
                else:
                    result_expr = f"DATEADD({unit}, {amount}, {result_expr})"
            
            # Replace in SQL
            sql = sql[:expr_start] + result_expr + sql[interval_end:]
        
        if sql == original_sql:
            break

    # ============================================================================
    # STEP 2: Standard interval processing
    # ============================================================================
    
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
    
    def extract_balanced_parens(text, start_pos):
        """Extract content between balanced parentheses starting at start_pos"""
        if start_pos >= len(text) or text[start_pos] != '(':
            return None, start_pos
        
        end_pos = find_matching_paren(text, start_pos)
        if end_pos == -1:
            return None, start_pos
        
        return text[start_pos:end_pos], end_pos
    
    # Pattern for INTERVAL formats that may appear:
    # 1. INTERVAL 'N UNIT' or INTERVAL N UNIT (SQLglot clean format)
    # 2. CAST('N unit' AS INTERVAL) (SQLglot outputs '3 years' as a single string)
    # 3. 'N unit'::interval (Original PostgreSQL format when SQLglot fails)
    interval_pattern = re.compile(
        r"(?:INTERVAL\s+'([0-9.]+)\s+(YEAR|MONTH|WEEK|DAY|HOUR|MINUTE|SECOND)S?'|"
        r"INTERVAL\s+([0-9.]+)\s+(YEAR|MONTH|WEEK|DAY|HOUR|MINUTE|SECOND)S?|"
        r"CAST\s*\(\s*'([0-9.]+)\s+(years?|months?|weeks?|days?|hours?|minutes?|seconds?)'\s+AS\s+INTERVAL\s*\)|"
        r"'([0-9.]+)\s+(YEAR|MONTH|WEEK|DAY|HOUR|MINUTE|SECOND)S?'::\s*interval)",
        re.IGNORECASE
    )

    
    # Main conversion loop - process the SQL iteratively
    max_iterations = 500  # Increased to handle very large SQL files with many intervals
    for iteration in range(max_iterations):
        original_sql = sql
        result = []
        i = 0

        
        while i < len(sql):
            # Look for interval pattern
            interval_match = interval_pattern.search(sql, i)
            
            if not interval_match:
                # No more intervals found
                result.append(sql[i:])
                break
            
            # Found an interval - need to find what comes before it
            interval_start = interval_match.start()
            interval_end = interval_match.end()
            
            # Extract amount and unit from whichever pattern matched
            if interval_match.group(1):  # INTERVAL 'N UNIT' format (with quotes)
                amount = interval_match.group(1)
                unit = interval_match.group(2).upper().rstrip('S')
            elif interval_match.group(3):  # INTERVAL N UNIT format (without quotes)
                amount = interval_match.group(3)
                unit = interval_match.group(4).upper().rstrip('S')
            elif interval_match.group(5):  # CAST('N unit' AS INTERVAL) format
                amount = interval_match.group(5)
                unit = interval_match.group(6).upper().rstrip('S')
            else:  # 'N unit'::interval format (PostgreSQL original)
                amount = interval_match.group(7)
                unit = interval_match.group(8).upper().rstrip('S')
            
            # Find the operator before the interval (+ or -)
            # Walk backward from interval_start to find the operator
            search_pos = interval_start - 1
            while search_pos >= i and sql[search_pos].isspace():
                search_pos -= 1
            
            if search_pos < i or sql[search_pos] not in ('+', '-'):
                # No valid operator found, skip this interval
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            operator = sql[search_pos]
            operator_pos = search_pos
            
            # Now find the left expression before the operator
            # This could be:
            # - CAST(...) 
            # - A column name
            # - DATE_TRUNC(...)
            # - Any function call
            # - A parenthesized expression
            
            expr_end = operator_pos
            expr_start = expr_end - 1
            
            # Skip whitespace before operator
            while expr_start >= i and sql[expr_start].isspace():
                expr_start -= 1
            
            if expr_start < i:
                # Can't find expression
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            # Check what type of expression we have
            if sql[expr_start] == ')':
                # Ends with ) - could be CAST(...), function(...), or (...)
                # Find the matching opening paren
                paren_count = 1
                expr_start -= 1
                while expr_start >= i and paren_count > 0:
                    if sql[expr_start] == ')':
                        paren_count += 1
                    elif sql[expr_start] == '(':
                        paren_count -= 1
                    expr_start -= 1
                expr_start += 1
                
                # Check if there's a function/keyword before the opening paren
                temp = expr_start - 1
                while temp >= i and sql[temp].isspace():
                    temp -= 1
                
                if temp >= i and (sql[temp].isalnum() or sql[temp] == '_'):
                    # There's a function name, include it
                    while temp >= i and (sql[temp].isalnum() or sql[temp] == '_'):
                        temp -= 1
                    expr_start = temp + 1
            else:
                # Simple column or literal
                # Go back to find the start (alphanumeric, underscore, dot, quotes, dollar, curly braces)
                while expr_start >= i and (sql[expr_start].isalnum() or sql[expr_start] in ('_', '.', "'", '"', '$', '{', '}', ':')):
                    expr_start -= 1
                expr_start += 1
            
            # Extract the base expression
            base_expr = sql[expr_start:expr_end].strip()
            
            if not base_expr:
                # Couldn't find base expression
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            # Build the DATEADD expression
            amount_val = f"-{amount}" if operator == '-' else amount
            dateadd_expr = f"DATEADD({unit}, {amount_val}, {base_expr})"
            
            # Add everything before the base expression to result
            result.append(sql[i:expr_start])
            # Add the DATEADD replacement
            result.append(dateadd_expr)
            # Add everything after the interval
            result.append(sql[interval_end:])
            
            # Break to rebuild SQL and restart
            break
        
        sql = ''.join(result)
        
        # If no changes were made, we're done
        if sql == original_sql:
            logging.debug(f"[Date Arithmetic] No more intervals to convert after {iteration} iterations")
            break
    else:
        # Loop completed without break - hit max iterations
        logging.warning(f"[Date Arithmetic] Hit max_iterations ({max_iterations}) - some intervals may remain unconverted")
    
    # Second pass: Handle expr * interval'N unit' patterns  
    # Find all occurrences of "* interval'" and work backwards to get the expression
    max_mult_iterations = 10
    for mult_iter in range(max_mult_iterations):
        original_sql = sql
        
        # Find "* interval'"
        mult_marker = re.compile(r"\*\s*interval\s*'", re.IGNORECASE)
        
        result = []
        i = 0
        
        while i < len(sql):
            match = mult_marker.search(sql, i)
            if not match:
                result.append(sql[i:])
                break
            
            marker_start = match.start()
            marker_end = match.end()
            
            # Extract the interval part: 'N unit'
            interval_part_match = re.match(r"([0-9.]+)\s+(year|month|week|day|hour|minute|second)s?'", sql[marker_end:], re.IGNORECASE)
            if not interval_part_match:
                result.append(sql[i:marker_end])
                i = marker_end
                continue
            
            amount = interval_part_match.group(1)
            unit = interval_part_match.group(2).upper()
            interval_end = marker_end + interval_part_match.end()
            
            # Work backwards from marker_start to find the expression being multiplied
            # Skip whitespace before *
            expr_end = marker_start
            while expr_end > i and sql[expr_end - 1].isspace():
                expr_end -= 1
            
            # Find the start of the expression
            expr_start = expr_end - 1
            
            if expr_start < i:
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            if sql[expr_start] == ')':
                # Balanced parentheses
                paren_count = 1
                expr_start -= 1
                while expr_start >= i and paren_count > 0:
                    if sql[expr_start] == ')':
                        paren_count += 1
                    elif sql[expr_start] == '(':
                        paren_count -= 1
                    expr_start -= 1
                expr_start += 1
                
                # Check for function name before the paren
                temp = expr_start - 1
                while temp >= i and sql[temp].isspace():
                    temp -= 1
                    
                if temp >= i and (sql[temp].isalnum() or sql[temp] == '_'):
                    # There's a function name
                    while temp >= i and (sql[temp].isalnum() or sql[temp] == '_'):
                        temp -= 1
                    expr_start = temp + 1
            else:
                # Simple identifier
                while expr_start >= i and (sql[expr_start].isalnum() or sql[expr_start] in ('_', '.')):
                    expr_start -= 1
                expr_start += 1
            
            expr = sql[expr_start:expr_end].strip()
            
            if not expr or expr[0] in ('+', '-', '*', '/', ','):
                # Invalid expression start (but allow parentheses)
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            # Build the amount expression
            if amount == '1' or amount == '1.0':
                amount_expr = expr
            else:
                amount_expr = f"{amount} * ({expr})"
            
            # Replace with INTERVAL 'expr' UNIT format for the next pass
            result.append(sql[i:expr_start])
            result.append(f"INTERVAL '{amount_expr}' {unit}")
            i = interval_end
        
        sql = ''.join(result)
        
        if sql == original_sql:
            break
    
    # Then run another iteration of the main loop to convert these to DATEADD
    # if they're part of addition/subtraction
    for iteration in range(3):  # A few more iterations for the new patterns
        original_sql = sql
        result = []
        i = 0
        
        while i < len(sql):
            # Look for interval pattern (now including our generated patterns with expressions)
            # Updated pattern to match INTERVAL 'expr' UNIT
            dynamic_interval_pattern = re.compile(
                r"INTERVAL\s+'([^']+)'\s+(YEAR|MONTH|WEEK|DAY|HOUR|MINUTE|SECOND)S?",
                re.IGNORECASE
            )
            interval_match = dynamic_interval_pattern.search(sql, i)
            
            if not interval_match:
                result.append(sql[i:])
                break
            
            interval_start = interval_match.start()
            interval_end = interval_match.end()
            amount_expr = interval_match.group(1)  # Could be numeric or expression
            unit = interval_match.group(2).upper()
            
            # Find operator before interval
            search_pos = interval_start - 1
            while search_pos >= i and sql[search_pos].isspace():
                search_pos -= 1
            
            if search_pos < i or sql[search_pos] not in ('+', '-'):
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            operator = sql[search_pos]
            operator_pos = search_pos
            
            # Find base expression before operator
            expr_end = operator_pos
            expr_start = expr_end - 1
            
            while expr_start >= i and sql[expr_start].isspace():
                expr_start -= 1
            
            if expr_start < i:
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            # Find start of expression
            if sql[expr_start] == ')':
                paren_count = 1
                expr_start -= 1
                while expr_start >= i and paren_count > 0:
                    if sql[expr_start] == ')':
                        paren_count += 1
                    elif sql[expr_start] == '(':
                        paren_count -= 1
                    expr_start -= 1
                expr_start += 1
                
                temp = expr_start - 1
                while temp >= i and sql[temp].isspace():
                    temp -= 1
                
                if temp >= i and (sql[temp].isalnum() or sql[temp] == '_'):
                    while temp >= i and (sql[temp].isalnum() or sql[temp] == '_'):
                        temp -= 1
                    expr_start = temp + 1
            else:
                while expr_start >= i and (sql[expr_start].isalnum() or sql[expr_start] in ('_', '.', "'", '"', '$', '{', '}', ':')):
                    expr_start -= 1
                expr_start += 1
            
            base_expr = sql[expr_start:expr_end].strip()
            
            if not base_expr:
                result.append(sql[i:interval_end])
                i = interval_end
                continue
            
            # Build DATEADD
            amount_val = f"-({amount_expr})" if operator == '-' else amount_expr
            dateadd_expr = f"DATEADD({unit}, {amount_val}, {base_expr})"
            
            result.append(sql[i:expr_start])
            result.append(dateadd_expr)
            i = interval_end
        
        sql = ''.join(result)
        
        if sql == original_sql:
            break
    
    # ============================================================================
    # STEP 1d: Handle split-column intervals (amount_col || ' ' || unit_col)
    # ============================================================================
    # Handles dynamic intervals built from two columns: one for the amount and one
    # for the unit (e.g. fil.validtime_amount and fil.validtime_unit).
    #
    # Snowflake requires the unit argument of DATEADD to be a literal, so a dynamic
    # unit column must be handled with a CASE expression that branches on its value.
    #
    # sqlglot-produced CAST form:
    #   date_expr - CAST((amount_col || ' ' || unit_col) AS INTERVAL)
    # Original PostgreSQL ::interval form (sqlglot failed):
    #   date_expr - (amount_col || ' ' || unit_col)::interval
    #
    # Both are converted to:
    #   CASE
    #     WHEN unit_col = 'year'  THEN DATEADD('year',  -amount_col, date_expr)
    #     WHEN unit_col = 'month' THEN DATEADD('month', -amount_col, date_expr)
    #     WHEN unit_col = 'week'  THEN DATEADD('week',  -amount_col, date_expr)
    #     WHEN unit_col = 'day'   THEN DATEADD('day',   -amount_col, date_expr)
    #   END

    def _build_dateadd_case(unit_expr: str, amount_expr: str, date_expr: str, operator: str) -> str:
        """Build a CASE expression that dispatches DATEADD on a dynamic unit column."""
        amount_val = f"-{amount_expr}" if operator == '-' else amount_expr
        # Each unit accepts both singular and plural spellings (year/years, month/months)
        whens = (
            f"WHEN {unit_expr} IN ('year', 'years') THEN DATEADD('year', {amount_val}, {date_expr})\n"
            f"    WHEN {unit_expr} IN ('month', 'months') THEN DATEADD('month', {amount_val}, {date_expr})"
        )
        return f"CASE\n    {whens}\n  END"

    split_col_interval_patterns = [
        # sqlglot-produced CAST form (whitespace/newlines may appear inside)
        re.compile(
            r"CAST\s*\(\s*\(\s*([\w.]+)\s*\|\|\s*'\s*'\s*\|\|\s*([\w.]+)\s*\)\s*AS\s+INTERVAL\s*\)",
            re.IGNORECASE,
        ),
        # Original PostgreSQL ::interval form (used when sqlglot fails)
        re.compile(
            r"\(\s*([\w.]+)\s*\|\|\s*'\s*'\s*\|\|\s*([\w.]+)\s*\)\s*::\s*interval",
            re.IGNORECASE,
        ),
    ]

    for split_col_pattern in split_col_interval_patterns:
        for _ in range(20):
            match = split_col_pattern.search(sql)
            if not match:
                break

            amount_expr = match.group(1).strip()
            unit_expr = match.group(2).strip()
            interval_start = match.start()
            interval_end = match.end()

            # Find the +/- operator immediately before the interval expression
            search_pos = interval_start - 1
            while search_pos >= 0 and sql[search_pos].isspace():
                search_pos -= 1

            if search_pos < 0 or sql[search_pos] not in ('+', '-'):
                # No arithmetic operator in context — stop to avoid an infinite loop
                break

            operator = sql[search_pos]
            left_expr, left_start = _find_left_expr_before_operator(sql, search_pos)

            if not left_expr:
                break  # Cannot extract the left-hand date expression

            case_expr = _build_dateadd_case(unit_expr, amount_expr, left_expr, operator)
            sql = sql[:left_start] + case_expr + sql[interval_end:]

    # ============================================================================
    # STEP 3: Handle simple column-based date arithmetic (for month columns)
    # ============================================================================
    # Pattern: CAST(date_expr AS DATE) +/- column_name
    # This handles cases where interval columns are represented as numeric months
    # Convert to: DATEADD(month, +/-column_name, CAST(date_expr AS DATE))
    
    max_simple_iterations = 10
    for iteration in range(max_simple_iterations):
        original_sql = sql
        
        # Build a regex pattern to match date_expr +/- column_name patterns
        # Match: (date expression) (+/-) (column name)
        # Where column name is one of the configured month columns
        
        # Create pattern for matching month columns (escape special regex chars)
        column_patterns = []
        for col in month_columns:
            # Match qualified (table.column) or unqualified (column) names
            # Use word boundaries to avoid partial matches
            escaped_col = re.escape(col)
            column_patterns.append(rf'\w+\.{escaped_col}\b|{escaped_col}\b')
        
        column_pattern = '|'.join(column_patterns)
        
        # Pattern explanation:
        # 1. Date expression: ends with AS DATE) or AS TIMESTAMP) or is a date literal/column
        # 2. Operator: + or - (with optional whitespace)
        # 3. Column: one of the configured month columns
        pattern = re.compile(
            rf'(\b(?:CAST|TRY_TO_DATE|TO_DATE|DATE_TRUNC|DATE_PART)\s*\([^)]+\)|'  # Function with parens
            rf"'\d{{4}}-\d{{2}}-\d{{2}}'(?:\s*::\s*(?:DATE|TIMESTAMP))?|"  # Date literal with optional cast
            rf'\bCURRENT_DATE\b|'  # CURRENT_DATE
            rf'\bCURRENT_TIMESTAMP\b)'  # CURRENT_TIMESTAMP
            rf'\s*([+\-])\s*'  # Operator
            rf'({column_pattern})',  # One of the month columns
            re.IGNORECASE
        )
        
        matches = list(pattern.finditer(sql))
        
        if not matches:
            break
        
        # Process matches in reverse order to preserve positions
        for match in reversed(matches):
            date_expr = match.group(1).strip()
            operator = match.group(2)
            column_expr = match.group(3).strip()
            
            # Build the DATEADD expression
            if operator == '-':
                dateadd_expr = f"DATEADD(month, -({column_expr}), {date_expr})"
            else:
                dateadd_expr = f"DATEADD(month, {column_expr}, {date_expr})"
            
            # Replace in SQL
            sql = sql[:match.start()] + dateadd_expr + sql[match.end():]
        
        if sql == original_sql:
            break
    
    return sql

def convert_postgres_to_snowflake(sql: str, function_macros: list = None, wrap_array_to_string: bool = True) -> str:
    """
    Convert SQL from PostgreSQL to Snowflake dialect using sqlglot.
    
    This function applies a series of pre-processing transformations, attempts conversion
    with sqlglot, and then applies post-processing fixes for patterns that sqlglot
    doesn't handle correctly.
    
    Args:
        sql: PostgreSQL SQL statement to convert
        function_macros: Optional list of function names to preserve as macros
        
    Returns:
        Converted Snowflake SQL statement
    """
    logging.debug(f"[convert_postgres_to_snowflake] Input SQL:\n{sql}")

    # Snapshot the original SQL before any pre-processing so post-processing steps
    # can check what was actually written by the user (not added by our pipeline).
    original_sql = sql

    # ============================================================================
    # PRE-PROCESSING: Transform PostgreSQL-specific syntax before sqlglot parsing
    # ============================================================================
    
    # Convert PostgreSQL escape strings E'...' to regular strings with proper escaping
    if "E'" in sql or 'E"' in sql:
        logging.info("Converting PostgreSQL escape strings (E'...')")
        sql = convert_postgres_escape_strings(sql)

    # Pre-process: Convert PostgreSQL REGEXP_REPLACE(s, p, r [, flags]) to Snowflake form.
    # PostgreSQL defaults to replacing only the first occurrence; Snowflake defaults to all.
    # Must run after escape-string normalisation so flag string literals are clean.
    if 'regexp_replace(' in sql.lower():
        logging.info("Converting PostgreSQL REGEXP_REPLACE flags to Snowflake position/occurrence/parameters")
        sql = convert_postgres_regexp_replace(sql)

    # Pre-process: Remove redundant casts on numeric literals and NULL values.
    # Keep expression casts intact because they can affect runtime semantics.
    if '::' in sql or 'cast(' in sql.lower():
        logging.info("Removing redundant numeric-literal and NULL casts")
        sql = remove_redundant_literal_and_null_casts(sql)
    
    # Pre-process: Replace citext with varchar (case-insensitive text type)
    if 'citext' in sql.lower():
        logging.info("Converting citext to VARCHAR")
        sql = re.sub(r'\bcitext\b', 'VARCHAR', sql, flags=re.IGNORECASE)

    # Pre-process: Replace ::timestamp with ::date
    if '::timestamp' in sql.lower():
        logging.info("Converting ::timestamp to ::date")
        sql = re.sub(r'::timestamp\b', '::date', sql, flags=re.IGNORECASE)
    
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

    # Pre-process: Rewrite STRING_AGG(... ) FILTER (WHERE ...) to a conditional
    # first argument so sqlglot preserves FILTER semantics when ORDER BY is present.
    if 'string_agg(' in sql.lower() and 'filter' in sql.lower():
        logging.info("Rewriting STRING_AGG FILTER clauses to conditional arguments")
        sql = rewrite_string_agg_filter_to_conditional(sql)
    
    # Pre-process: Remove MATERIALIZED keyword from CTEs (not supported in Snowflake)
    if 'materialized' in sql.lower():
        logging.info("Removing MATERIALIZED keyword from CTEs")
        # Remove AS MATERIALIZED from CTEs: WITH cte AS MATERIALIZED (...) -> WITH cte AS (...)
        sql = re.sub(r'\bAS\s+MATERIALIZED\b', 'AS', sql, flags=re.IGNORECASE)
    
    # Pre-process: Convert PostgreSQL regex match operator ~ to RLIKE with pattern translation
    if '~' in sql:
        logging.info("Converting PostgreSQL regex match operator ~ to RLIKE")
        sql = convert_postgres_regex_to_rlike(sql)

    # Pre-process: Replace ILIKE 'ja'/'nee' with direct equality checks.
    if 'ilike' in sql.lower() and ("'ja'" in sql.lower() or "'nee'" in sql.lower()):
        logging.info("Converting ILIKE 'ja'/'nee' to '=' comparisons")
        sql = convert_ja_nee_ilike_to_equals(sql)
    
    # Pre-process: In CASE expressions that return 'NaN', change ::numeric to ::float
    # so Snowflake accepts the NaN value (only allowed in FLOAT columns, not NUMBER).
    if "'nan'" in sql.lower() or "'NaN'" in sql:
        logging.info("Replacing ::numeric with ::float inside CASE expressions that return 'NaN'")
        sql = replace_numeric_cast_with_float_in_nan_cases(sql)

    # Pre-process: Add ::varchar to array subscript accesses without an explicit cast
     #so that identifier[N] becomes identifier[N]::varchar and is converted to
     #CAST(identifier[N-1] AS VARCHAR) in Snowflake instead of staying as VARIANT.
    if '[' in sql:
        logging.info("Adding ::varchar cast to untyped array subscript accesses")
        sql = add_varchar_cast_to_untyped_array_access(sql)

    # Pre-process: Convert ARRAY[...] to ARRAY_CONSTRUCT(...)
    if 'array[' in sql.lower():
        logging.info("Converting ARRAY[...] to ARRAY_CONSTRUCT(...)")
        sql = convert_array_to_array_construct(sql)
    
    # Pre-process: Replace ::bigint with ::int (Snowflake uses INT for big integers)
    if '::bigint' in sql.lower():
        logging.info("Converting ::bigint to ::int")
        sql = re.sub(r'::bigint\b', '::int', sql, flags=re.IGNORECASE)
    
    # Pre-process: Strip ::type[] casts inside ILIKE/LIKE ANY/ALL(...) before the general
    # ::type[] → ::ARRAY conversion runs.  The cast is redundant there (it just annotates
    # the subquery result as an array) and would otherwise be turned into CAST(... AS ARRAY)
    # by sqlglot, which breaks the subquery-pattern regex in convert_like_any_to_regexp_like.
    # e.g.  col ILIKE ANY((SELECT x FROM t)::text[])  →  col ILIKE ANY((SELECT x FROM t))
    # e.g.  col ILIKE ALL((SELECT x FROM t)::text[])  →  col ILIKE ALL((SELECT x FROM t))
    if '::' in sql and '[]' in sql and re.search(r'I?LIKE\s+(?:ANY|ALL)\s*\(', sql, re.IGNORECASE):
        logging.info("Stripping ::type[] casts inside ILIKE/LIKE ANY/ALL(...)")
        sql = re.sub(
            r'(I?LIKE\s+(?:ANY|ALL)\s*\()(.*?)::\w+(?:\(\d+\))?\[\](\s*\))',
            r'\1\2\3',
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )

    # Pre-process: Convert PostgreSQL array type casts (::text[], ::varchar[], etc.) to ::ARRAY
    # These indicate that the accessed element is itself an array (e.g. col[4]::varchar[]).
    # In Snowflake, array elements are VARIANT; casting to ARRAY preserves array semantics.
    if '::' in sql and '[]' in sql:
        logging.info("Converting PostgreSQL array type casts to ::ARRAY")
        sql = re.sub(r'::(text|varchar|character varying|integer|int|bigint|smallint|numeric|float|double precision|boolean|date|timestamp)\[\]', '::ARRAY', sql, flags=re.IGNORECASE)
    
    # Pre-process: Handle crosstab function (not supported in Snowflake)
    is_crosstab = bool(re.search(r'\bcrosstab\s*\(', sql, re.IGNORECASE))
    if is_crosstab:
        sql = handle_crosstab(sql)

    # Pre-process: Convert unnest(ARRAY[...]) to SELECT ... FROM VALUES (...)
    sql = convert_unnest_array_to_values(sql) # 19 files affected

    # Pre-process: Convert CROSS JOIN LATERAL unnest(...) to Snowflake FLATTEN 
    sql = convert_lateral_unnest_to_snowflake(sql) # 2 files affected

    # Pre-process: Inject ORDER BY into every ARRAY_AGG that lacks one so that
    # Snowflake produces deterministic results (ARRAY_AGG has no guaranteed order).
    # Skip for crosstab queries — each CTE body was already processed individually
    # by convert_postgres_to_snowflake inside parse_crosstab_sql, which already
    # injected ORDER BY. Running it again on the assembled output would double them.
    if 'array_agg(' in sql.lower() and not is_crosstab:
        logging.info("Injecting ORDER BY into ARRAY_AGG calls without an existing ORDER BY")
        sql = add_order_by_to_array_agg(sql)

    # ============================================================================
    # SQLGLOT CONVERSION: Parse as PostgreSQL, generate as Snowflake
    # ============================================================================
    
    converted = sql
    sqlglot_succeeded = False
    try:
        # Parse with PostgreSQL dialect
        parsed = sqlglot.parse_one(sql, read="postgres")
        # Generate with custom Snowflake dialect
        converted = parsed.sql(dialect=FixedSnowflake, pretty=True)
        sqlglot_succeeded = True
    except Exception as e:
        logging.info(f"[Error] Failed to convert SQL with sqlglot: {e}\n")
        logging.info("Continuing with pre-processed SQL and applying post-processing steps")
        # When sqlglot fails, manually convert array indices (PostgreSQL 1-based to Snowflake 0-based)
        logging.info("Applying array index offset (-1) for Snowflake 0-based indexing")
        converted = convert_array_indices_postgres_to_snowflake(converted)
    
    # ============================================================================
    # POST-PROCESSING: Fix patterns that sqlglot doesn't handle correctly
    # ============================================================================
    
    # Post-process: Convert date arithmetic from SQLglot output to DATEADD
    # This handles patterns like: CAST(expr AS DATE) +/- CAST('N unit' AS INTERVAL)
    # Also converts month-based columns (like validtime) to DATEADD(month, ...)
    logging.info("Post-processing date arithmetic to DATEADD")
    month_columns = ['validtime']  # Configure which columns represent months
    converted = postprocess_date_arithmetic_snowflake(converted, month_columns=month_columns)
    
    # Post-process: Convert remaining ::type casts to CAST(expression AS type) (handles cases when sqlglot fails)
    if '::' in converted:
        logging.info("Converting remaining ::type casts to CAST(expression AS type)")
        converted = convert_postgres_cast_to_standard_cast(converted)

    # Post-process: Fix incorrect FLATTEN column qualifiers
    if 'TABLE(FLATTEN(INPUT =>' in converted.upper():
        logging.info("Fixing incorrect table alias prefixes in FLATTEN subqueries")
        converted = fix_flatten_column_qualifiers(converted)
    
    # Convert DISTINCT ON to Snowflake-compatible syntax
    if 'distinct on' in sql.lower():
        logging.info("Converting DISTINCT ON to Snowflake-compatible syntax")
        converted = convert_distinct_on_to_snowflake(converted)
    
    # Post-process: Convert CAST(... AS DATE|DECIMAL|NUMBER) to TRY_TO_DATE/TRY_TO_DECFLOAT
    if 'CAST(' in converted.upper():
        logging.info("Converting CAST to TRY_TO_DATE / TRY_CAST(... AS DECFLOAT) for DATE, DECIMAL, NUMBER, NUMERIC types")
        converted = convert_cast_to_try_cast(converted)
    
    # Post-process: Rewrite ARRAY_REMOVE to a PostgreSQL-compatible Snowflake UDF
    if 'array_remove(' in converted.lower():
        logging.info("Rewriting ARRAY_REMOVE to {{ function('pg_array_remove') }}(...) and wrapping value parameters in TO_VARIANT")
        converted = convert_array_remove_to_variant(converted)
    
    # Post-process: Convert any remaining && to ARRAYS_OVERLAP
    if '&&' in converted:
        logging.info("Converting remaining && to ARRAYS_OVERLAP")
        converted = convert_array_overlap_to_snowflake(converted)
    
    # Post-process: Convert char_length to LENGTH
    if 'char_length(' in converted.lower():
        logging.info("Converting CHAR_LENGTH to LENGTH")
        converted = re.sub(
            r'\bchar_length\s*\(',
            r'LENGTH(',
            converted,
            flags=re.IGNORECASE
        )

    # Post-process: Convert GREATEST/LEAST to GREATEST_IGNORE_NULLS/LEAST_IGNORE_NULLS
    converted = re.sub(r'\bGREATEST\s*\(', 'GREATEST_IGNORE_NULLS(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bLEAST\s*\(', 'LEAST_IGNORE_NULLS(', converted, flags=re.IGNORECASE)


    # Post-process: Convert EXTRACT(ISODOW FROM ...) and DATE_PART(ISODOW, ...) to DAYOFWEEKISO(...)
    if 'ISODOW' in converted.upper():
        logging.info("Converting EXTRACT(ISODOW FROM ...) and DATE_PART(ISODOW, ...) to DAYOFWEEKISO(...)")
        converted = re.sub(
            r'EXTRACT\s*\(\s*ISODOW\s+FROM\s+([^)]+)\)',
            r'DAYOFWEEKISO(\1)',
            converted,
            flags=re.IGNORECASE
        )
        converted = re.sub(
            r'DATE_PART\s*\(\s*ISODOW\s*,\s*([^)]+)\)',
            r'DAYOFWEEKISO(\1)',
            converted,
            flags=re.IGNORECASE
        )
        
    # Post-process: Fix DATE_PART(day, date_expr - date_expr) to just date_expr - date_expr
    # Since date subtraction in Snowflake returns an integer number of days
    # Use a custom function to handle nested parentheses properly
    converted = remove_date_part_day(converted)
    
    # Post-process: Convert ARRAY_GENERATE_RANGE in SELECT to row-generating FLATTEN
    if 'ARRAY_GENERATE_RANGE' in converted and 'SELECT' in converted.upper():
        logging.info("Converting ARRAY_GENERATE_RANGE to TABLE(FLATTEN(...))")  
        converted = convert_array_generate_range_to_flatten(converted)
    
    # Post-process: Wrap ARRAY_TO_STRING array arguments with ARRAY_COMPACT to mimic
    # PostgreSQL behaviour (array_to_string silently ignores NULL elements).
    # Only applies when array_to_string was already present in the ORIGINAL SQL —
    # not when it was synthetically inserted (e.g. by crosstab conversion).
    if wrap_array_to_string and 'array_to_string(' in original_sql.lower():
        logging.info("Wrapping ARRAY_TO_STRING array arguments with ARRAY_COMPACT")
        converted = wrap_array_to_string_with_compact(converted)

    # Post-process: Simplify overly complex UNNEST/EXPLODE patterns generated by sqlglot
    if '_u.pos = _u_2.pos_2' in converted and 'ARRAY_GENERATE_RANGE' in converted:
        logging.info("Simplifying overly complex UNNEST/EXPLODE patterns")
        converted = simplify_unnest_flatten(converted)
    
    # Post-process: Fix sqlglot misplacement of NOT in "col op NOT expr IS NULL".
    # sqlglot converts "(a >= b) IS NOT NULL" to "a >= NOT b IS NULL" (missing parens).
    # Correct form is "NOT (a >= b) IS NULL".
    converted = re.sub(
        r'(\w+)\s*(>=|<=|>|<|=|<>|!=)\s*NOT\s+(\w+)\s+IS\s+NULL',
        r'(\1 \2 \3) IS NOT NULL',
        converted,
        flags=re.IGNORECASE
    )

    # Post-process: Fix ARRAY_AGG(IFF(NOT x IS NULL, DISTINCT x, NULL)) to ARRAY_AGG(DISTINCT x)
    converted = re.sub(
        r"ARRAY_AGG\(\s*IFF\(\s*NOT\s+([a-zA-Z0-9_]+)\s+IS\s+NULL\s*,\s*DISTINCT\s+\1\s*,\s*NULL\s*\)\s*\)",
        r"ARRAY_AGG(DISTINCT \1)",
        converted,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Post-process: Ordered variant
    # ARRAY_AGG(DISTINCT IFF(NOT x IS NULL, x, NULL)) WITHIN GROUP (ORDER BY ...)
    # -> ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY ...)
    converted = re.sub(
        r"ARRAY_AGG\(\s*DISTINCT\s+IFF\(\s*NOT\s+([a-zA-Z0-9_\.]+)\s+IS\s+NULL\s*,\s*\1\s*,\s*NULL\s*\)\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+?)\s*\)",
        r"ARRAY_AGG(DISTINCT \1) WITHIN GROUP (ORDER BY \2)",
        converted,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # Post-process: Fix aggregate functions with IFF(condition, DISTINCT expression, NULL)
    converted = fix_aggregate_distinct_iff(converted)
    
    # Post-process: Convert AGE() to DATEDIFF(year, ...)
    if 'AGE(' in converted.upper():
        logging.info("Converting AGE() to DATEDIFF(year, ...)")
        
        # First, handle EXTRACT(YEAR FROM AGE(...)) and DATE_PART(YEAR, AGE(...)) -> DATEDIFF(year, ...)
        # This removes the redundant wrapper since DATEDIFF already returns years
        converted = re.sub(
            r'(?:EXTRACT\s*\(\s*YEAR\s+FROM|DATE_PART\s*\(\s*YEAR\s*,)\s*AGE\s*\(\s*([^,()]+(?:\([^)]*\))?[^,()]*)\s*,\s*([^,()]+(?:\([^)]*\))?[^,()]*)\s*\)\s*\)',
            r'DATEDIFF(year, \2, \1)',
            converted,
            flags=re.IGNORECASE
        )
        converted = re.sub(
            r'(?:EXTRACT\s*\(\s*YEAR\s+FROM|DATE_PART\s*\(\s*YEAR\s*,)\s*AGE\s*\(\s*([^,()]+(?:\([^)]*\))?[^,()]*)\s*\)\s*\)',
            r'DATEDIFF(year, \1, CURRENT_TIMESTAMP())',
            converted,
            flags=re.IGNORECASE
        )
        
        # Then handle remaining plain AGE() calls
        # AGE(end, start) -> DATEDIFF(year, start, end) - swap arguments
        converted = re.sub(
            r'\bAGE\s*\(\s*([^,()]+(?:\([^)]*\))?[^,()]*)\s*,\s*([^,()]+(?:\([^)]*\))?[^,()]*)\s*\)',
            r'DATEDIFF(year, \2, \1)',
            converted,
            flags=re.IGNORECASE
        )
        # AGE(date) -> DATEDIFF(year, date, CURRENT_TIMESTAMP())
        converted = re.sub(
            r'\bAGE\s*\(\s*([^,()]+(?:\([^)]*\))?[^,()]*)\s*\)',
            r'DATEDIFF(year, \1, CURRENT_TIMESTAMP())',
            converted,
            flags=re.IGNORECASE
        )
    
    # Post-process: Convert MAX(CASE WHEN ... THEN ARRAY_CONSTRUCT(...) ...) to ARRAY_AGG
    converted = convert_max_array_to_array_agg(converted)
    
    # Post-process: Convert remaining ANY expressions (CRITICAL - runs regardless of sqlglot success)
    if 'ANY(' in converted.upper():
        logging.info("Converting remaining ANY expressions to Snowflake equivalents")
        converted = convert_any_to_snowflake_post(converted)
    
    # Post-process: Convert ALL expressions to Snowflake equivalents
    if 'ALL(' in converted.upper():
        logging.info("Converting ALL expressions to Snowflake equivalents")
        converted = convert_all_to_snowflake_post(converted)

    # Post-process: Convert SIMILAR TO to RLIKE (handles cases when sqlglot fails)
    if 'SIMILAR TO' in converted.upper():
        logging.info("Converting SIMILAR TO to RLIKE (post-processing)")
        converted = convert_similar_to_to_rlike(converted)

    # Post-process: Convert LIKE/ILIKE ANY(ARRAY_CONSTRUCT(...)) to LIKE/ILIKE ANY(...)
    # Snowflake supports ILIKE ANY with comma-separated values directly
    # Just need to remove the ARRAY_CONSTRUCT wrapper
    def like_any_array_construct_repl(match):
        like_op = match.group(1).upper()  # LIKE or ILIKE
        args = match.group(2)
        # Remove whitespace and split by comma, but keep quoted strings intact
        # This regex splits on commas not inside quotes
        import re
        parts = re.findall(r"'[^']*'|\"[^\"]*\"|[^,]+", args)
        # Clean up whitespace
        parts = [p.strip() for p in parts if p.strip()]
        return f"{like_op} ANY({', '.join(parts)})"

    converted = re.sub(
        r"(I?LIKE)\s+ANY\s*\(\s*ARRAY_CONSTRUCT\((.*?)\)\s*\)",
        like_any_array_construct_repl,
        converted,
        flags=re.IGNORECASE | re.DOTALL
    )


    # Post-process: Convert LIKE/ILIKE ANY(column1 || column2 || ...) to
    # {{ function('ILIKE_ANY') }}(col, CONCAT_WS('|', ARRAY_TO_STRING(col1, '|'), ARRAY_TO_STRING(col2, '|'), ...))
    # ARRAY_CAT only accepts two args in Snowflake, so CONCAT_WS is used for any number of arrays.
    concat_any_pattern = re.compile(
        r"(I?LIKE)\s+ANY\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\s*\|\|\s*[a-zA-Z_][a-zA-Z0-9_]*)+)\s*\)",
        re.IGNORECASE,
    )
    for _ in range(200):
        match = concat_any_pattern.search(converted)
        if not match:
            break
        like_op = match.group(1).upper()
        columns = [col.strip() for col in re.split(r'\s*\|\|\s*', match.group(2))]
        left_result = _find_left_expr_before_operator(converted, match.start())
        if not left_result:
            break
        left_expr, left_start = left_result
        udf_name = "ILIKE_ANY" if like_op == 'ILIKE' else "LIKE_ANY"
        if len(columns) == 1:
            pattern_arg = f"ARRAY_TO_STRING({columns[0]}, '|')"
        else:
            parts = ", ".join(f"ARRAY_TO_STRING({col}, '|')" for col in columns)
            pattern_arg = f"CONCAT_WS('|', {parts})"
        replacement = f"{{{{ function('{udf_name}') }}}}({left_expr}, {pattern_arg})"
        converted = converted[:left_start] + replacement + converted[match.end():]

    # Post-process: Convert LIKE/ILIKE ANY(dynamic_expr) to REGEXP_LIKE().
    # Covers column references, subqueries, and JOIN ... ON conditions.
    # ILIKE becomes REGEXP_LIKE(..., 'i'); % wildcard is mapped to .* in the regex pattern.
    # LIKE/ILIKE ANY(ARRAY_CONSTRUCT(...)) with literal strings is handled above and skipped here.
    if re.search(r'I?LIKE\s+ANY\s*\(', converted, re.IGNORECASE):
        logging.info("Converting LIKE/ILIKE ANY(dynamic) to REGEXP_LIKE")
        converted = convert_like_any_to_regexp_like(converted)

    # Post-process: Strip CAST(... AS ARRAY) wrapper inside ILIKE/LIKE ALL(...) that sqlglot
    # may introduce when it sees a remaining ::ARRAY cast on the subquery.
    # e.g.  col ILIKE ALL (CAST((SELECT x FROM t) AS ARRAY))  →  col ILIKE ALL ((SELECT x FROM t))
    if re.search(r'I?LIKE\s+ALL\s*\(', converted, re.IGNORECASE):
        converted = re.sub(
            r'(I?LIKE\s+ALL\s*\(\s*)CAST\s*\(\s*(.*?)\s*AS\s+ARRAY\s*\)(\s*\))',
            r'\1\2\3',
            converted,
            flags=re.IGNORECASE | re.DOTALL,
        )

    # Post-process: Convert LIKE/ILIKE ALL(dynamic_expr) to ILIKE_ALL/LIKE_ALL UDF calls.
    if re.search(r'I?LIKE\s+ALL\s*\(', converted, re.IGNORECASE):
        logging.info("Converting LIKE/ILIKE ALL(dynamic) to ILIKE_ALL/LIKE_ALL UDF calls")
        converted = convert_like_all_to_regexp_like(converted)

    # Post-process: Convert ARRAY_AGG([DISTINCT] IFF(..., expr ORDER BY ..., NULL)) to ARRAY_AGG([DISTINCT] IFF(..., expr, NULL)) WITHIN GROUP(ORDER BY ...)
    def array_agg_iff_orderby_repl(match):
        distinct = match.group(1).strip() + ' ' if match.group(1) else ''
        condition = match.group(2).strip()
        expr = match.group(3).strip()
        orderby = match.group(4).strip()
        return f"ARRAY_AGG({distinct}IFF({condition}, {expr}, NULL)) WITHIN GROUP (ORDER BY {orderby})"

    converted = re.sub(
        r"ARRAY_AGG\(\s*(DISTINCT\s+)?IFF\(\s*(.*?),\s*(.*?)\s+ORDER\s+BY\s+(.*?),\s*NULL\s*\)\s*\)",
        array_agg_iff_orderby_repl,
        converted,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Post-process: Fix malformed sqlglot shape where WITHIN GROUP is placed inside ARRAY_AGG(...)
    # ARRAY_AGG(DISTINCT IFF(NOT x IS NULL, x) WITHIN GROUP (ORDER BY ...))
    # -> ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY ...)
    converted = re.sub(
        r"ARRAY_AGG\(\s*DISTINCT\s+IFF\(\s*NOT\s+([a-zA-Z0-9_\.]+)\s+IS\s+NULL\s*,\s*\1\s*(?:,\s*NULL\s*)?\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+?)\s*\)\s*\)",
        r"ARRAY_AGG(DISTINCT \1) WITHIN GROUP (ORDER BY \2)",
        converted,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # Post-process: Remove trailing NULL sort key introduced in WITHIN GROUP ORDER BY
    converted = re.sub(
        r"WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+?),\s*NULL\s*\)",
        r"WITHIN GROUP (ORDER BY \1)",
        converted,
        flags=re.IGNORECASE,
    )

    # Post-process: Final ordered simplification after all ARRAY_AGG+IFF rewrites
    # ARRAY_AGG(DISTINCT IFF(NOT x IS NULL, x, NULL)) WITHIN GROUP (ORDER BY ...)
    # -> ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY ...)
    converted = re.sub(
        r"ARRAY_AGG\(\s*DISTINCT\s+IFF\(\s*NOT\s+([a-zA-Z0-9_\.]+)\s+IS\s+NULL\s*,\s*\1\s*,\s*NULL\s*\)\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+?)\s*\)",
        r"ARRAY_AGG(DISTINCT \1) WITHIN GROUP (ORDER BY \2)",
        converted,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # Post-process: Fix COUNT(IFF(..., *, NULL)) to COUNT(IFF(..., 1, NULL)) and pretty-format
    def fix_and_format_count_iff(match):
        condition = match.group(1).strip()
        # Replace * with 1 if present, otherwise keep as is
        then_value = match.group(2).strip()
        if then_value == '*':
            then_value = '1'
        return (
            "COUNT(\n    IFF(\n        "
            + condition
            + ",\n        " + then_value + ", NULL"
            + "\n    )\n)"
        )
    
    converted = re.sub(
        r'COUNT\s*\(\s*IFF\s*\((.*?),\s*(\*|1),\s*NULL\s*\)\s*\)',
        fix_and_format_count_iff,
        converted,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Post-process: Wrap ARRAY_AGG(...) with NULLIF(..., []) to match PostgreSQL NULL semantics.
    # In PostgreSQL, ARRAY_AGG returns NULL for empty sets; in Snowflake it returns [].
    if 'ARRAY_AGG(' in converted.upper():
        logging.info("Wrapping ARRAY_AGG with NULLIF to match PostgreSQL NULL semantics")
        converted = wrap_array_agg_with_nullif(converted)

    # Post-process: Wrap LISTAGG(...) with NULLIF(..., '') to match PostgreSQL STRING_AGG behavior.
    # In PostgreSQL, STRING_AGG returns NULL when all elements are NULL or empty; in Snowflake, LISTAGG returns ''.
    if 'LISTAGG(' in converted.upper():
        logging.info("Wrapping LISTAGG with NULLIF to match PostgreSQL STRING_AGG NULL semantics")
        converted = wrap_listagg_with_nullif(converted)

    # Post-process: Convert FILTER-clause-derived ARRAY_AGG patterns.
    # ARRAY_AGG(DISTINCT IFF(...))              → ARRAY_UNIQUE_AGG(IFF(...))
    # ARRAY_AGG(IFF(...)) WITHIN GROUP (ORDER BY ...) → ARRAY_SORT(ARRAY_AGG(IFF(...)))
    if 'ARRAY_AGG(' in converted.upper():
        logging.info("Converting FILTER-derived ARRAY_AGG patterns (ARRAY_UNIQUE_AGG / ARRAY_SORT)")
        converted = convert_filter_derived_array_agg(converted)

    # Post-process: Wrap ROUND(expr, n) with CAST(... AS NUMBER(36, n)) to enforce scale in Snowflake
    if 'ROUND(' in converted.upper():
        logging.info("Wrapping ROUND(expr, n) as CAST(ROUND(expr, n) AS NUMBER(36, n))")
        converted = wrap_round_with_number_scale(converted)

    # Post-process: Normalize date string literals with '/' separators inside TO_DATE / TRY_TO_DATE.
    # PostgreSQL accepts '2026/01/01' even with a 'yyyy-mm-dd' format; Snowflake does not.
    # Replace slashes with dashes only inside quoted string literals that are the first argument
    # of TO_DATE or TRY_TO_DATE and match a date-like pattern (digits separated by slashes).
    if re.search(r'\b(?:TO_DATE|TRY_TO_DATE|DATE)\s*\(', converted, re.IGNORECASE):
        logging.info("Normalizing '/' to '-' in date literals inside TO_DATE / TRY_TO_DATE / DATE")

        def _normalize_date_slashes(m: re.Match) -> str:
            fn = m.group(1)       # TO_DATE, TRY_TO_DATE, or DATE
            literal = m.group(2)  # quoted date string, e.g. '2026/01/01'
            rest = m.group(3)     # everything after the first argument
            normalized = literal.replace('/', '-')
            return f"{fn}({normalized}{rest}"

        converted = re.sub(
            r"\b(TO_DATE|TRY_TO_DATE|DATE)\s*\(\s*('[^']*\d{1,4}/\d{1,2}/\d{1,4}[^']*')\s*((?:,|\))[^)]*\)?)",
            _normalize_date_slashes,
            converted,
            flags=re.IGNORECASE,
        )

        # Wrap column reference arguments with REPLACE(..., '/', '-') so that runtime values
        # containing slashes (e.g. '2026/01/01') are accepted by Snowflake's TO_DATE.
        # Only applies when a format string is present (two-argument form) and the first
        # argument is a column reference (identifier, optionally table-qualified), not a literal.
        logging.info("Wrapping column arguments of TO_DATE / TRY_TO_DATE with REPLACE(..., '/', '-')")

        def _wrap_col_with_replace(m: re.Match) -> str:
            fn = m.group(1)     # TO_DATE or TRY_TO_DATE
            col = m.group(2)    # column reference, e.g. uitslag or mw.uitslag
            fmt = m.group(3)    # format string, e.g. 'MM-DD-YYYY'
            # Skip if already wrapped
            if col.upper().startswith('REPLACE('):
                return m.group(0)
            # Only wrap when the format contains a separator — formats like 'DDMMYYYY'
            # are purely numeric so a slash can never appear in valid input.
            fmt_inner = fmt.strip("'\"")
            if not re.search(r'[-/. ]', fmt_inner):
                return m.group(0)
            return f"{fn}(REPLACE({col}, '/', '-'), {fmt})"

        converted = re.sub(
            r"\b(TO_DATE|TRY_TO_DATE|DATE)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*,\s*('[^']+')\s*\)",
            _wrap_col_with_replace,
            converted,
            flags=re.IGNORECASE,
        )

    # Post-process: Replace uitslag_num with {{ function('get_number') }}(uitslag) in
    # queries that select from bepaling. Done after sqlglot so Jinja {{ }} doesn't
    # confuse the parser.
    if 'uitslag_num' in converted.lower() and 'bepaling' in converted.lower():
        logging.info("Replacing uitslag_num with {{ function('get_number') }}(uitslag) for bepaling queries")
        converted = replace_uitslag_num_for_bepaling(converted)

    return converted

def wrap_array_agg_with_nullif(sql: str) -> str:
    """
    Wrap ARRAY_AGG(...) with NULLIF(..., []) to match PostgreSQL NULL semantics.
    In PostgreSQL, ARRAY_AGG returns NULL for empty input rows; in Snowflake it returns [].
    NULLIF(ARRAY_AGG(...), []) restores that NULL-on-empty behavior.

    Also handles ARRAY_AGG(...) WITHIN GROUP (ORDER BY ...) as a single unit.
    Skips any ARRAY_AGG that is already the direct argument of NULLIF.
    """
    def find_top_level_order_by(expression: str) -> int:
        depth = 0
        in_str = False
        str_char = ''

        for idx, ch in enumerate(expression):
            if in_str:
                if ch == str_char and (idx == 0 or expression[idx - 1] != '\\'):
                    in_str = False
                continue

            if ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and expression[idx:idx + 8].upper() == 'ORDER BY':
                prev_char = expression[idx - 1] if idx > 0 else ' '
                next_char = expression[idx + 8] if idx + 8 < len(expression) else ' '
                if not (prev_char.isalnum() or prev_char == '_') and not (next_char.isalnum() or next_char == '_'):
                    return idx

        return -1

    result = []
    i = 0
    n = len(sql)

    while i < n:
        m = re.match(r'ARRAY_AGG\s*\(', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        # Skip if already wrapped in NULLIF(
        prefix = ''.join(result).rstrip()
        if prefix.upper().endswith('NULLIF('):
            result.append(sql[i])
            i += 1
            continue

        # Find the closing ) of ARRAY_AGG using balanced paren matching
        paren_start = i + m.end() - 1  # position of opening (
        depth = 1
        j = paren_start + 1
        in_str = False
        str_char = ''
        while j < n and depth > 0:
            ch = sql[j]
            if in_str:
                if ch == str_char and (j == 0 or sql[j - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            j += 1

        if depth != 0:
            # Unbalanced - leave as-is
            result.append(sql[i])
            i += 1
            continue

        # j is now one past the closing )
        agg_expr = sql[i:j]
        inner_expr = sql[paren_start + 1:j - 1]

        # Convert PostgreSQL inline ordering to Snowflake WITHIN GROUP using the
        # same balanced parsing path that already wraps ARRAY_AGG with NULLIF.
        order_by_pos = find_top_level_order_by(inner_expr)
        if order_by_pos != -1:
            value_expr = inner_expr[:order_by_pos].rstrip()
            order_by_expr = inner_expr[order_by_pos + 8:].strip()
            if value_expr and order_by_expr:
                agg_expr = f"ARRAY_AGG({value_expr}) WITHIN GROUP (ORDER BY {order_by_expr})"
                j = i + len(sql[i:j])

        # Also consume WITHIN GROUP (ORDER BY ...) if present
        within_m = re.match(r'\s+WITHIN\s+GROUP\s*\(', sql[j:], re.IGNORECASE)
        if within_m:
            k = j + within_m.end() - 1  # position of the opening ( of WITHIN GROUP
            depth2 = 1
            k2 = k + 1
            in_str2 = False
            str_char2 = ''
            while k2 < n and depth2 > 0:
                ch = sql[k2]
                if in_str2:
                    if ch == str_char2 and (k2 == 0 or sql[k2 - 1] != '\\'):
                        in_str2 = False
                elif ch in ("'", '"'):
                    in_str2 = True
                    str_char2 = ch
                elif ch == '(':
                    depth2 += 1
                elif ch == ')':
                    depth2 -= 1
                k2 += 1
            agg_expr = sql[i:k2]
            j = k2

        result.append(f"NULLIF({agg_expr}, [])")
        i = j

    return ''.join(result)


def convert_filter_derived_array_agg(sql: str) -> str:
    """
    Convert FILTER-clause-derived ARRAY_AGG patterns to proper Snowflake equivalents.

    Runs AFTER wrap_array_agg_with_nullif so the input already contains NULLIF wrappers.
    Detects the presence of IFF(...) directly inside ARRAY_AGG (with or without DISTINCT),
    which is the signature that sqlglot produced this from a FILTER (WHERE ...) clause.

      NULLIF(ARRAY_AGG(DISTINCT IFF(...)), [])
          → NULLIF(ARRAY_UNIQUE_AGG(IFF(...)), [])

      NULLIF(ARRAY_AGG(IFF(...)) WITHIN GROUP (ORDER BY col DESC), [])
          → NULLIF(ARRAY_AGG(IFF(...)) WITHIN GROUP (ORDER BY col DESC), [])

      NULLIF(ARRAY_AGG(DISTINCT IFF(...)) WITHIN GROUP (ORDER BY ...), [])
          → NULLIF(ARRAY_SORT(ARRAY_UNIQUE_AGG(IFF(...))), [])

    Cases without DISTINCT and without ORDER BY are left unchanged.
    """
    result = []
    i = 0
    n = len(sql)

    while i < n:
        # Find next ARRAY_AGG(
        m = re.match(r'ARRAY_AGG\s*\(', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        agg_start = i
        inner_start = i + m.end()  # index right after the opening '(' of ARRAY_AGG

        # Check if inner content starts with DISTINCT
        distinct_m = re.match(r'\s*DISTINCT\s+', sql[inner_start:], re.IGNORECASE)
        has_distinct = distinct_m is not None
        after_distinct = inner_start + (distinct_m.end() if has_distinct else 0)

        # Check if the token after optional DISTINCT is IFF( (allow leading whitespace)
        iff_m = re.match(r'\s*IFF\s*\(', sql[after_distinct:], re.IGNORECASE)
        if not iff_m:
            # Not a FILTER-derived pattern — pass through char by char
            result.append(sql[i])
            i += 1
            continue

        # Scan for the balancing ')' of ARRAY_AGG using paren depth tracking
        depth = 1
        j = inner_start
        in_str = False
        str_char = ''
        while j < n and depth > 0:
            ch = sql[j]
            if in_str:
                if ch == str_char and (j == 0 or sql[j - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            j += 1

        if depth != 0:
            # Unbalanced — leave as-is
            result.append(sql[i])
            i += 1
            continue

        # Extract the IFF(...) call (strip leading/trailing whitespace from the inner slice)
        iff_call = sql[after_distinct:j - 1].strip()

        # Check for WITHIN GROUP (ORDER BY ...) immediately following ARRAY_AGG(...)
        within_m = re.match(r'\s*WITHIN\s+GROUP\s*\(', sql[j:], re.IGNORECASE)
        has_order_by = within_m is not None
        after_within = j  # default: no WITHIN GROUP consumed
        order_by_content = ''  # extracted ORDER BY expression (without the ORDER BY keyword)

        if has_order_by:
            # Find the closing ')' of WITHIN GROUP (...)
            wg_paren_start = j + within_m.end() - 1  # position of the '(' in WITHIN GROUP (
            depth2 = 1
            k = wg_paren_start + 1
            in_str2 = False
            str_char2 = ''
            while k < n and depth2 > 0:
                ch = sql[k]
                if in_str2:
                    if ch == str_char2:
                        in_str2 = False
                elif ch in ("'", '"'):
                    in_str2 = True
                    str_char2 = ch
                elif ch == '(':
                    depth2 += 1
                elif ch == ')':
                    depth2 -= 1
                k += 1
            after_within = k
            # Extract the ORDER BY clause content (strip the ORDER BY keyword itself)
            wg_inner = sql[wg_paren_start + 1:k - 1].strip()
            order_by_content = re.sub(r'^ORDER\s+BY\s+', '', wg_inner, flags=re.IGNORECASE).strip()

        # Build replacement
        if has_distinct and has_order_by:
            result.append(f"ARRAY_SORT(ARRAY_UNIQUE_AGG({iff_call}))")
        elif has_distinct:
            result.append(f"ARRAY_UNIQUE_AGG({iff_call})")
        elif has_order_by:
            result.append(f"ARRAY_AGG({iff_call}) WITHIN GROUP (ORDER BY {order_by_content})")
        else:
            # Plain FILTER without DISTINCT or ORDER BY — no change needed
            result.append(sql[agg_start:j])

        i = after_within

    return ''.join(result)


def wrap_listagg_with_nullif(sql: str) -> str:
    """
    Wrap LISTAGG(...) with NULLIF(..., '') to match PostgreSQL STRING_AGG behavior.
    
    In PostgreSQL, STRING_AGG returns NULL when all elements are NULL or empty.
    In Snowflake, LISTAGG returns '' (empty string) in that case.
    NULLIF(LISTAGG(...), '') restores that NULL-on-empty behavior.

    Also handles LISTAGG(...) WITHIN GROUP (ORDER BY ...) as a single unit.
    Skips any LISTAGG that is already the direct argument of NULLIF.
    """
    result = []
    i = 0
    n = len(sql)

    def find_top_level_comma(text: str) -> int:
        depth = 0
        in_str = False
        str_char = ''

        for idx, ch in enumerate(text):
            if in_str:
                if ch == str_char and (idx == 0 or text[idx - 1] != '\\'):
                    in_str = False
                continue

            if ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                return idx

        return -1

    while i < n:
        m = re.match(r'LISTAGG\s*\(', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        # Skip if already wrapped in NULLIF(
        prefix = ''.join(result).rstrip()
        if prefix.upper().endswith('NULLIF('):
            result.append(sql[i])
            i += 1
            continue

        # Find the closing ) of LISTAGG using balanced paren matching
        paren_start = i + m.end() - 1  # position of opening (
        depth = 1
        j = paren_start + 1
        in_str = False
        str_char = ''
        while j < n and depth > 0:
            ch = sql[j]
            if in_str:
                if ch == str_char and (j == 0 or sql[j - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            j += 1

        if depth != 0:
            # Unbalanced - leave as-is
            result.append(sql[i])
            i += 1
            continue

        # j is now one past the closing )
        agg_expr = sql[i:j]

        # Also consume WITHIN GROUP (ORDER BY ...) if present
        within_m = re.match(r'\s+WITHIN\s+GROUP\s*\(', sql[j:], re.IGNORECASE)
        if within_m:
            k = j + within_m.end() - 1  # position of the opening ( of WITHIN GROUP
            depth2 = 1
            k2 = k + 1
            in_str2 = False
            str_char2 = ''
            while k2 < n and depth2 > 0:
                ch = sql[k2]
                if in_str2:
                    if ch == str_char2 and (k2 == 0 or sql[k2 - 1] != '\\'):
                        in_str2 = False
                elif ch in ("'", '"'):
                    in_str2 = True
                    str_char2 = ch
                elif ch == '(':
                    depth2 += 1
                elif ch == ')':
                    depth2 -= 1
                k2 += 1
            listagg_inner = sql[i:j]
            listagg_open = i + m.end() - 1
            listagg_args = sql[listagg_open + 1:j - 1]
            comma_idx = find_top_level_comma(listagg_args)
            if comma_idx != -1:
                first_arg = listagg_args[:comma_idx].strip()
                distinct_m = re.match(r'DISTINCT\s+(.+)', first_arg, re.IGNORECASE | re.DOTALL)
                if distinct_m:
                    distinct_expr = distinct_m.group(1).strip()
                    agg_expr = f"{listagg_inner} WITHIN GROUP (ORDER BY {distinct_expr})"
                else:
                    agg_expr = sql[i:k2]
            else:
                agg_expr = sql[i:k2]
            j = k2

        result.append(f"NULLIF({agg_expr}, '')")
        i = j

    return ''.join(result)


def convert_similar_to_to_rlike(sql: str) -> str:
    """
    Convert PostgreSQL SIMILAR TO to Snowflake RLIKE with pattern translation.
    This is a post-processing step that ensures conversion even when sqlglot fails.
    
    Pattern translation:
    - % → .* (zero or more characters)
    - _ → . (single character)
    - Add ^ at the beginning if not present (SIMILAR TO is anchored by default)
    
    Example:
        column SIMILAR TO '(C|V|T|E)%' → column RLIKE '^(C|V|T|E).*'
        column SIMILAR TO '_abc%' → column RLIKE '^.abc.*'
    """
    def translate_similar_to_pattern(pattern: str) -> str:
        """Translate SIMILAR TO pattern to regex pattern for RLIKE"""
        # Replace SIMILAR TO wildcards with regex equivalents
        translated = pattern.replace('%', '.*').replace('_', '.')
        # Add ^ at the beginning if not already present
        if not translated.startswith('^'):
            translated = '^' + translated
        return translated
    
    def repl(match):
        expression = match.group(1)
        pattern = match.group(2)
        translated_pattern = translate_similar_to_pattern(pattern)
        return f"{expression} RLIKE '{translated_pattern}'"
    
    # Match: expression SIMILAR TO 'pattern'
    # Handles multi-line and different whitespace
    sql = re.sub(
        r'(\S+(?:\s+\S+)*?)\s+SIMILAR\s+TO\s+\'([^\']+)\'',
        repl,
        sql,
        flags=re.IGNORECASE
    )
    
    return sql


def convert_postgres_cast_to_standard_cast(sql: str) -> str:
    """
    Convert PostgreSQL-style ::type casts to standard CAST(expression AS type) syntax.
    This is a post-processing step that handles cases when sqlglot fails to convert these.
    
    Handles patterns like:
        'value'::date → CAST('value' AS DATE)
        column[3]::numeric → CAST(column[3] AS NUMERIC)
        (expression)::type → CAST((expression) AS type)
    
    SKIPS ::interval casts - these are handled exclusively by postprocess_date_arithmetic_snowflake
    
    The function walks backwards from :: to find the start of the expression, handling:
    - String literals: 'value'
    - Parenthesized expressions: (...)
    - Array access: column[index]
    - Function calls: func(...)
    - Column names: column_name
    """
    # Use regex to replace expr::type with CAST(expr AS type)
    # Handles nested parentheses, array access, string literals, identifiers
    def cast_repl(match):
        expr = match.group(1)
        typ = match.group(2)
        # Skip interval casts - let date arithmetic processing handle them
        if typ.upper() == 'INTERVAL':
            return match.group(0)  # Return original unchanged
        return f"CAST({expr} AS {typ.upper()})"

    # Pattern components:
    # - Quoted strings: '[^']*' or "[^"]*"
    # - Function calls: IDENTIFIER(...) - handled with balanced parentheses
    # - Array access: identifier[...]
    # - Simple identifiers: identifier or schema.table.column
    # Type pattern: alphanumeric/underscore only (no spaces), optionally followed by (precision)
    
    # First, match function calls with their arguments (including nested parens)
    # Pattern: function_name(args)::type
    # Include common SQL clause/condition keywords so casts in CASE/WHEN and predicates are converted.
    cast_follow_boundary = (
        r"(?=\s*(?:[,;:/\*\+\-<>=!]|\)|\]"
        r"|END\b|AS\b|AND\b|OR\b|THEN\b|WHEN\b|ELSE\b|IS\b|NOT\b"
        r"|LIKE\b|ILIKE\b|IN\b|BETWEEN\b|ON\b|USING\b|FROM\b|WHERE\b"
        r"|GROUP\b|ORDER\b|HAVING\b|LIMIT\b|OVER\b|UNION\b|INTERSECT\b|EXCEPT\b"
        r"|[a-zA-Z_][a-zA-Z0-9_]*\b"
        r"|$))"
    )
    pattern_func = re.compile(
        r"([a-zA-Z_][\w\.]*\s*\([^()]*(?:\([^()]*\)[^()]*)*\))\s*::\s*"
        r"([a-zA-Z_][a-zA-Z0-9_]*(?:\([^)]+\))?)"
        + cast_follow_boundary,
        re.IGNORECASE,
    )
    sql = pattern_func.sub(cast_repl, sql)
    
    # Then, match other patterns (quoted strings, simple parenthesis, array access, identifiers)
    pattern_other = re.compile(
        r"((?:'[^']*'|\"[^\"]*\"|\([^()]+\)|[a-zA-Z_][\w\.]*\[[^\]]+\]|[a-zA-Z_][\w\.]+))\s*::\s*"
        r"([a-zA-Z_][a-zA-Z0-9_]*(?:\([^)]+\))?)"
        + cast_follow_boundary,
        re.IGNORECASE,
    )
    sql = pattern_other.sub(cast_repl, sql)
    
    return sql


def find_expression_start(sql: str, end_pos: int) -> int:
    """
    Find the start of the expression before a :: cast operator.
    Walks backwards from end_pos to find where the expression begins.
    
    Handles:
    - String literals: 'value' or "value"
    - Parenthesized expressions: (...)
    - Array access: column[index]
    - Function calls: func(args)
    - Column/table names: schema.table.column or column
    """
    if end_pos <= 0:
        return 0
    
    pos = end_pos - 1
    
    # Skip trailing whitespace
    while pos >= 0 and sql[pos].isspace():
        pos -= 1
    
    if pos < 0:
        return 0
    
    # Check what type of expression we have
    last_char = sql[pos]
    
    # Case 1: Closing parenthesis - could be (expr) or func() or array[index]
    if last_char == ')':
        # Find matching opening parenthesis
        paren_count = 1
        pos -= 1
        while pos >= 0 and paren_count > 0:
            if sql[pos] == ')':
                paren_count += 1
            elif sql[pos] == '(':
                paren_count -= 1
            pos -= 1
        pos += 1  # Adjust back to the opening paren
        
        # Check if there's a function name or column name before the (
        start = pos
        while pos > 0 and (sql[pos-1].isalnum() or sql[pos-1] in '_$.'):
            pos -= 1
        
        return pos
    
    # Case 2: Closing bracket - array access column[index]
    elif last_char == ']':
        # Find matching opening bracket
        bracket_count = 1
        pos -= 1
        while pos >= 0 and bracket_count > 0:
            if sql[pos] == ']':
                bracket_count += 1
            elif sql[pos] == '[':
                bracket_count -= 1
            pos -= 1
        pos += 1  # Adjust back to the opening bracket
        
        # Find the column name before [
        while pos > 0 and (sql[pos-1].isalnum() or sql[pos-1] in '_.'):
            pos -= 1
        
        return pos
    
    # Case 3: String literal with single quotes
    elif last_char == "'":
        pos -= 1
        # Walk backwards to find opening quote, handling escaped quotes
        while pos >= 0:
            if sql[pos] == "'" and (pos == 0 or sql[pos-1] != '\\'):
                return pos
            pos -= 1
        return 0
    
    # Case 4: String literal with double quotes
    elif last_char == '"':
        pos -= 1
        # Walk backwards to find opening quote, handling escaped quotes
        while pos >= 0:
            if sql[pos] == '"' and (pos == 0 or sql[pos-1] != '\\'):
                return pos
            pos -= 1
        return 0
    
    # Case 5: Identifier (column name, possibly qualified)
    elif last_char.isalnum() or last_char == '_':
        # Walk backwards while we have valid identifier characters
        # Include . for qualified names (schema.table.column)
        while pos > 0 and (sql[pos-1].isalnum() or sql[pos-1] in '_.'):
            pos -= 1
        # Also handle $ in identifiers (like ${variable})
        if pos > 0 and sql[pos-1] == '$':
            pos -= 1
            # Check for { before $
            if pos > 0 and sql[pos-1] == '{':
                pos -= 1
        
        return pos
    
    # Default: return current position
    return pos


def find_type_end(sql: str, start_pos: int) -> int:
    """
    Find the end of the type name after :: operator.
    Type names can include spaces (e.g., 'double precision', 'character varying')
    and parentheses for precision (e.g., 'numeric(10,2)', 'varchar(100)').
    """
    pos = start_pos
    
    # Skip leading whitespace
    while pos < len(sql) and sql[pos].isspace():
        pos += 1
    
    if pos >= len(sql):
        return start_pos
    
    # Read the type name
    # Type can be alphanumeric with underscores, and may contain spaces for multi-word types
    type_chars = []
    
    while pos < len(sql):
        char = sql[pos]
        
        # Type names can have letters, numbers, underscores
        if char.isalnum() or char == '_':
            type_chars.append(char)
            pos += 1
        # Spaces are allowed in type names like "double precision"
        elif char.isspace():
            # Look ahead to see if there's more type name coming
            next_pos = pos + 1
            while next_pos < len(sql) and sql[next_pos].isspace():
                next_pos += 1
            
            # If next character is alphanumeric, include the space
            if next_pos < len(sql) and (sql[next_pos].isalnum() or sql[next_pos] == '_'):
                type_chars.append(char)
                pos += 1
            else:
                break
        # Parentheses for precision/scale like numeric(10,2)
        elif char == '(':
            type_chars.append(char)
            pos += 1
            # Read until closing paren
            paren_count = 1
            while pos < len(sql) and paren_count > 0:
                if sql[pos] == '(':
                    paren_count += 1
                elif sql[pos] == ')':
                    paren_count -= 1
                type_chars.append(sql[pos])
                pos += 1
        # Brackets for array types like integer[]
        elif char == '[':
            type_chars.append(char)
            pos += 1
            # Read until closing bracket
            while pos < len(sql) and sql[pos] != ']':
                type_chars.append(sql[pos])
                pos += 1
            if pos < len(sql) and sql[pos] == ']':
                type_chars.append(sql[pos])
                pos += 1
        else:
            # End of type name
            break
    
    return pos

def convert_array_overlap_to_snowflake(sql: str) -> str:
    """
    Convert PostgreSQL array overlap operator && to Snowflake ARRAYS_OVERLAP function.
    Used in post-processing to handle any remaining && not converted by sqlglot.
    Handles patterns like: ARRAY_CONSTRUCT(...) && column, column && column, etc.
    """
    # Match array expressions && array expressions
    def repl(match):
        left = match.group(1).strip()
        right = match.group(2).strip()
        # Convert any ARRAY[...] to ARRAY_CONSTRUCT(...) on both sides
        left = convert_array_to_array_construct(left)
        right = convert_array_to_array_construct(right)
        return f"ARRAYS_OVERLAP({left}, {right})"
    
    # Simple targeted replacement for common && patterns
    import re
    # Replace (SPLIT(...)) && ARRAY_CONSTRUCT(...) - multiline version
    sql = re.sub(r'\(\s*SPLIT\([^)]+\)\s*\)\s*&&\s*ARRAY_CONSTRUCT\([^)]+\)', 
                 lambda m: f"ARRAYS_OVERLAP({m.group(0).split(' && ')[0].strip()}, {m.group(0).split(' && ')[1].strip()})", 
                 sql, flags=re.DOTALL)
    # Replace SPLIT(...) && ARRAY_CONSTRUCT(...) 
    sql = re.sub(r'SPLIT\([^)]+\)\s*&&\s*ARRAY_CONSTRUCT\([^)]+\)', 
                 lambda m: f"ARRAYS_OVERLAP({m.group(0).split(' && ')[0]}, {m.group(0).split(' && ')[1]})", 
                 sql)
    # Replace ARRAY_CONSTRUCT(...) && ARRAY_CONSTRUCT(...)
    sql = re.sub(r'ARRAY_CONSTRUCT\([^)]+\)\s*&&\s*ARRAY_CONSTRUCT\([^)]+\)', 
                 lambda m: f"ARRAYS_OVERLAP({m.group(0).split(' && ')[0]}, {m.group(0).split(' && ')[1]})", 
                 sql)
    # Replace column && ARRAY_CONSTRUCT(...)
    sql = re.sub(r'[\w.]+\s*&&\s*ARRAY_CONSTRUCT\([^)]+\)', 
                 lambda m: f"ARRAYS_OVERLAP({m.group(0).split(' && ')[0]}, {m.group(0).split(' && ')[1]})", 
                 sql)
    # Replace ARRAY_CONSTRUCT(...) && column
    sql = re.sub(r'ARRAY_CONSTRUCT\([^)]+\)\s*&&\s*[\w.]+', 
                 lambda m: f"ARRAYS_OVERLAP({m.group(0).split(' && ')[0]}, {m.group(0).split(' && ')[1]})", 
                 sql)
    return sql

def convert_unnest_array_to_values(sql: str) -> str:
    """
    Replace SELECT unnest(ARRAY[...]) col1, unnest(ARRAY[...]) col2, ...
    with SELECT col1, col2 FROM (VALUES (..., ...)) AS t(col1, col2).
    Handles both single-column and multi-column parallel unnest patterns using
    bracket-level parsing (robust to parentheses inside quoted strings).
    """

    def _extract_bracket_content(s: str, pos: int):
        """Extract content between matching brackets starting at pos (pointing at '[' or '(').
        Returns (content_str, end_pos) where end_pos is the index after the closing bracket."""
        open_ch = s[pos]
        close_ch = ']' if open_ch == '[' else ')'
        depth = 1
        i = pos + 1
        in_q = False
        q_ch = None
        while i < len(s) and depth > 0:
            c = s[i]
            if in_q:
                if c == q_ch:
                    if i + 1 < len(s) and s[i + 1] == q_ch:
                        i += 2  # doubled-quote escape, skip both
                        continue
                    in_q = False
            else:
                if c in ("'", '"'):
                    in_q, q_ch = True, c
                elif c == open_ch:
                    depth += 1
                elif c == close_ch:
                    depth -= 1
                    if depth == 0:
                        return s[pos + 1:i], i + 1
            i += 1
        return s[pos + 1:], len(s)

    def _split_top_level_commas(content: str) -> list:
        """Split a string by top-level commas (not inside brackets or quotes)."""
        parts, cur, depth, in_q, q_ch = [], [], 0, False, None
        for c in content:
            if in_q:
                cur.append(c)
                if c == q_ch:
                    in_q = False
            elif c in ("'", '"'):
                in_q, q_ch = True, c
                cur.append(c)
            elif c in ('(', '['):
                depth += 1
                cur.append(c)
            elif c in (')', ']'):
                depth -= 1
                cur.append(c)
            elif c == ',' and depth == 0:
                part = ''.join(cur).strip()
                if part:
                    parts.append(part)
                cur = []
            else:
                cur.append(c)
        part = ''.join(cur).strip()
        if part:
            parts.append(part)
        return parts

    def _parse_unnest_calls(s: str, start: int):
        """Parse one or more comma-separated unnest(ARRAY[...]) [AS] colname expressions.
        Returns (columns, end_pos) where columns is [(colname, [elements]), ...],
        or (None, start) if the pattern does not match."""
        columns = []
        i = start
        while True:
            # Skip whitespace
            while i < len(s) and s[i] in ' \t\n\r':
                i += 1
            # Must start with 'unnest' (case-insensitive, not part of a longer word)
            if s[i:i + 6].lower() != 'unnest':
                break
            if i + 6 < len(s) and (s[i + 6].isalnum() or s[i + 6] == '_'):
                break  # 'unnest' is part of a longer identifier
            i += 6
            # Skip whitespace
            while i < len(s) and s[i] in ' \t\n\r':
                i += 1
            # Expect opening '(' of unnest(
            if i >= len(s) or s[i] != '(':
                return None, start
            i += 1
            # Skip whitespace
            while i < len(s) and s[i] in ' \t\n\r':
                i += 1
            # Expect ARRAY_CONSTRUCT or ARRAY keyword
            upper_rest = s[i:i + 15].upper()
            if upper_rest.startswith('ARRAY_CONSTRUCT'):
                i += 15
            elif s[i:i + 5].upper() == 'ARRAY':
                i += 5
            else:
                return None, start
            # Skip whitespace
            while i < len(s) and s[i] in ' \t\n\r':
                i += 1
            # Expect '[' or '(' opening the array literal
            if i >= len(s) or s[i] not in ('[', '('):
                return None, start
            content, i = _extract_bracket_content(s, i)
            elements = [e for e in _split_top_level_commas(content) if e]
            # Skip whitespace
            while i < len(s) and s[i] in ' \t\n\r':
                i += 1
            # Expect closing ')' of unnest(
            if i >= len(s) or s[i] != ')':
                return None, start
            i += 1
            # Skip whitespace
            while i < len(s) and s[i] in ' \t\n\r':
                i += 1
            # Optional AS keyword
            if s[i:i + 2].upper() == 'AS' and (i + 2 >= len(s) or not (s[i + 2].isalnum() or s[i + 2] == '_')):
                i += 2
                while i < len(s) and s[i] in ' \t\n\r':
                    i += 1
            # Column name (identifier)
            col_start = i
            while i < len(s) and (s[i].isalnum() or s[i] == '_'):
                i += 1
            col_name = s[col_start:i]
            if not col_name:
                return None, start
            columns.append((col_name, elements))
            # Skip whitespace
            while i < len(s) and s[i] in ' \t\n\r':
                i += 1
            # If next char is ',' check whether next non-whitespace is another 'unnest'
            if i < len(s) and s[i] == ',':
                j = i + 1
                while j < len(s) and s[j] in ' \t\n\r':
                    j += 1
                if s[j:j + 6].lower() == 'unnest':
                    i += 1  # consume the comma and loop
                    continue
                # Comma belongs to outer query; stop here
            break
        return (columns, i) if columns else (None, start)

    # Scan the SQL for SELECT keywords and attempt to replace unnest patterns
    select_re = re.compile(r'\bSELECT\b', re.IGNORECASE)
    result = []
    i = 0
    while i < len(sql):
        m = select_re.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        # Append everything before this SELECT
        result.append(sql[i:m.start()])
        j = m.end()
        # Skip whitespace after SELECT
        while j < len(sql) and sql[j] in ' \t\n\r':
            j += 1
        # Try to parse unnest calls starting here
        if sql[j:j + 6].lower() == 'unnest':
            columns, end_pos = _parse_unnest_calls(sql, j)
            if columns:
                arrays = [c[1] for c in columns]
                # Only proceed if all parallel arrays have the same length
                if arrays and all(len(a) == len(arrays[0]) for a in arrays):
                    col_names = [c[0] for c in columns]
                    rows = []
                    for row_i in range(len(arrays[0])):
                        vals = ', '.join(arrays[col_i][row_i] for col_i in range(len(arrays)))
                        rows.append(f"({vals})")
                    values_str = ',\n    '.join(rows)
                    cols_str = ', '.join(col_names)
                    result.append(f"SELECT\n    {cols_str}\n  FROM (VALUES\n    {values_str}\n  ) AS t({cols_str})")
                    i = end_pos
                    continue
        # Pattern not matched — keep SELECT as-is and advance past it
        result.append(sql[m.start():m.end()])
        i = m.end()
    return ''.join(result)

def find_matching_paren(s: str, start_pos: int) -> int:
    """Find the index of the closing parenthesis that matches the opening one at start_pos."""
    count = 1
    i = start_pos + 1
    while i < len(s) and count > 0:
        if s[i] == '(':
            count += 1
        elif s[i] == ')':
            count -= 1
        i += 1
    return i - 1 if count == 0 else -1

def simplify_unnest_flatten(sql: str) -> str:
    """
    Simplify overly complex UNNEST/EXPLODE patterns generated by sqlglot's Snowflake dialect.
    
    sqlglot converts PostgreSQL UNNEST to a complex dual-FLATTEN approach with ARRAY_GENERATE_RANGE:
    - IFF(_u.pos = _u_2.pos_2, _u_2.column, NULL) AS column
    - CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(...))) AS _u(...)
    - CROSS JOIN TABLE(FLATTEN(INPUT => array_expr)) AS _u_2(...)
    - WHERE _u.pos = _u_2.pos_2 OR (...)
    
    This function simplifies it to:
    - _f.value AS column
    - CROSS JOIN LATERAL FLATTEN(INPUT => array_expr) AS _f
    
    Processes ALL occurrences of this pattern in the SQL.
    """
    
    # Check if this pattern exists
    if not ('_u.pos = _u_2.pos_2' in sql and 'ARRAY_GENERATE_RANGE' in sql):
        return sql
    
    result = sql
    max_iterations = 50  # Safety limit to prevent infinite loops
    iteration = 0
    
    # Keep processing until no more patterns are found
    while '_u.pos = _u_2.pos_2' in result and 'ARRAY_GENERATE_RANGE' in result and iteration < max_iterations:
        iteration += 1
        old_result = result
        
        # Step 1: Replace IFF expressions with simple _f.value (only the first occurrence)
        iff_pattern = r'IFF\s*\(\s*_u\.pos\s*=\s*_u_2\.pos_2\s*,\s*_u_2\.(\w+)\s*,\s*NULL\s*\)\s+AS\s+(\w+)'
        result = re.sub(iff_pattern, r'_f.value AS \2', result, count=1, flags=re.IGNORECASE)
        
        # Step 2: Extract the array expression from the FIRST second FLATTEN before we modify it
        # Try function call pattern first (e.g., SPLIT(col, '|'))
        second_match = re.search(
            r'FLATTEN\s*\(\s*INPUT\s*=>\s*([A-Z_]+\s*\([^)]*\))\s*\)\s*\)\s*AS\s+_u_2',
            result,
            re.IGNORECASE
        )
        
        # If no function call, try simple column/expression pattern (e.g., column_name or table.column)
        if not second_match:
            second_match = re.search(
                r'FLATTEN\s*\(\s*INPUT\s*=>\s*([a-zA-Z_][\w.]*)\s*\)\s*\)\s*AS\s+_u_2',
                result,
                re.IGNORECASE
            )
        
        if not second_match:
            break
        
        array_expr = second_match.group(1).strip()
        
        # Step 3: Remove FIRST ARRAY_GENERATE_RANGE CROSS JOIN using position-based matching
        array_gen_match = re.search(
            r'\s+(CROSS\s+JOIN\s+TABLE\s*\(\s*FLATTEN\s*\(\s*INPUT\s*=>\s*ARRAY_GENERATE_RANGE)',
            result,
            re.IGNORECASE
        )
        
        if array_gen_match:
            start = array_gen_match.start()
            # Find the AS _u part
            as_u_match = re.search(r'\)\s*AS\s+_u\s*\(', result[array_gen_match.start():])
            if as_u_match:
                # Find the closing paren of _u(...)
                paren_start = array_gen_match.start() + as_u_match.end() - 1
                paren_end = find_matching_paren(result, paren_start)
                if paren_end != -1:
                    # Remove from start to after the closing paren, including trailing whitespace
                    end = paren_end + 1
                    # Skip trailing whitespace/newlines
                    while end < len(result) and result[end] in ' \t\n':
                        end += 1
                    result = result[:start] + '\n  ' + result[end:]
        
        # Step 4: Replace FIRST second FLATTEN with simple LATERAL FLATTEN
        # Match either a function call or a simple column/expression
        second_pattern = r'CROSS\s+JOIN\s+TABLE\s*\(\s*FLATTEN\s*\(\s*INPUT\s*=>\s*(?:[A-Z_]+\s*\([^)]*\)|[a-zA-Z_][\w.]*)\s*\)\s*\)\s*AS\s+_u_2\s*\([^)]+\)'
        result = re.sub(
            second_pattern,
            f'CROSS JOIN LATERAL FLATTEN(INPUT => {array_expr}) AS _f',
            result,
            count=1,  # Only replace the first occurrence
            flags=re.IGNORECASE
        )
        
        # Step 5: Remove the FIRST WHERE/AND (..._u.pos...) clause
        # First try to find as WHERE clause (beginning of conditions)
        where_match = re.search(
            r'\s+WHERE\s+[^A-Z]*?_u\.pos\s*=\s*_u_2\.pos_2',
            result,
            re.IGNORECASE
        )
        
        if where_match:
            start = where_match.start()
            # Find text after WHERE up to the condition
            where_start = re.search(r'WHERE', result[start:], re.IGNORECASE)
            if where_start:
                where_pos = start + where_start.end()
                # Find the complete condition block - look for the outer OR expression
                # Pattern: _u.pos = _u_2.pos_2 OR (...)
                condition_match = re.search(
                    r'_u\.pos\s*=\s*_u_2\.pos_2\s+OR\s+\(',
                    result[where_pos:],
                    re.IGNORECASE
                )
                if condition_match:
                    # Find the closing paren of the OR clause
                    or_paren_start = where_pos + condition_match.end() - 1
                    or_paren_end = find_matching_paren(result, or_paren_start)
                    if or_paren_end != -1:
                        # Remove entire WHERE clause from WHERE to the end of the OR clause
                        end_pos = or_paren_end + 1
                        # Skip trailing whitespace/newlines
                        while end_pos < len(result) and result[end_pos] in ' \t\n':
                            end_pos += 1
                        result = result[:start + where_start.start()] + '\n  ' + result[end_pos:]
        else:
            # Try as AND clause (middle of existing conditions)
            and_match = re.search(
                r'\s+AND\s+\(\s*_u\.pos\s*=\s*_u_2\.pos_2',
                result,
                re.IGNORECASE
            )
            
            if and_match:
                start = and_match.start()
                # Find the opening paren after AND
                paren_pos = result.find('(', and_match.start() + 4)
                if paren_pos != -1:
                    paren_end = find_matching_paren(result, paren_pos)
                    if paren_end != -1:
                        # Remove from AND to closing paren
                        result = result[:start] + result[paren_end + 1:]
        
        # If nothing changed, break to avoid infinite loop
        if result == old_result:
            break
    
    # Clean up empty parentheses and orphaned WHERE clauses
    result = re.sub(r'\(\s*\)', '', result)
    result = re.sub(r'WHERE\s*\)', ')', result, flags=re.IGNORECASE)
    
    if iteration >= max_iterations:
        logging.warning(f"simplify_unnest_flatten reached max iterations ({max_iterations})")
    
    return result

def convert_lateral_unnest_to_snowflake(sql: str) -> str:
    """
    Convert PostgreSQL CROSS JOIN LATERAL unnest(column) AS alias(column) to Snowflake equivalent.
    CROSS JOIN LATERAL unnest(column_name) AS table_alias(column_alias)
    becomes:
    CROSS JOIN LATERAL FLATTEN(input => column_name) AS table_alias
    And replace table_alias.column_alias with table_alias.value
    """
    # Helper function to extract the unnest argument handling nested parentheses
    def extract_unnest_arg(sql_text, start_pos):
        """Extract the argument from unnest(...) handling nested parentheses"""
        depth = 1
        i = start_pos
        while i < len(sql_text) and depth > 0:
            if sql_text[i] == '(':
                depth += 1
            elif sql_text[i] == ')':
                depth -= 1
            i += 1
        return sql_text[start_pos:i-1].strip()
    
    # Find all CROSS JOIN LATERAL unnest patterns
    mappings = []
    result = sql
    
    # Pattern to find CROSS JOIN LATERAL unnest
    simple_pattern = re.compile(
        r'CROSS\s+JOIN\s+LATERAL\s+unnest\s*\(',
        re.IGNORECASE
    )
    
    offset = 0
    while True:
        match = simple_pattern.search(result, offset)
        if not match:
            break
        
        # Extract the unnest argument (handling nested parentheses)
        unnest_arg_start = match.end()
        unnest_arg = extract_unnest_arg(result, unnest_arg_start)
        unnest_end = unnest_arg_start + len(unnest_arg) + 1  # +1 for closing paren
        
        # Now look for AS table_alias(column_alias) after the unnest
        as_pattern = re.compile(
            r'\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
            re.IGNORECASE
        )
        as_match = as_pattern.match(result, unnest_end)
        
        if as_match:
            table_alias = as_match.group(1)
            column_alias = as_match.group(2)
            mappings.append((table_alias, column_alias))
            
            # Replace this occurrence with FLATTEN
            replacement = f"CROSS JOIN LATERAL FLATTEN(INPUT => {unnest_arg}) AS {table_alias}"
            result = result[:match.start()] + replacement + result[as_match.end():]
            offset = match.start() + len(replacement)
        else:
            offset = match.end()
    
    # Replace all table_alias.column_alias with table_alias.value
    for table_alias, column_alias in mappings:
        # Use regex to avoid partial matches
        result = re.sub(rf'\b{re.escape(table_alias)}\.{re.escape(column_alias)}\b', f'{table_alias}.value', result)

    return result

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

def convert_array_generate_range_to_flatten(sql: str) -> str:
    """
    Convert ARRAY_GENERATE_RANGE in SELECT clauses to TABLE(FLATTEN(...)) to produce rows.
    
    Transforms:
      SELECT ARRAY_GENERATE_RANGE(start, end + 1) AS col
    To:
      SELECT f.VALUE AS col FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(start, end + 1))) AS f
    
    This handles the case when sqlglot converts generate_series to ARRAY_GENERATE_RANGE,
    which creates a single row with an array instead of multiple rows.
    """
    # Use a more sophisticated approach to handle nested parentheses
    result = []
    i = 0
    last_pos = 0  # Track the last position we've added to result
    
    while i < len(sql):
        # Look for SELECT followed by ARRAY_GENERATE_RANGE
        if sql[i:i+6].upper() == 'SELECT':
            # Case 1: SELECT CAST(ARRAY_GENERATE_RANGE(...) AS TYPE) AS alias
            cast_match = re.match(r'\s+CAST\s*\(\s*(ARRAY_GENERATE_RANGE\s*\()', sql[i+6:], re.IGNORECASE)
            if cast_match:
                select_start = i
                array_start = i + 6 + cast_match.start(1)
                paren_start = array_start + len(cast_match.group(1)) - 1

                # Find matching closing parenthesis for ARRAY_GENERATE_RANGE
                paren_count = 1
                j = paren_start + 1
                while j < len(sql) and paren_count > 0:
                    if sql[j] == '(':
                        paren_count += 1
                    elif sql[j] == ')':
                        paren_count -= 1
                    j += 1

                if paren_count == 0:
                    array_expr = sql[array_start:j]
                    after_expr = sql[j:]
                    cast_type_alias_match = re.match(
                        r'\s+AS\s+([A-Z][A-Z0-9_\s]*(?:\([^)]+\))?)\s*\)\s+AS\s+(\w+)',
                        after_expr,
                        re.IGNORECASE,
                    )

                    if cast_type_alias_match:
                        cast_type = cast_type_alias_match.group(1).strip()
                        alias = cast_type_alias_match.group(2)
                        result.append(sql[last_pos:select_start])
                        result.append(
                            f"SELECT CAST(f.VALUE AS {cast_type}) AS {alias} "
                            f"FROM TABLE(FLATTEN(INPUT => {array_expr})) AS f"
                        )
                        i = j + cast_type_alias_match.end()
                        last_pos = i
                        continue

            # Find start of ARRAY_GENERATE_RANGE
            match = re.match(r'\s+(ARRAY_GENERATE_RANGE\s*\()', sql[i+6:], re.IGNORECASE)
            if match:
                select_start = i
                array_start = i + 6 + match.start(1)
                paren_start = array_start + len(match.group(1)) - 1
                
                # Find matching closing parenthesis
                paren_count = 1
                j = paren_start + 1
                while j < len(sql) and paren_count > 0:
                    if sql[j] == '(':
                        paren_count += 1
                    elif sql[j] == ')':
                        paren_count -= 1
                    j += 1
                
                if paren_count == 0:
                    # Found complete ARRAY_GENERATE_RANGE expression
                    array_expr = sql[array_start:j]
                    
                    # Look for AS alias after the expression
                    after_expr = sql[j:]
                    alias_match = re.match(r'\s+AS\s+(\w+)', after_expr, re.IGNORECASE)
                    
                    if alias_match:
                        alias = alias_match.group(1)
                        # Add everything from last_pos to select_start
                        result.append(sql[last_pos:select_start])
                        # Build replacement
                        result.append(f"SELECT f.VALUE AS {alias} FROM TABLE(FLATTEN(INPUT => {array_expr})) AS f")
                        i = j + alias_match.end()
                        last_pos = i
                        continue
        
        i += 1
    
    # Add any remaining content
    result.append(sql[last_pos:])
    return ''.join(result)


def convert_max_array_to_array_agg(sql: str) -> str:
    """
    Convert MAX(CASE WHEN ... THEN ARRAY_CONSTRUCT(...) ELSE NULL END) to ARRAY_AGG(...)[0].
    Snowflake doesn't support MAX() on arrays. We use ARRAY_AGG which ignores NULLs by
    default, and [0] to retrieve the single element.

    Array structure is preserved so downstream array index access (col[0], col[1], etc.)
    continues to work. ARRAY_TO_STRING wrapping is handled separately in the crosstab path.

    Transforms:
      MAX(CASE WHEN condition THEN ARRAY_CONSTRUCT(...) [ELSE NULL] END)
    To:
      ARRAY_AGG(CASE WHEN condition THEN ARRAY_CONSTRUCT(...) [ELSE NULL] END)[0]
    """
    result = []
    i = 0
    n = len(sql)

    while i < n:
        # Look for MAX( (case-insensitive)
        max_match = re.match(r'MAX\s*\(', sql[i:], re.IGNORECASE)
        if not max_match:
            result.append(sql[i])
            i += 1
            continue

        # Check that what follows the opening paren starts with CASE WHEN
        inner_start = i + max_match.end()
        inner_preview = sql[inner_start:inner_start + 200].lstrip()
        if not re.match(r'CASE\s+WHEN\b', inner_preview, re.IGNORECASE):
            result.append(sql[i])
            i += 1
            continue

        # Use bracket matching to find the closing ) of MAX(
        depth = 1
        j = inner_start
        in_str = False
        str_char = ''
        while j < n and depth > 0:
            ch = sql[j]
            if in_str:
                if ch == str_char and (j == 0 or sql[j-1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            j += 1

        if depth != 0:
            result.append(sql[i])
            i += 1
            continue

        inner_content = sql[inner_start:j - 1]

        # Only transform if ARRAY_CONSTRUCT is inside
        if not re.search(r'ARRAY_CONSTRUCT\b', inner_content, re.IGNORECASE):
            result.append(sql[i])
            i += 1
            continue

        result.append(f"ARRAY_AGG({inner_content})[0]")
        i = j

    return ''.join(result)

def fix_aggregate_distinct_iff(sql: str) -> str:
    """
    Fix IFF(condition, DISTINCT expression, NULL) → DISTINCT IFF(condition, expression, NULL).

    sqlglot places DISTINCT as the "then" argument of IFF when converting
    FILTER (WHERE ...) clauses that contain DISTINCT. The previous regex-based
    approach was broken because `.+?` with DOTALL could match across multiple
    IFF calls in the same query, consuming one IFF's condition as part of the
    "condition" capture group of an earlier match and silently dropping DISTINCT
    from the intended target.

    This implementation is paren-balanced: it scans for each IFF(, walks through
    the argument list while respecting nested parens/strings, and only rewrites
    when the second argument (the "then" value) starts with DISTINCT.
    """
    result = []
    i = 0
    n = len(sql)

    while i < n:
        # Look for IFF( at the current position
        m = re.match(r'IFF\s*\(', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        iff_start = i
        args_start = i + m.end()  # index right after the opening '('

        # Walk through the IFF arguments using balanced-paren scanning.
        # We need to find the positions of the two top-level commas that
        # separate arg1 (condition), arg2 (then), and arg3 (else).
        depth = 1
        in_str = False
        str_char = ''
        top_level_commas = []
        j = args_start

        while j < n and depth > 0:
            ch = sql[j]
            if in_str:
                if ch == str_char and (j == 0 or sql[j - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    break
            elif ch == ',' and depth == 1:
                top_level_commas.append(j)
            j += 1

        if depth != 0 or len(top_level_commas) < 2:
            # Unbalanced or not a 3-arg IFF — pass through char by char
            result.append(sql[i])
            i += 1
            continue

        iff_end = j + 1  # one past the closing ')'
        comma1 = top_level_commas[0]
        comma2 = top_level_commas[1]

        condition = sql[args_start:comma1].strip()
        then_val = sql[comma1 + 1:comma2].strip()
        else_val = sql[comma2 + 1:j].strip()

        # Only rewrite when:
        #  - the then-value starts with DISTINCT (case-insensitive)
        #  - the else-value is NULL
        distinct_m = re.match(r'DISTINCT\s+', then_val, re.IGNORECASE)
        if distinct_m and else_val.upper() == 'NULL':
            expr = then_val[distinct_m.end():]
            result.append(f"DISTINCT IFF({condition}, {expr}, NULL)")
        else:
            result.append(sql[iff_start:iff_end])

        i = iff_end

    return ''.join(result)

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


def rewrite_string_agg_filter_to_conditional(sql: str) -> str:
    """
    Rewrite PostgreSQL STRING_AGG(... ) FILTER (WHERE ...) to a conditional first argument.

    sqlglot currently drops FILTER for STRING_AGG during Postgres->Snowflake conversion,
    which changes query semantics. This pre-processing step rewrites:

      STRING_AGG(expr, delim ORDER BY ... ) FILTER (WHERE cond)

    into:

    STRING_AGG(IFF(cond, expr, NULL), delim ORDER BY ...)

    This preserves semantics and lets sqlglot emit correct Snowflake LISTAGG output.
    """
    def find_matching_paren(text: str, open_idx: int) -> int:
        depth = 1
        i = open_idx + 1
        in_str = False
        str_char = ''

        while i < len(text):
            ch = text[i]
            if in_str:
                if ch == str_char and (i == 0 or text[i - 1] != '\\'):
                    in_str = False
            elif ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return i
            i += 1

        return -1

    def find_top_level_comma(text: str) -> int:
        depth = 0
        in_str = False
        str_char = ''

        for i, ch in enumerate(text):
            if in_str:
                if ch == str_char and (i == 0 or text[i - 1] != '\\'):
                    in_str = False
                continue

            if ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                return i

        return -1

    result = []
    i = 0
    n = len(sql)

    while i < n:
        m = re.match(r'STRING_AGG\s*\(', sql[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i])
            i += 1
            continue

        func_open = i + m.end() - 1
        func_close = find_matching_paren(sql, func_open)
        if func_close == -1:
            result.append(sql[i])
            i += 1
            continue

        args = sql[func_open + 1:func_close]
        comma_idx = find_top_level_comma(args)
        if comma_idx == -1:
            # Malformed STRING_AGG args; leave untouched.
            result.append(sql[i:func_close + 1])
            i = func_close + 1
            continue

        first_arg = args[:comma_idx].strip()
        rest_args = args[comma_idx + 1:].strip()

        # Detect optional FILTER (WHERE ...) immediately after STRING_AGG(...).
        j = func_close + 1
        while j < n and sql[j].isspace():
            j += 1

        filter_match = re.match(r'FILTER\s*\(', sql[j:], re.IGNORECASE)
        if not filter_match:
            result.append(sql[i:func_close + 1])
            i = func_close + 1
            continue

        filter_open = j + filter_match.end() - 1
        filter_close = find_matching_paren(sql, filter_open)
        if filter_close == -1:
            result.append(sql[i:func_close + 1])
            i = func_close + 1
            continue

        filter_inner = sql[filter_open + 1:filter_close].strip()
        where_match = re.match(r'WHERE\s+(.+)', filter_inner, re.IGNORECASE | re.DOTALL)
        if not where_match:
            result.append(sql[i:filter_close + 1])
            i = filter_close + 1
            continue

        condition = where_match.group(1).strip()

        distinct_match = re.match(r'DISTINCT\s+(.+)', first_arg, re.IGNORECASE | re.DOTALL)
        if distinct_match:
            expr = distinct_match.group(1).strip()
            conditional_first_arg = f"DISTINCT IFF({condition}, {expr}, NULL)"
        else:
            conditional_first_arg = f"IFF({condition}, {first_arg}, NULL)"

        rewritten = f"STRING_AGG({conditional_first_arg}, {rest_args})"
        result.append(rewritten)
        i = filter_close + 1

    return ''.join(result)