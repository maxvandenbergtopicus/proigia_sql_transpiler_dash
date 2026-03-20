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
            # Convert GREATEST to GREATEST_IGNORE_NULLS for Snowflake
            exp.Greatest: lambda self, e: f"GREATEST_IGNORE_NULLS({', '.join(self.sql(arg) for arg in ([e.this] if e.this else []) + (e.expressions or []))})",
        }
        
        def extract_from_age_sql(self, expression: exp.Extract) -> str:
            """Handle EXTRACT(YEAR/MONTH FROM AGE(...)) conversions"""
            unit = expression.this.this.upper()
            age_expr = expression.expression
            args = age_expr.expressions
            
            if unit == "YEAR":
                # EXTRACT(YEAR FROM AGE(end, start)) -> DATEDIFF(year, start, end)
                if len(args) == 2:
                    return f"DATEDIFF(year, {self.sql(args[1])}, {self.sql(args[0])})"
                elif len(args) == 1:
                    return f"DATEDIFF(year, {self.sql(args[0])}, CURRENT_TIMESTAMP())"
            elif unit == "MONTH":
                # EXTRACT(MONTH FROM AGE(end, start)) -> MOD(DATEDIFF(month, start, end), 12)
                if len(args) == 2:
                    return f"DATEDIFF(month, {self.sql(args[1])}, {self.sql(args[0])})"
                elif len(args) == 1:
                    return f"DATEDIFF(month, {self.sql(args[0])}, CURRENT_TIMESTAMP())"
            elif unit == "DAY":
                # EXTRACT(DAY FROM AGE(end, start)) -> DATEDIFF(day, start, end)
                if len(args) == 2:
                    return f"DATEDIFF(day, {self.sql(args[1])}, {self.sql(args[0])})"
                elif len(args) == 1:
                    return f"DATEDIFF(day, {self.sql(args[0])}, CURRENT_TIMESTAMP())"
            
            # Fallback to default AGE conversion
            return f"DATEDIFF(month, {self.sql(args[1]) if len(args) == 2 else self.sql(args[0])}, {self.sql(args[0]) if len(args) == 2 else 'CURRENT_TIMESTAMP()'})"
        
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
                    return f"REGEXP_SUBSTR({value}, {pattern})"
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
                
                # If we successfully calculated a static rowcount, use it
                if rowcount is not None and base_start_expr is not None and offset_value_int is not None:
                    # Use the base expression (without the offset) as the actual start
                    # and adjust using SEQ4() starting from negative offset
                    base_start = self.sql(base_start_expr)
                    
                    return (
                        f"TABLE(GENERATOR(ROWCOUNT => {rowcount})), "
                        f"LATERAL (SELECT DATEADD({step_unit}, -{offset_value_int} + (SEQ4() * {step_value}), {base_start}) AS VALUE)"
                    )
                else:
                    # Fallback: Use dynamic DATEDIFF (may fail if not constant in Snowflake)
                    # Render start and end as SQL strings
                    start = self.sql(start_expr) if start_expr else "0"
                    end = self.sql(end_expr) if end_expr else "0"
                    rowcount_expr = f"DATEDIFF({step_unit}, {start}, {end}) / {step_value} + 1"
                    return (
                        f"TABLE(GENERATOR(ROWCOUNT => {rowcount_expr})), "
                        f"LATERAL (SELECT DATEADD({step_unit}, SEQ4() * {step_value}, {start}) AS VALUE)"
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
            """Handle function conversions (AGE, ARRAYS_OVERLAP, ARRAY_POSITION, CHAR_LENGTH)"""
            if expression.this.upper() == "AGE":
                args = expression.expressions
                if len(args) == 2:
                    # AGE(end, start) -> DATEDIFF(month, start, end) for better precision
                    return f"DATEDIFF(year, {self.sql(args[1])}, {self.sql(args[0])})"
                elif len(args) == 1:
                    # AGE(timestamp) -> DATEDIFF(month, timestamp, CURRENT_TIMESTAMP())
                    return f"DATEDIFF(year, {self.sql(args[0])}, CURRENT_TIMESTAMP())"
            
            if expression.this.upper() == "ARRAYS_OVERLAP":
                args = expression.expressions
                return f"ARRAYS_OVERLAP({self.sql(args[0])}, {self.sql(args[1])})"
            
            if expression.this.upper() == "ARRAY_POSITION":
                args = expression.expressions
                if len(args) == 2:
                    # PostgreSQL: ARRAY_POSITION(array, value) -> Snowflake: ARRAY_POSITION(value, array)
                    return f"ARRAY_POSITION({self.sql(args[1])}, {self.sql(args[0])})"
            
            if expression.this.upper() == "CHAR_LENGTH":
                args = expression.expressions
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
    Convert ARRAY_REMOVE(array, value) to ARRAY_REMOVE(array, TO_VARIANT(value))
    to ensure proper type matching in Snowflake.
    Processes all occurrences by repeatedly finding and wrapping one at a time.
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
            value_expr = sql[value_start:value_end-1].strip()
            
            # Wrap this value and reconstruct SQL
            sql = (sql[:value_start] + 
                   f"TO_VARIANT({value_expr})" + 
                   sql[value_end-1:])
            found = True
            break  # Process one at a time, restart from beginning
        
        if not found:
            break  # No more unwrapped calls
    
    return sql

def convert_cast_to_try_cast(sql: str) -> str:
    """
    Convert CAST(... AS DATE|DECIMAL|NUMBER) to TRY_TO_DATE/TRY_TO_NUMBER/TRY_TO_DECIMAL functions.
    Wraps the inner expression in TO_VARCHAR to ensure proper type handling for VARIANT types.
    """
    # Map target types to their conversion functions
    type_to_function = {
        'DATE': 'TRY_TO_DATE',
        'DECIMAL': 'TRY_TO_DECIMAL',
        'NUMBER': 'TRY_TO_NUMBER',
        'NUMERIC': 'TRY_TO_NUMERIC'
    }
    
    # Process each target type
    for dtype, func_name in type_to_function.items():
        # Find all CAST occurrences
        result = []
        i = 0
        while i < len(sql):
            # Look for CAST(
            match = re.match(r'\bCAST\s*\(', sql[i:], re.IGNORECASE)
            if match:
                # Found CAST(, now find the matching closing parenthesis
                start = i
                i += match.end()
                paren_count = 1
                expr_start = i
                
                # Track parentheses to find the end of CAST
                while i < len(sql) and paren_count > 0:
                    if sql[i] == '(':
                        paren_count += 1
                    elif sql[i] == ')':
                        paren_count -= 1
                    i += 1
                
                if paren_count == 0:
                    # Extract the full CAST expression
                    cast_content = sql[expr_start:i-1]
                    
                    # Check if this CAST is for our target type
                    # Look for AS TYPE at the end of the expression
                    as_pattern = rf'\s+AS\s+{dtype}\s*$'
                    if re.search(as_pattern, cast_content, re.IGNORECASE):
                        # Extract the expression being cast (before AS)
                        as_match = re.search(as_pattern, cast_content, re.IGNORECASE)
                        expr = cast_content[:as_match.start()].strip()
                        
                        # Replace with TRY_TO_DATE/TRY_TO_NUMBER/TRY_TO_DECIMAL function
                        # Wrap expression in TO_VARCHAR to handle VARIANT types
                        result.append(f'{func_name}(TO_VARCHAR({expr}))')
                    else:
                        # Keep original CAST
                        result.append(sql[start:i])
                else:
                    # Malformed, keep original
                    result.append(sql[start:i])
            else:
                result.append(sql[i])
                i += 1
        
        sql = ''.join(result)
    
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
    - element <= ALL(array) -> element <= ARRAY_MIN(array)
    - element < ALL(array) -> element < ARRAY_MIN(array)
    - element >= ALL(array) -> element >= ARRAY_MAX(array)
    - element > ALL(array) -> element > ARRAY_MAX(array)
    - element = ALL(array) -> ARRAY_SIZE(ARRAY_DISTINCT(array)) = 1 AND ARRAY_CONTAINS(element, array)
    - element <> ALL(array) -> NOT ARRAY_CONTAINS(element, array)
    - element != ALL(array) -> NOT ARRAY_CONTAINS(element, array)
    """
    
    # Process each operator type
    operators = [
        ('<=', lambda l, a: f"{l} <= ARRAY_MIN({a})"),
        ('>=', lambda l, a: f"{l} >= ARRAY_MAX({a})"),
        ('<>', lambda l, a: f"NOT ARRAY_CONTAINS(TO_VARIANT({l}), {a})"),
        ('!=', lambda l, a: f"NOT ARRAY_CONTAINS(TO_VARIANT({l}), {a})"),
        ('<', lambda l, a: f"{l} < ARRAY_MIN({a})"),
        ('>', lambda l, a: f"{l} > ARRAY_MAX({a})"),
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
            (i == 0 or not sql[i-1].isalnum() and sql[i-1] != '_')):
            quote_char = sql[i+1]
            result.append(quote_char)  # Remove the E, keep the quote
            i += 2  # Skip past E'
            
            # Process the string content
            while i < len(sql):
                if sql[i] == '\\' and i + 1 < len(sql):
                    next_char = sql[i+1]
                    if next_char == '\\':
                        # E'\\' -> keep as \\ in output
                        result.append('\\\\')
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
            else:  # || pattern
                expr = match.group(3).strip()
                unit_str = match.group(4).strip()
            
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

def convert_postgres_to_snowflake(sql: str, function_macros: list = None) -> str:
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
    
    # ============================================================================
    # PRE-PROCESSING: Transform PostgreSQL-specific syntax before sqlglot parsing
    # ============================================================================
    
    # Convert PostgreSQL escape strings E'...' to regular strings with proper escaping
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
    
    # Pre-process: Convert PostgreSQL regex match operator ~ to RLIKE
    if '~' in sql:
        logging.info("Converting PostgreSQL regex match operator ~ to RLIKE")
        # Replace ~ with RLIKE (but not ~* which is case-insensitive and handled separately)
        sql = re.sub(r'(\s)~(\s)', r'\1RLIKE\2', sql)
    
    # Pre-process: Convert ARRAY[...] to ARRAY_CONSTRUCT(...)
    if 'array[' in sql.lower():
        logging.info("Converting ARRAY[...] to ARRAY_CONSTRUCT(...)")
        sql = convert_array_to_array_construct(sql)
    
    # Pre-process: Replace ::bigint with ::int (Snowflake uses INT for big integers)
    if '::bigint' in sql.lower():
        logging.info("Converting ::bigint to ::int")
        sql = re.sub(r'::bigint\b', '::int', sql, flags=re.IGNORECASE)
    
    # Pre-process: Remove PostgreSQL array type casts (::text[], ::varchar[], etc.)
    if '::' in sql and '[]' in sql:
        logging.info("Removing PostgreSQL array type casts")
        sql = re.sub(r'::(text|varchar|character varying|integer|int|bigint|smallint|numeric|float|double precision|boolean|date|timestamp)\[\]', '', sql, flags=re.IGNORECASE)
    
    # Pre-process: Handle crosstab function (not supported in Snowflake)
    if re.search(r'\bcrosstab\s*\(', sql, re.IGNORECASE):
        sql = handle_crosstab(sql)

    # Pre-process: Convert unnest(ARRAY[...]) to SELECT ... FROM VALUES (...)
    sql = convert_unnest_array_to_values(sql) # 19 files affected

    # Pre-process: Convert CROSS JOIN LATERAL unnest(...) to Snowflake FLATTEN 
    sql = convert_lateral_unnest_to_snowflake(sql) # 2 files affected
    
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
    
    # Post-process: Convert CAST(... AS DATE|DECIMAL|NUMBER) to TRY_TO_DATE/TRY_TO_NUMBER/TRY_TO_DECIMAL
    if 'CAST(' in converted.upper():
        logging.info("Converting CAST to TRY_TO_DATE/TRY_TO_NUMBER/TRY_TO_DECIMAL for DATE, DECIMAL, NUMBER types")
        converted = convert_cast_to_try_cast(converted)
    
    # Post-process: Wrap ARRAY_REMOVE value parameter in TO_VARIANT
    if 'array_remove(' in converted.lower():
        logging.info("Wrapping ARRAY_REMOVE value parameters in TO_VARIANT")
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


    # Post-process: Convert LIKE/ILIKE ANY(column1 || column2 || ...) to LIKE/ILIKE ANY(ARRAY_CONCAT(SPLIT(column1, ','), SPLIT(column2, ','), ...))
    def like_any_concat_repl(match):
        like_op = match.group(1).upper()  # LIKE or ILIKE
        concat_expr = match.group(2)
        # Split by || and strip
        columns = [col.strip() for col in re.split(r'\s*\|\|\s*', concat_expr)]
        # Build ARRAY_CONCAT(ARRAY_TO_STRING(col, ','), ...)
        splits = [f"ARRAY_TO_STRING({col}, ',')" for col in columns]
        if len(splits) == 1:
            return f"{like_op} ANY({splits[0]})"
        else:
            return f"{like_op} ANY(CONCAT({', '.join(splits)}))"

    converted = re.sub(
        r"(I?LIKE)\s+ANY\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\s*\|\|\s*[a-zA-Z_][a-zA-Z0-9_]*)+)\s*\)",
        like_any_concat_repl,
        converted,
        flags=re.IGNORECASE
    )

    # Post-process: Convert LIKE/ILIKE ANY((SELECT column FROM ...)) to LIKE/ILIKE ANY((SELECT ARRAY_TO_STRING(column, ',')))
    def like_any_select_repl(match):
        like_op = match.group(1).upper()  # LIKE or ILIKE
        column = match.group(2).strip()
        rest_of_query = match.group(3).strip()
        # Normalize whitespace in the rest of the query for better formatting
        rest_of_query = ' '.join(rest_of_query.split())
        return f"{like_op} ANY ((SELECT ARRAY_TO_STRING({column}, ',') {rest_of_query}))"

    converted = re.sub(
        r"(I?LIKE)\s+ANY\s*\(\s*\(\s*SELECT\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(FROM\s+.*?)\)\s*\)",
        like_any_select_repl,
        converted,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Post-process: Convert JOIN table ON col LIKE ANY(array) to use ARRAY_TO_STRING
    # This MUST run BEFORE like_any_column_repl to avoid double-wrapping
    if re.search(r'JOIN\s+.*\s+ON\s+.*\s+(LIKE|ILIKE)\s+ANY\s*\(', converted, re.IGNORECASE | re.DOTALL):
        logging.info("Converting JOIN ... ON ... LIKE ANY(array) to ARRAY_TO_STRING wrapper")
        converted = convert_join_like_any_to_array_to_string(converted)

    # Post-process: Convert LIKE/ILIKE ANY(column) to LIKE/ILIKE ANY(ARRAY_TO_STRING(column, ','))
    # Skip if already wrapped with ARRAY_TO_STRING
    def like_any_column_repl(match):
        like_op = match.group(1).upper()  # LIKE or ILIKE
        column = match.group(2)
        return f"{like_op} ANY(ARRAY_TO_STRING({column}, ','))"

    converted = re.sub(
        r"(I?LIKE)\s+ANY\s*\(\s*(?!ARRAY_TO_STRING)([a-zA-Z_][a-zA-Z0-9_\.]*)\s*\)",
        like_any_column_repl,
        converted,
        flags=re.IGNORECASE
    )

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

    return converted

def wrap_array_agg_with_nullif(sql: str) -> str:
    """
    Wrap ARRAY_AGG(...) with NULLIF(..., []) to match PostgreSQL NULL semantics.
    In PostgreSQL, ARRAY_AGG returns NULL for empty input rows; in Snowflake it returns [].
    NULLIF(ARRAY_AGG(...), []) restores that NULL-on-empty behavior.

    Also handles ARRAY_AGG(...) WITHIN GROUP (ORDER BY ...) as a single unit.
    Skips any ARRAY_AGG that is already the direct argument of NULLIF.
    """
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
    pattern_func = re.compile(r"([a-zA-Z_][\w\.]*\s*\([^()]*(?:\([^()]*\)[^()]*)*\))\s*::\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\([^)]+\))?)(?=\s*(?:[,/\*\+\-<>=!]|END\b|AS\b|AND\b|OR\b|\)|$))", re.IGNORECASE)
    sql = pattern_func.sub(cast_repl, sql)
    
    # Then, match other patterns (quoted strings, simple parenthesis, array access, identifiers)
    pattern_other = re.compile(r"((?:'[^']*'|\"[^\"]*\"|\([^()]+\)|[a-zA-Z_][\w\.]*\[[^\]]+\]|[a-zA-Z_][\w\.]+))\s*::\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\([^)]+\))?)(?=\s*(?:[,/\*\+\-<>=!]|END\b|AS\b|AND\b|OR\b|\)|$))", re.IGNORECASE)
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
    Replace SELECT unnest(ARRAY[...] or ARRAY_CONSTRUCT(...)) col with SELECT col FROM (VALUES (...)) AS t(col)
    Handles both single and multi-line arrays.
    """
    # Match pattern with optional AS keyword: unnest(ARRAY[...] or ARRAY_CONSTRUCT(...)) [AS] col_name
    pattern = re.compile(r"SELECT\s+unnest\s*\(\s*(ARRAY(?:_CONSTRUCT)?)\s*[\(\[](.*?)[)\]]\s*\)\s+(?:AS\s+)?(\w+)", re.DOTALL | re.IGNORECASE)

    def repl(match):
        array_type = match.group(1).upper()  # ARRAY or ARRAY_CONSTRUCT
        array_content = match.group(2)
        col = match.group(3)
        
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
    Fix IFF(condition, DISTINCT expression, NULL) to DISTINCT IFF(condition, expression, NULL)
    This moves DISTINCT outside the IFF for proper aggregate function syntax.
    """
    sql = re.sub(
        r"IFF\s*\(\s*(.+?)\s*,\s*DISTINCT\s*(.+?)\s*,\s*NULL\s*\)",
        r"DISTINCT IFF(\1, \2, NULL)",
        sql,
        flags=re.IGNORECASE | re.DOTALL
    )
    
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