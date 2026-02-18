import sys
import re
import logging
import sqlglot
from sqlglot import exp
from sqlglot.dialects.snowflake import Snowflake

from crosstabs.crosstabs_new import parse_crosstab_sql

# ---- Custom Dialect Definition ----
class FixedSnowflake(Snowflake):
    class Generator(Snowflake.Generator):
        # Override TRANSFORMS to handle EXTRACT(YEAR FROM AGE(...)) pattern and ArrayOverlaps
        TRANSFORMS = {
            **Snowflake.Generator.TRANSFORMS,
            exp.Extract: lambda self, e: (
                self.anonymous_sql(e.expression)
                if e.this and e.this.this == "YEAR" and isinstance(e.expression, exp.Anonymous) and e.expression.this.upper() == "AGE"
                else Snowflake.Generator.TRANSFORMS[exp.Extract](self, e)
            ),
            exp.ArrayOverlaps: lambda self, e: self.arrayoverlaps_sql(e),
            exp.DPipe: lambda self, e: self.dpipe_sql(e),
            exp.Substring: lambda self, e: self.substring_sql(e),
        }
        
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
            
            if expression.this.upper() == "ARRAYS_OVERLAP":
                args = expression.expressions
                return f"ARRAYS_OVERLAP({self.sql(args[0])}, {self.sql(args[1])})"
            
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
        'NUMBER': 'TRY_TO_NUMBER'
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
        ('=', lambda l, a: f"ARRAY_CONTAINS({l}, {a})"),
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

def convert_join_like_any_to_lateral_flatten(sql: str) -> str:
    """Convert JOIN table ON col LIKE ANY(array) to Snowflake LATERAL FLATTEN syntax.
    
    Snowflake doesn't support ON clause with LATERAL FLATTEN, so we convert:
    JOIN table ON col LIKE ANY(array) AND other_conditions
    
    To:
    CROSS JOIN table, LATERAL FLATTEN(input => array) AS f
    WHERE col LIKE f.value AND other_conditions
    
    Handles LEFT/RIGHT/INNER JOIN types as well.
    """
    # Pattern to match [LEFT|RIGHT|INNER] JOIN ... ON ... LIKE ANY (...)
    # Captures the join type (if any) separately
    pattern = re.compile(
        r'(LEFT|RIGHT|INNER)?\s*JOIN\s+([\w\{\}\.\'\']+(?:\s+(?:AS\s+)?\w+)?)\s+ON\s+([\w\.]+)\s+(LIKE|ILIKE)\s+ANY\s*\(\s*([^\)]+?)\s*\)(.*?)(?=\s*(?:UNION|FROM|WHERE|GROUP|ORDER|JOIN|$))',
        re.IGNORECASE | re.DOTALL
    )
    
    def replace_join(match):
        join_type = match.group(1)  # LEFT, RIGHT, INNER, or None
        table_ref = match.group(2).strip()
        column_ref = match.group(3).strip()
        like_op = match.group(4).upper()
        array_ref = match.group(5).strip()
        trailing = match.group(6) if match.group(6) else ""
        
        # Extract AND conditions from trailing text
        and_conditions = ""
        if trailing.strip():
            # Look for AND at the start of trailing text
            and_match = re.match(r'\s*AND\s+(.+)', trailing, re.DOTALL | re.IGNORECASE)
            if and_match:
                and_conditions = and_match.group(1).strip()
                trailing = ""  # Consumed the AND part
        
        # Build replacement based on join type
        if join_type:
            # For LEFT/RIGHT/INNER JOIN, preserve the join type
            join_prefix = join_type.upper()
            result = f" {join_prefix} JOIN {table_ref}, LATERAL FLATTEN(input => {array_ref}) AS f{trailing}"
        else:
            # For simple JOIN, use CROSS JOIN
            result = f" CROSS JOIN {table_ref}, LATERAL FLATTEN(input => {array_ref}) AS f{trailing}"
        
        # Mark WHERE condition for injection
        where_cond = f"{column_ref} {like_op} f.value"
        if and_conditions:
            where_cond += f"\n    AND {and_conditions}"
        result += f" /*WHERE:{where_cond}*/"
        
        return result
    
    # Replace all JOIN patterns
    sql = pattern.sub(replace_join, sql)
    
    # Now inject WHERE clauses - find each /*WHERE:...*/ and inject before next clause keyword
    while True:
        marker_match = re.search(r'/\*WHERE:(.*?)\*/', sql, re.DOTALL)
        if not marker_match:
            break
            
        where_condition = marker_match.group(1).strip()
        marker_start = marker_match.start()
        marker_end = marker_match.end()
        
        # Find the insertion point: before closing paren (subquery), UNION, JOIN, GROUP, ORDER, etc.
        after_marker = sql[marker_end:]
        
        # Check for closing parenthesis (end of subquery) - this is highest priority
        next_close_paren = re.search(r'\s*\)', after_marker)
        # Check for JOIN
        next_join = re.search(r'\s+((?:LEFT|RIGHT|INNER|CROSS)?\s*JOIN)\s+', after_marker, re.IGNORECASE)
        # Check for other clause keywords
        next_clause = re.search(r'\s+(\bUNION\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b)', after_marker, re.IGNORECASE)
        
        # Determine insertion point (use the nearest one)
        candidates = []
        if next_close_paren:
            candidates.append(next_close_paren.start())
        if next_join:
            candidates.append(next_join.start())
        if next_clause:
            candidates.append(next_clause.start())
        
        if candidates:
            insert_pos = marker_end + min(candidates)
        else:
            # No keywords found, insert at end of line or before semicolon
            eol = re.search(r'(;|\n\s*$|$)', after_marker)
            if eol:
                insert_pos = marker_end + eol.start()
            else:
                insert_pos = len(sql)
        
        if insert_pos:
            section_to_check = sql[marker_end:insert_pos]
            
            # Check if there's already a WHERE clause in this section
            existing_where = re.search(r'\bWHERE\b', section_to_check, re.IGNORECASE)
            
            if existing_where:
                # There's already a WHERE, append with AND instead
                sql = sql[:marker_start] + sql[marker_end:insert_pos] + f"\n  AND {where_condition}" + sql[insert_pos:]
            else:
                # No WHERE yet, add new WHERE clause
                sql = sql[:marker_start] + sql[marker_end:insert_pos] + f"\n  WHERE {where_condition}" + sql[insert_pos:]
        else:
            # Just remove marker
            sql = sql[:marker_start] + sql[marker_end:]
    
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
    
    # Pre-process: Convert PostgreSQL date arithmetic to Snowflake DATEADD
    sql = convert_date_arithmetic_to_snowflake(sql) # 600 filers affected
    
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
    try:
        # Parse with PostgreSQL dialect
        parsed = sqlglot.parse_one(sql, read="postgres")
        # Generate with custom Snowflake dialect
        converted = parsed.sql(dialect=FixedSnowflake, pretty=True)
    except Exception as e:
        logging.info(f"[Error] Failed to convert SQL with sqlglot: {e}\n")
        logging.info("Continuing with pre-processed SQL and applying post-processing steps")
    
    # ============================================================================
    # POST-PROCESSING: Fix patterns that sqlglot doesn't handle correctly
    # ============================================================================
    
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

    # Post-process: Convert LIKE/ILIKE ANY(ARRAY_CONSTRUCT(...)) to LIKE/ILIKE ANY('...', ...)
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

    # Post-process: Convert LIKE/ILIKE ANY(column) to LIKE/ILIKE ANY(ARRAY_TO_STRING(column, ','))
    def like_any_column_repl(match):
        like_op = match.group(1).upper()  # LIKE or ILIKE
        column = match.group(2)
        return f"{like_op} ANY(ARRAY_TO_STRING({column}, ','))"

    converted = re.sub(
        r"(I?LIKE)\s+ANY\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)",
        like_any_column_repl,
        converted,
        flags=re.IGNORECASE
    )

    # Post-process: Convert ARRAY_AGG(IFF(..., expr ORDER BY ..., NULL)) to ARRAY_AGG(IFF(..., expr, NULL)) WITHIN GROUP(ORDER BY ...)
    def array_agg_iff_orderby_repl(match):
        condition = match.group(1)
        expr = match.group(2)
        orderby = match.group(3)
        return f"ARRAY_AGG(\n  IFF(\n    {condition},\n    {expr},\n    NULL\n  )\n) WITHIN GROUP(ORDER BY {orderby})"

    converted = re.sub(
        r"ARRAY_AGG\(\s*IFF\(\s*(.*?),\s*(.*?)\s+ORDER\s+BY\s+(.*?),\s*NULL\s*\)\s*\)",
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

    # Post-process: Convert JOIN table ON col LIKE ANY(array) to LATERAL FLATTEN
    if re.search(r'JOIN\s+.*\s+ON\s+.*\s+(LIKE|ILIKE)\s+ANY\s*\(', converted, re.IGNORECASE | re.DOTALL):
        logging.info("Converting JOIN ... ON ... LIKE ANY(array) to LATERAL FLATTEN")
        converted = convert_join_like_any_to_lateral_flatten(converted)

    return converted

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
    # Find all matches and collect mappings before modifying
    pattern = re.compile(
        r'CROSS\s+JOIN\s+LATERAL\s+unnest\s*\(\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*\)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
        re.IGNORECASE
    )
    mappings = []
    for match in pattern.finditer(sql):
        table_alias = match.group(2)
        column_alias = match.group(3)
        mappings.append((table_alias, column_alias))

    # Replace the unnest patterns with FLATTEN
    sql = pattern.sub(lambda m: f"CROSS JOIN LATERAL FLATTEN(input => {m.group(1)}) AS {m.group(2)}", sql)

    # Replace all table_alias.column_alias with table_alias.value
    for table_alias, column_alias in mappings:
        # Use regex to avoid partial matches
        sql = re.sub(rf'\b{re.escape(table_alias)}\.{re.escape(column_alias)}\b', f'{table_alias}.value', sql)

    return sql

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
    # Pattern to match SELECT with ARRAY_GENERATE_RANGE
    # Match: SELECT ARRAY_GENERATE_RANGE(start, end[, step]) AS alias
    pattern = re.compile(
        r'SELECT\s+(ARRAY_GENERATE_RANGE\s*\([^)]+\))\s+AS\s+(\w+)',
        re.IGNORECASE
    )
    
    def repl(match):
        array_expr = match.group(1)
        alias = match.group(2)
        
        # Convert to FLATTEN to generate rows
        return f"SELECT f.VALUE AS {alias} FROM TABLE(FLATTEN(INPUT => {array_expr})) AS f"
    
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
        (\w+(?:\.\w+)?(?:::\w+)?)  # base column (with optional table.column qualification and ::type cast)
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
    
    # Pattern 1b: Handle (function(...) +/- 'N unit'::interval)::cast
    # Example: (max(voorschrijfdatum) + '1 year'::interval)::date
    pattern1b = re.compile(
        r'\((\w+)\(([^)]+)\)\s*([-+])\s*\'([\d\.]+)\s+(year|month|week|day|hour|minute|second)s?\'::interval\)::\s*(\w+)',
        re.IGNORECASE
    )
    
    def repl1b(match):
        func_name = match.group(1)
        func_args = match.group(2)
        operator = match.group(3)
        amount = match.group(4)
        unit = match.group(5).upper()
        cast_type = match.group(6).upper()
        
        # Build DATEADD - wrap the arguments in TRY_TO_DATE/TO_VARCHAR if cast is DATE
        amount_val = f"-{amount}" if operator == '-' else amount
        if cast_type == 'DATE':
            # Wrap the function arguments to ensure they're properly cast to date
            wrapped_args = f"TRY_TO_DATE(TO_VARCHAR({func_args}))"
            return f"DATEADD({unit}, {amount_val}, {func_name}({wrapped_args}))"
        else:
            return f"DATEADD({unit}, {amount_val}, {func_name}({func_args}))"
    
    sql = pattern1b.sub(repl1b, sql)
    
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
    
    # Pattern 2a_pre: Specifically handle FUNCTION(...)::cast +/- interval
    # This pattern uses recursion - we apply it multiple times until no more matches
    # This way it handles nested functions from innermost to outermost
    def convert_func_cast_interval(sql_text):
        """Convert FUNCTION(args)::cast +/- 'N unit'::interval to DATEADD"""
        # Match: word(anything)::word-'number unit'::interval
        # We use a non-greedy match for the function arguments and apply recursively
        pattern = re.compile(
            r'\b(\w+)\(([^()]*)\)::\s*(\w+)\s*([-+])\s*\'([\d\.]+)\s+(year|month|week|day|hour|minute|second)s?\'::interval',
            re.IGNORECASE | re.DOTALL
        )
        
        # Apply the pattern recursively until no more matches
        max_iterations = 10
        for iteration in range(max_iterations):
            new_sql = pattern.sub(
                lambda m: f"DATEADD({m.group(6).upper()}, {'-' + m.group(5) if m.group(4) == '-' else m.group(5)}, {m.group(1)}({m.group(2)}))",
                sql_text
            )
            if new_sql == sql_text:
                break
            sql_text = new_sql
        
        return sql_text
    
    # Apply the conversion for function(...)::cast +/- interval
    sql = convert_func_cast_interval(sql)
    
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
            
            # Check if there's a type cast after the closing paren (e.g., ::date, allowing whitespace)
            cast_match = re.match(r'::\s*\w+', sql_text[paren_end:], re.DOTALL)
            check_start = paren_end
            if cast_match:
                base_expr += cast_match.group(0)
                check_start = paren_end + cast_match.end()
            
            # Check if followed by +/- interval
            interval_match = re.match(
                r"\s*([-+])\s*'([\d\.]+)\s+(year|month|week|day|hour|minute|second)s?'::interval",
                sql_text[check_start:],
                re.IGNORECASE
            )
            
            if interval_match:
                operator = interval_match.group(1)
                amount = interval_match.group(2)
                unit = interval_match.group(3).upper()
                
                # Convert to DATEADD
                amount_val = f"-{amount}" if operator == '-' else amount
                result.append(f"DATEADD({unit}, {amount_val}, {base_expr})")
                i = check_start + interval_match.end()
            else:
                result.append(base_expr)
                i = check_start
        
        return ''.join(result)
    
    sql = repl2a(sql)
    
    # Pattern 2b: Simple column or literal + 'N unit'::interval
    # Capture: base expression, operator (+ or -), number, unit
    pattern2b = re.compile(
        r"(\w+(?:\.\w+)?(?:::\w+)?|'[^']+'::\w+)"  # simple column or literal with cast (supports table.column)
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
        r"(\([^)]+\)(?:::\w+)?|'[^']+'::\w+|\w+(?:\.\w+)?(?:::\w+)?)"  # base: parenthesized expr, string literal, or column (with optional table.column and cast)
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
    
    # Helper function to match balanced parentheses from a position
    def extract_balanced_expression(text, start_pos):
        """Extract a balanced parenthesized expression starting at start_pos."""
        if start_pos >= len(text) or text[start_pos] != '(':
            return None, start_pos
        
        depth = 0
        i = start_pos
        while i < len(text):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    return text[start_pos:i+1], i+1
            i += 1
        return None, start_pos
    
    # Pattern for base_expr [+|-] 'N unit'::interval with proper DATEADD handling
    # Process this iteratively to handle nested DATEADD expressions
    def process_chained_intervals(sql_text):
        """Process chained interval arithmetic, handling DATEADD expressions correctly."""
        result = []
        i = 0
        
        while i < len(sql_text):
            # Look for DATEADD or other expressions followed by interval arithmetic
            # Match: DATEADD(...) +/- 'N unit'::interval
            dateadd_match = re.match(r'DATEADD\s*\(', sql_text[i:], re.IGNORECASE)
            
            if dateadd_match:
                # Extract the full DATEADD expression with balanced parens
                dateadd_start = i
                expr, end_pos = extract_balanced_expression(sql_text, i + dateadd_match.end() - 1)
                if expr:
                    full_dateadd = sql_text[i:end_pos]
                    
                    # Check if followed by +/- interval
                    interval_match = re.match(
                        r"\s*([-+])\s*'([\d\.]+)\s*(year|month|week|day|hour|minute|second)s?'::interval",
                        sql_text[end_pos:],
                        re.IGNORECASE
                    )
                    
                    if interval_match:
                        operator = interval_match.group(1)
                        amount = interval_match.group(2)
                        unit = interval_match.group(3).upper()
                        amount_val = f"-{amount}" if operator == '-' else amount
                        result.append(f"DATEADD({unit}, {amount_val}, {full_dateadd})")
                        i = end_pos + interval_match.end()
                    else:
                        result.append(full_dateadd)
                        i = end_pos
                else:
                    result.append(sql_text[i])
                    i += 1
            else:
                # Look for other patterns: column/literal +/- interval
                other_match = re.match(
                    r"([\w'\"]+(?:\.[\w'\"]+)?(?:::\w+)?)"  # simple base (column with optional table.column, literal with cast)
                    r"\s*([-+])\s*"
                    r"'([\d\.]+)\s*(year|month|week|day|hour|minute|second)s?'::interval",
                    sql_text[i:],
                    re.IGNORECASE
                )
                
                if other_match:
                    base = other_match.group(1)
                    operator = other_match.group(2)
                    amount = other_match.group(3)
                    unit = other_match.group(4).upper()
                    amount_val = f"-{amount}" if operator == '-' else amount
                    result.append(f"DATEADD({unit}, {amount_val}, {base})")
                    i += other_match.end()
                else:
                    result.append(sql_text[i])
                    i += 1
        
        return ''.join(result)
    
    sql = process_chained_intervals(sql)
    
    # Pattern 4 (run LAST): Edge case for integer columns representing months (validtime, loopduur, etc.)
    # Handles: date_expr - table.column or date_expr + table.column
    # where column is validtime, loopduur, looptijd (integer months)
    # This pattern runs after all interval conversions so it can handle DATEADD results
    def process_month_columns(sql_text):
        """Process DATEADD expressions followed by month column arithmetic."""
        result = []
        i = 0
        
        while i < len(sql_text):
            dateadd_match = re.match(r'DATEADD\s*\(', sql_text[i:], re.IGNORECASE)
            
            if dateadd_match:
                expr, end_pos = extract_balanced_expression(sql_text, i + dateadd_match.end() - 1)
                if expr:
                    full_dateadd = sql_text[i:end_pos]
                    
                    # Check if followed by +/- month column
                    month_match = re.match(
                        r"\s*([-+])\s*(\w+\.)?(validtime|loopduur|looptijd)\b",
                        sql_text[end_pos:],
                        re.IGNORECASE
                    )
                    
                    if month_match:
                        operator = month_match.group(1)
                        table_prefix = month_match.group(2) if month_match.group(2) else ''
                        column_name = month_match.group(3)
                        full_column = f"{table_prefix}{column_name}"
                        amount_expr = f"-{full_column}" if operator == '-' else full_column
                        result.append(f"DATEADD(MONTH, {amount_expr}, {full_dateadd})")
                        i = end_pos + month_match.end()
                    else:
                        result.append(full_dateadd)
                        i = end_pos
                else:
                    result.append(sql_text[i])
                    i += 1
            else:
                result.append(sql_text[i])
                i += 1
        
        return ''.join(result)
    
    sql = process_month_columns(sql)
    
    # Pattern 5: Handle remaining INTERVAL expressions (from sqlglot conversion)
    # Matches patterns like: function(...) + INTERVAL 'N' UNIT or column + INTERVAL 'N' UNIT
    # This needs to handle function calls properly
    def process_remaining_intervals(sql_text):
        """Process INTERVAL expressions that sqlglot partially converted."""
        result = []
        i = 0
        
        interval_pattern = re.compile(
            r"\s*([-+])\s*INTERVAL\s+'([0-9.]+)'\s+(YEAR|MONTH|WEEK|DAY|HOUR|MINUTE|SECOND)S?",
            re.IGNORECASE
        )
        
        while i < len(sql_text):
            # Search for INTERVAL starting from position i
            match = interval_pattern.search(sql_text, i)
            if not match:
                result.append(sql_text[i:])
                break
            
            # Found an interval - need to find the base expression before it
            interval_start = match.start()
            operator = match.group(1)
            amount = match.group(2)
            unit = match.group(3).upper()
            
            # Look backward from interval_start to find the base expression
            # Skip whitespace before the operator
            expr_end = interval_start
            while expr_end > i and sql_text[expr_end - 1].isspace():
                expr_end -= 1
            
            # Now find the start of the base expression
            expr_start = expr_end - 1
            if expr_start < i:
                # Can't find base expression, skip this interval
                result.append(sql_text[i:match.end()])
                i = match.end()
                continue
            
            # If it ends with ), find the matching opening paren (could be a function)
            if sql_text[expr_end - 1] == ')':
                paren_count = 1
                expr_start = expr_end - 2
                while expr_start >= i and paren_count > 0:
                    if sql_text[expr_start] == ')':
                        paren_count += 1
                    elif sql_text[expr_start] == '(':
                        paren_count -= 1
                    expr_start -= 1
                expr_start += 1
                
                # Check if there's a function name before the opening paren
                temp = expr_start - 1
                while temp >= i and sql_text[temp].isspace():
                    temp -= 1
                
                if temp >= i and (sql_text[temp].isalnum() or sql_text[temp] == '_'):
                    # There's a function name, go back to include it
                    while temp >= i and (sql_text[temp].isalnum() or sql_text[temp] == '_'):
                        temp -= 1
                    expr_start = temp + 1
            else:
                # Simple identifier or literal - go back to find its start
                while expr_start >= i and (sql_text[expr_start].isalnum() or sql_text[expr_start] in ('_', '.', "'", '"')):
                    expr_start -= 1
                expr_start += 1
            
            # Extract the base expression
            base_expr = sql_text[expr_start:expr_end].strip()
            
            if not base_expr:
                # Couldn't find base expression, skip
                result.append(sql_text[i:match.end()])
                i = match.end()
                continue
            
            # Apply operator to amount
            amount_val = f"-{amount}" if operator == '-' else amount
            
            # Add everything before the base expression
            result.append(sql_text[i:expr_start])
            # Add the DATEADD replacement
            result.append(f"DATEADD({unit}, {amount_val}, {base_expr})")
            i = match.end()
        
        return ''.join(result)
    
    sql = process_remaining_intervals(sql)
    
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