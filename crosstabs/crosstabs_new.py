
import re
import logging
import sqlglot

# Ensure INFO-level logs are shown in the console
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def cast_array_constructs_to_variant(sql: str, array_name: str = None) -> tuple[str, int]:
    """
    Cast ARRAY_CONSTRUCT(...) to VARIANT (ARRAY_CONSTRUCT(...)::variant).

    If array_name is provided, only casts the ARRAY_CONSTRUCT that has that alias.
    If array_name is None, casts all matching ARRAY_CONSTRUCT calls.

    This is needed for crosstab payload/value arrays, including ARRAY_CONSTRUCT
    calls that appear inside CASE expressions where the enclosing alias is on the
    CASE ... END expression rather than on ARRAY_CONSTRUCT itself.
    """
    result = []
    i = 0
    cast_count = 0
    pattern = re.compile(r'ARRAY_CONSTRUCT\s*\(', re.IGNORECASE)

    while i < len(sql):
        match = pattern.match(sql, i)
        if not match:
            result.append(sql[i])
            i += 1
            continue

        func_start = i
        paren_depth = 1
        j = match.end()
        in_string = False
        string_char = None

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

            j += 1

        if paren_depth != 0:
            result.append(sql[i])
            i += 1
            continue

        func_sql = sql[func_start:j]
        
        # Check if this ARRAY_CONSTRUCT should be casted
        should_cast = False
        if array_name:
            # Strategy 1: ARRAY_CONSTRUCT(...) AS array_name — alias directly follows closing paren
            rest = sql[j:]
            direct_alias = re.match(rf'\s*AS\s+{re.escape(array_name)}\b', rest, re.IGNORECASE)

            # Strategy 2: CASE ... THEN ARRAY_CONSTRUCT(...) ... ELSE ... END AS array_name
            # The alias is on the END keyword, not on the ARRAY_CONSTRUCT itself.
            # Detect by checking if preceded by THEN and then scanning forward for END AS array_name.
            preceded_by_then = re.search(r'\bTHEN\s*$', sql[:func_start], re.IGNORECASE)
            if preceded_by_then:
                # Look for END AS array_name in the text following this ARRAY_CONSTRUCT's close
                in_case_alias = re.search(rf'\bEND\s+AS\s+{re.escape(array_name)}\b', rest, re.IGNORECASE)
                should_cast = bool(in_case_alias)
            else:
                should_cast = bool(direct_alias)
        else:
            # Legacy: cast all ARRAY_CONSTRUCT calls, except those already inside ARRAY_TO_STRING
            prefix = sql[:func_start].rstrip()
            should_cast = not prefix.lower().endswith('array_to_string(')
        
        if should_cast:
            prefix = sql[:func_start].rstrip()
            if not prefix.lower().endswith('array_to_string('):
                result.append(f"{func_sql}::variant")
                cast_count += 1
            else:
                result.append(func_sql)
        else:
            result.append(func_sql)

        i = j

    return ''.join(result), cast_count


def parse_crosstab_to_macro(sql: str) -> str:
    """
    Convert PostgreSQL crosstab to Snowflake pivot macro call.
    
    Structure: SELECT * FROM crosstab($$ query1 $$, $$ query2 $$) AS (cols...)
    - query1: Main query with data (patient_id, category, values)
    - query2: Query to get distinct categories
    - cols: Output column definitions
    """
    logging.debug("="*80)
    logging.debug("Converting PostgreSQL crosstab to Snowflake macro")
    
    # Extract $$ blocks
    dollar_blocks = re.findall(r'\$\$(.*?)\$\$', sql, re.DOTALL | re.IGNORECASE)
    if len(dollar_blocks) < 2:
        logging.error(f"Expected 2 $$ blocks, found {len(dollar_blocks)}")
        return ""
    
    query1 = dollar_blocks[0].strip()  # Main data query
    query2 = dollar_blocks[1].strip()  # Category query
    
    # Check if query1 contains CTEs (WITH clause) and extract them
    inner_ctes = ""
    query1_without_ctes = query1
    
    with_match = re.match(r'^\s*WITH\s+', query1, re.IGNORECASE)
    if with_match:
        logging.info("Detected CTEs inside query1, extracting them")
        
        # Find the last SELECT statement (the main query after all CTEs)
        # Strategy: Find all top-level SELECT keywords and take the last one
        # We need to track parenthesis depth to avoid matching SELECTs inside CTEs
        
        # Find the position of the main SELECT (after all CTEs)
        depth = 0
        in_string = False
        string_char = None
        select_positions = []
        i = 0
        
        while i < len(query1):
            char = query1[i]
            
            # Track string literals
            if char in ('"', "'") and (i == 0 or query1[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            # Track parenthesis depth (only outside strings)
            if not in_string:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                # Look for SELECT at depth 0
                elif depth == 0 and query1[i:i+6].upper() == 'SELECT':
                    select_positions.append(i)
            
            i += 1
        
        if select_positions:
            # Use the last SELECT at depth 0 (the main query)
            main_select_pos = select_positions[-1]
            
            # Everything before the main SELECT is CTEs
            inner_ctes = query1[:main_select_pos].strip()
            # Remove trailing comma if present
            inner_ctes = re.sub(r',\s*$', '', inner_ctes)
            # Strip leading "WITH " if present
            inner_ctes = inner_ctes.lstrip('WITH ').strip()

            # --- Split and print each CTE one by one ---
            def extract_ctes(ctes_sql):
                transformed_ctes = []
                i = 0
                n = len(ctes_sql)
                while i < n:
                    # Skip whitespace and comments
                    while i < n and ctes_sql[i] in ' \n\r\t':
                        i += 1
                    # Skip SQL single-line comments
                    if ctes_sql[i:i+2] == '--':
                        while i < n and ctes_sql[i] != '\n':
                            i += 1
                        i += 1
                        continue
                    # Find CTE name
                    name_match = re.match(r'\s*([a-zA-Z_][\w]*)\s+AS\s*\(', ctes_sql[i:], re.IGNORECASE)
                    if not name_match:
                        break
                    name = name_match.group(1)
                    i += name_match.end()
                    # Find body by tracking parentheses
                    depth = 1
                    body_start = i
                    in_string = False
                    string_char = ''
                    while i < n and depth > 0:
                        char = ctes_sql[i]
                        if not in_string and char in ('"', "'"):
                            in_string = True
                            string_char = char
                        elif in_string and char == string_char:
                            in_string = False
                            string_char = ''
                        elif not in_string:
                            if char == '(':
                                depth += 1
                            elif char == ')':
                                depth -= 1
                        i += 1
                    body_end = i - 1
                    body = ctes_sql[body_start:body_end].strip()
                    # Convert the body using the shared converter pipeline so CTEs
                    # get the same post-processing (including generate_series fixes).
                    try:
                        from code.functions.dialect_converter import convert_postgres_to_snowflake
                        transpiled_body = convert_postgres_to_snowflake(body)
                    except Exception as e:
                        logging.warning(f"Failed full conversion for CTE {name}: {e}; falling back to sqlglot transpile")
                        try:
                            transpiled_body = sqlglot.transpile(body, read="postgres", write="snowflake", pretty=True)[0]
                        except Exception as e2:
                            logging.warning(f"Failed to transpile CTE {name}: {e2}")
                            transpiled_body = body  # Fallback to original
                    
                    # Post-process: Fix ARRAY_AGG(IFF(NOT x IS NULL, DISTINCT x, NULL)) to ARRAY_AGG(DISTINCT x)
                    transpiled_body = re.sub(
                        r"ARRAY_AGG\(\s*IFF\(\s*NOT\s+([a-zA-Z0-9_]+)\s+IS\s+NULL\s*,\s*DISTINCT\s+\1\s*,\s*NULL\s*\)\s*\)",
                        r"ARRAY_AGG(DISTINCT \1)",
                        transpiled_body,
                        flags=re.IGNORECASE | re.DOTALL
                    )
                    
                    msg = f"\n{'#'*40}\n### INDIVIDUAL CTE EXTRACTED: {name} ###\n{'-'*40}\nWITH {name} AS (\n{transpiled_body}\n)\n{'#'*40}\n"
                    logging.info(msg)
                    # Add to transformed CTEs list
                    transformed_ctes.append(f"{name} AS (\n{transpiled_body}\n)")
                    # Skip whitespace and comments after CTE
                    while i < n and ctes_sql[i] in ' \n\r\t':
                        i += 1
                    if ctes_sql[i:i+2] == '--':
                        while i < n and ctes_sql[i] != '\n':
                            i += 1
                        i += 1
                    # Skip comma after CTE
                    if i < n and ctes_sql[i] == ',':
                        i += 1
                return transformed_ctes
            if inner_ctes:
                logging.info(f"Calling extract_ctes with inner_ctes (length={len(inner_ctes)})")
                transformed_ctes_list = extract_ctes(inner_ctes)
                logging.info("Finished extract_ctes call")

            # The main query is everything from SELECT onwards
            query1_without_ctes = query1[main_select_pos:].strip()

            logging.debug(f"Extracted {len(select_positions)} SELECT statements, using last one as main query")
            logging.debug(f"Inner CTEs length: {len(inner_ctes)} chars")
            logging.debug(f"Main query length: {len(query1_without_ctes)} chars")
        else:
            logging.warning("Found WITH but no SELECT statements")
            query1_without_ctes = query1
    # Match until the closing ) of the column definition
    # Pattern handles: ...AS ct(...)) or ...AS ct(...) or ...AS ct(...);
    # Allow whitespace/newlines between AS and identifier and opening paren
    output_match = re.search(r'\)\s*as\s+(\w*)\s*\((.+?)\)\s*(?:\)|;|$)', sql, re.IGNORECASE | re.DOTALL)
    if not output_match:
        logging.error("Could not find output columns")
        return ""
    
    output_section = output_match.group(2).strip()
    
    # Extract any trailing SELECT after the crosstab column definitions
    # e.g. "SELECT unique_field[1]::bigint AS patient_id, * FROM pre;"
    trailing_text = sql[output_match.end():].strip().rstrip(';').strip()
    if re.match(r'SELECT\b', trailing_text, re.IGNORECASE):
        # Find the outer CTE name (e.g. "WITH pre AS (") so we can replace it with draaitabel_ct
        outer_cte_match = re.match(r'^\s*WITH\s+(\w+)\s+AS\s*\(', sql, re.IGNORECASE)
        if outer_cte_match:
            outer_cte_name = outer_cte_match.group(1)
            trailing_text = re.sub(
                r'\bFROM\s+' + re.escape(outer_cte_name) + r'\b',
                'FROM draaitabel_ct',
                trailing_text,
                flags=re.IGNORECASE
            )
        final_select = trailing_text
        logging.info(f"Detected trailing SELECT after crosstab, using it as final query (replaced CTE ref with draaitabel_ct)")
    else:
        final_select = "SELECT * FROM draaitabel_ct"

    logging.debug(f"Output section before processing: {output_section[:200]}...")
    
    # Check for Jinja include, macro call, or preserved macro placeholder
    has_macro = ('{% include' in output_section or '{{' in output_section or '__DBT_MACRO_' in output_section)
    
    if has_macro:
        logging.info("Detected Jinja template or preserved macro in output columns - using macro call for column list")
        
        # Extract the macro name from {% include 'name.pry' %}
        include_match = re.search(r"{%\s*include\s+['\"](.+?)['\"]", output_section)
        if include_match:
            macro_name = include_match.group(1).replace('.pry', '').replace('.sql', '')
            logging.debug(f"Using macro: {macro_name}")
        else:
            # Try to extract from {{ macro() }}
            macro_match = re.search(r'{{\s*(\w+)\s*\(\s*\)\s*}}', output_section)
            if macro_match:
                macro_name = macro_match.group(1)
            else:
                # Check for preserved macro placeholder __DBT_MACRO_N__
                placeholder_match = re.search(r'__DBT_MACRO_(\d+)__', output_section)
                if placeholder_match:
                    # Extract just the placeholder identifier, we'll use it as the macro name
                    macro_name = f"__DBT_MACRO_{placeholder_match.group(1)}__"
                else:
                    logging.warning("Could not determine macro name")
                    macro_name = "unknown_macro"
        
        # Parse only the columns BEFORE the include/macro (these are the ID columns)
        # Remove the macro/include part and placeholders completely first
        cols_before_macro = re.sub(r'{%.*?%}|\{\{.*?\}\}|__DBT_MACRO_\d+__', '', output_section, flags=re.DOTALL).strip()
        if cols_before_macro and cols_before_macro.rstrip(','):
            # Parse column names (before type declarations)
            output_cols = [col.strip().split()[0] for col in cols_before_macro.rstrip(',').split(',') if col.strip()]
        else:
            output_cols = []
        
        use_macro_for_columns = macro_name
        logging.debug(f"Set use_macro_for_columns = {use_macro_for_columns}")
        logging.debug(f"Parsed output_cols (ID columns): {output_cols}")
    else:
        output_cols = [col.strip().split()[0] for col in output_section.split(',')]
        use_macro_for_columns = None
        logging.debug(f"No Jinja detected, parsed all output_cols: {output_cols}")
    
    logging.debug(f"Output columns: {output_cols}")
    
    # Parse query1 SELECT columns (use query1_without_ctes to avoid parsing CTE selects)
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query1_without_ctes, re.IGNORECASE | re.DOTALL)
    if not select_match:
        logging.error("No SELECT found in query1")
        return ""
    
    # Parse columns from SELECT (handle nested parens/brackets)
    select_clause = select_match.group(1)
    
    # Strip DISTINCT ON clause if present
    distinct_on_match = re.match(r'DISTINCT\s+ON\s*\([^)]*\)\s+', select_clause, re.IGNORECASE)
    if distinct_on_match:
        select_clause = select_clause[distinct_on_match.end():].strip()
        logging.debug("Stripped DISTINCT ON from select clause")
    
    columns = []
    current = ''
    depth = 0
    
    for char in select_clause:
        if char in '([': depth += 1
        elif char in ')]': depth -= 1
        elif char == ',' and depth == 0:
            if current.strip(): columns.append(current.strip())
            current = ''
            continue
        current += char
    if current.strip(): columns.append(current.strip())
    
    # Extract column aliases
    cte_cols = []
    for col in columns:
        as_match = re.search(r'\s+as\s+(\w+)', col, re.IGNORECASE)
        cte_cols.append(as_match.group(1) if as_match else col.split()[0].split('.')[-1])
    
    logging.debug(f"CTE columns: {cte_cols}")
    
    # Analyze ARRAY construction to determine aantal_split and eventuele_extra_split
    aantal_split = None
    eventuele_extra_split = None
    contains_aggregation = False
    array_column_expr = None
    
    # Find the values column that contains the ARRAY
    for col_expr in columns:
        if 'ARRAY[' in col_expr.upper() or 'ARRAY_CONSTRUCT' in col_expr.upper():
            array_column_expr = col_expr
            
            # Extract array content between brackets/parens (with or without 'as alias')
            if 'ARRAY[' in col_expr.upper():
                match = re.search(r'ARRAY\[(.*)\](?:\s+as)?', col_expr, re.IGNORECASE | re.DOTALL)
            else:
                match = re.search(r'ARRAY_CONSTRUCT\((.*)\)(?:\s+as)?', col_expr, re.IGNORECASE | re.DOTALL)
            
            if match:
                array_content = match.group(1)
                
                # Split by commas at depth 0 to get individual elements
                array_elements = []
                current_elem = ''
                depth = 0
                
                for char in array_content:
                    if char in '([': depth += 1
                    elif char in ')]': depth -= 1
                    elif char == ',' and depth == 0:
                        if current_elem.strip():
                            array_elements.append(current_elem.strip())
                        current_elem = ''
                        continue
                    current_elem += char
                
                if current_elem.strip():
                    array_elements.append(current_elem.strip())
                
                aantal_split = len(array_elements)
                
                # Check for aggregation functions
                agg_functions = ['max', 'min', 'count', 'sum', 'avg']
                for elem in array_elements:
                    if any(f'{func}('.upper() in elem.upper() for func in agg_functions):
                        contains_aggregation = True
                        break
                
                # Check for nested arrays (array_agg)
                for idx, elem in enumerate(array_elements, start=1):
                    if 'array_agg'.upper() in elem.upper() or 'ARRAY[' in elem.upper():
                        eventuele_extra_split = idx
                        break
                
                logging.debug(f"Array has {aantal_split} elements, aggregation={contains_aggregation}, nested_at={eventuele_extra_split}")
                break
    
    if aantal_split is None:
        logging.warning("Could not determine aantal_split, using default 0")
        aantal_split = 0
    
    # Determine column roles
    id_cols = [c for c in cte_cols if c in output_cols]
    pivot_cols = [c for c in cte_cols if c not in output_cols]
    
    if len(pivot_cols) < 2:
        logging.error(f"Need at least 2 pivot columns, found {len(pivot_cols)}")
        return ""
    
    pivot_key = pivot_cols[0]    # Category column
    array_name = pivot_cols[1]   # Values column
    
    # The pivoted category values are the output columns minus the id columns
    pivoted_values = [c for c in output_cols if c not in id_cols]
    
    # Parse column types from output_section
    column_types = {}
    for col_def in output_section.split(','):
        col_def = col_def.strip()
        if col_def:
            parts = col_def.split()
            if len(parts) >= 2:
                col_name = parts[0]
                col_type = ' '.join(parts[1:])
                column_types[col_name] = col_type
    
    # Determine output_type: 'scalar' if all pivoted columns are scalar types, 'array' otherwise
    output_type = 'array'  # default
    if pivoted_values:
        is_scalar = True
        for col in pivoted_values:
            col_type = column_types.get(col, '').upper()
            if '[]' in col_type or 'ARRAY' in col_type:
                is_scalar = False
                break
        if is_scalar:
            output_type = 'scalar'
    
    logging.debug(f"Column types: {column_types}")
    logging.debug(f"Output type: {output_type}")
    logging.debug(f"ID columns: {id_cols}")
    logging.debug(f"Pivot key: {pivot_key}, Array: {array_name}")
    logging.debug(f"Pivoted category values: {pivoted_values}")
    
    # Build macro call
    id_cols_str = "[" + ", ".join(f"'{c}'" for c in id_cols) + "]"
    eventuele_extra_split_str = str(eventuele_extra_split) if eventuele_extra_split is not None else 'none'
    
    # Handle column list - use macro if detected, otherwise static list
    macro_variable_declaration = ""
    if use_macro_for_columns:
        # Set variable first, then use it in the function call
        if use_macro_for_columns.startswith('__DBT_MACRO_'):
            # Placeholder - it will be restored to {{ macro() }}, but we need just macro()
            # So we keep the placeholder and it will be restored in the set statement context
            # dbt_wrapper will restore it to {{ macro() }}, but we need to strip those
            # Actually, let's just use the placeholder as-is and handle restoration differently
            macro_variable_declaration = f"{{%- set categorie_list = {use_macro_for_columns} -%}}\n"
        else:
            # Regular macro name - call it directly without {{ }}
            macro_variable_declaration = f"{{%- set categorie_list = {use_macro_for_columns}() -%}}\n"
        pivoted_values_str = "categorie_list"
        logging.debug(f"Using macro for column list with variable declaration")
    else:
        pivoted_values = [c for c in output_cols if c not in id_cols]
        pivoted_values_str = "[" + ", ".join(f"'{c}'" for c in pivoted_values) + "]"
        logging.debug(f"Using static column list: {pivoted_values_str}")
    
    # Convert ARRAY[] to ARRAY_CONSTRUCT and cast matching values to VARIANT
    # Use query1_without_ctes (the SELECT part only, not the CTEs)
    modified_query1 = query1_without_ctes
    
    logging.info(f"=== Array processing: array_column_expr={array_column_expr}")
    
    # Always convert ARRAY[] to ARRAY_CONSTRUCT()
    modified_query1 = re.sub(r'ARRAY\s*\[(.*?)\]', r'ARRAY_CONSTRUCT(\1)', modified_query1, flags=re.IGNORECASE | re.DOTALL)
    
    # Only cast the ARRAY_CONSTRUCT that's aliased as the values column (array_name)
    modified_query1, cast_count = cast_array_constructs_to_variant(modified_query1, array_name)
    if cast_count:
        logging.info(f"Cast {cast_count} ARRAY_CONSTRUCT occurrence(s) to ::variant")
    else:
        logging.warning("Could not find ARRAY_CONSTRUCT to cast")
    
    # Handle single quotes in pivot_key - if it contains single quotes, use double quotes for the string
    if "'" in pivot_key:
        pivot_key_quoted = f'"{pivot_key}"'
    else:
        pivot_key_quoted = f"'{pivot_key}'"
    
    # Similarly handle array_name
    if "'" in array_name:
        array_name_quoted = f'"{array_name}"'
    else:
        array_name_quoted = f"'{array_name}'"
    
    # Transpile the main query from PostgreSQL to Snowflake (after array processing)
    try:
        from code.functions.dialect_converter import convert_postgres_to_snowflake
        modified_query1 = convert_postgres_to_snowflake(modified_query1, wrap_array_to_string=False)
        # After transpilation, optionally resolve the alias for complex pivot expressions.
        # For simple identifiers (like fi_cat), keep the original value to avoid false
        # positives where substring matches (fi_cat in fi_cat_ind) overwrite the key.
        pivot_key_name = pivot_key.strip().strip('"').strip("'")
        is_simple_identifier = bool(re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', pivot_key_name))

        if not is_simple_identifier:
            # Look for the pivot_key expression in the transpiled query
            # (sqlglot may add spaces around operators like ||).
            pivot_key_normalized = pivot_key.replace('||', ' || ')
            pivot_key_pattern = re.compile(rf'(^|\W){re.escape(pivot_key_normalized)}(\W|$)', re.IGNORECASE)

            # Find lines containing the pivot expression with AS alias
            lines = modified_query1.split('\n')
            for line in lines:
                line = line.strip()
                if ' AS ' not in line.upper():
                    continue

                if pivot_key_pattern.search(line):
                    # Extract the alias after AS
                    as_match = re.search(r'\s+AS\s+(\w+)', line, re.IGNORECASE)
                    if as_match:
                        pivot_key = as_match.group(1)
                        logging.info(f"Found alias for pivot column: {pivot_key}")
                        break
        # Update the quoted version
        if "'" in pivot_key:
            pivot_key_quoted = f'"{pivot_key}"'
        else:
            pivot_key_quoted = f"'{pivot_key}'"
    except Exception as e:
        logging.warning(f"Failed to transpile main query: {e}")
        # modified_query1 remains as is

    # Enable index ordering only for known ordinal pivot keys.
    pivot_key_name = pivot_key.strip().strip("\"").strip("'")
    index_ordering_keys = {"NHGNR"}
    index_ordering_arg = ", index_ordering=true" if pivot_key_name.upper() in index_ordering_keys else ""
    if index_ordering_arg:
        logging.info(f"Enabling snowflake_pivot index_ordering=true (pivot key is {pivot_key_name})")
    
    # Post-process: Fix ARRAY_AGG(IFF(NOT x IS NULL, DISTINCT x, NULL)) to ARRAY_AGG(DISTINCT x)
    modified_query1 = re.sub(
        r"ARRAY_AGG\(\s*IFF\(\s*NOT\s+([a-zA-Z0-9_]+)\s+IS\s+NULL\s*,\s*DISTINCT\s+\1\s*,\s*NULL\s*\)\s*\)",
        r"ARRAY_AGG(DISTINCT \1)",
        modified_query1,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # Build the result - if there are inner CTEs, place them before the prepare CTE
    if inner_ctes:
        ctes_part = "WITH " + ",\n".join(transformed_ctes_list) + ","
        result = f"""-- CTEs extracted from crosstab query
{macro_variable_declaration}{ctes_part}
prepare AS (
{modified_query1}
)
-- Call snowflake pivot macro
{{{{snowflake_pivot({pivoted_values_str},{array_name_quoted}, {pivot_key_quoted}, {aantal_split},{eventuele_extra_split_str}, {id_cols_str}, output_type='{output_type}'{index_ordering_arg})}}}}

{final_select}"""
    else:
        result = f"""-- Prepare CTE from original query
{macro_variable_declaration}WITH prepare AS (
{modified_query1}
)
-- Call snowflake pivot macro
{{{{snowflake_pivot({pivoted_values_str},{array_name_quoted}, {pivot_key_quoted}, {aantal_split},{eventuele_extra_split_str}, {id_cols_str}, output_type='{output_type}'{index_ordering_arg})}}}}

{final_select}"""
    
    logging.debug("="*80)
    logging.debug("RESULT:")
    logging.debug(result)
    logging.debug("="*80)
    
    return result


# Backwards compatibility
def parse_crosstab_sql(sql: str) -> str:
    return parse_crosstab_to_macro(sql)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    
    if len(sys.argv) < 2:
        print("Usage: python crosstabs_new.py <sql_file>")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    if not input_file.exists():
        print(f"Error: File not found: {input_file}")
        sys.exit(1)
    
    print(f"\n{'='*80}\nProcessing: {input_file}\n{'='*80}\n")
    
    sql_content = input_file.read_text(encoding='utf-8')
    result = parse_crosstab_to_macro(sql_content)
    
    if result:
        print(f"\n{'='*80}\nCONVERTED SQL:\n{'='*80}\n")
        print(result)
        print(f"\n{'='*80}\n")
        
        # Create output files folder at same level as input folder
        # If input is in sql_files/input_files/, output goes to sql_files/output files/
        if input_file.parent.name in ['input_files', 'input files']:
            output_dir = input_file.parent.parent / 'output files'
        else:
            output_dir = input_file.parent / 'output files'
        
        output_dir.mkdir(exist_ok=True)
        
        # Save to output folder with same filename
        output_file = output_dir / input_file.name
        output_file.write_text(result, encoding='utf-8')
        print(f"Output saved to: {output_file}")
    else:
        print("\n[ERROR] Conversion failed!")
        sys.exit(1)