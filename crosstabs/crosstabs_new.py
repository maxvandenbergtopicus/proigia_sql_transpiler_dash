import re
import logging


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
    
    # Extract output columns from ) AS result (col1 type, col2 type, ...)
    # Match until the closing ) of the column definition
    # Pattern handles: ...AS ct(...)) or ...AS ct(...) or ...AS ct(...);
    # Allow whitespace/newlines between AS and identifier and opening paren
    output_match = re.search(r'\)\s*as\s+(\w*)\s*\((.+?)\)\s*(?:\)|;|$)', sql, re.IGNORECASE | re.DOTALL)
    if not output_match:
        logging.error("Could not find output columns")
        return ""
    
    output_section = output_match.group(2).strip()
    
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
    
    # Parse query1 SELECT columns
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query1, re.IGNORECASE | re.DOTALL)
    if not select_match:
        logging.error("No SELECT found in query1")
        return ""
    
    # Parse columns from SELECT (handle nested parens/brackets)
    select_clause = select_match.group(1)
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
            
            # Extract array content between brackets/parens
            if 'ARRAY[' in col_expr.upper():
                match = re.search(r'ARRAY\[(.*)\]\s+as', col_expr, re.IGNORECASE | re.DOTALL)
            else:
                match = re.search(r'ARRAY_CONSTRUCT\((.*)\)\s+as', col_expr, re.IGNORECASE | re.DOTALL)
            
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
    
    # Convert ARRAY[] to ARRAY_CONSTRUCT and wrap with array_to_string if needed
    modified_query1 = query1
    
    logging.info(f"=== Array wrapping check: contains_aggregation={contains_aggregation}, array_column_expr={array_column_expr}")
    
    if contains_aggregation and array_column_expr:
        logging.info(f"WRAPPING: Array contains aggregation functions - wrapping with array_to_string")
        
        # Try ARRAY[ first (PostgreSQL syntax)
        pattern = r'ARRAY\s*\[((?:[^\[\]]|\[(?:[^\[\]]|\[[^\[\]]*\])*\])*)\]\s+as\s+' + re.escape(array_name)
        match = re.search(pattern, modified_query1, re.IGNORECASE | re.DOTALL)
        
        if match:
            logging.info("Found ARRAY[ pattern - replacing with array_to_string wrapper")
            array_content = match.group(1)
            replacement = f"array_to_string(ARRAY_CONSTRUCT({array_content}),';') as {array_name}"
            modified_query1 = modified_query1[:match.start()] + replacement + modified_query1[match.end():]
        else:
            # Try ARRAY_CONSTRUCT( (Snowflake syntax)
            pattern = r'ARRAY_CONSTRUCT\s*\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)\s+as\s+' + re.escape(array_name)
            match = re.search(pattern, modified_query1, re.IGNORECASE | re.DOTALL)
            
            if match:
                logging.info("Found ARRAY_CONSTRUCT pattern - replacing with array_to_string wrapper")
                array_content = match.group(1)
                replacement = f"array_to_string(ARRAY_CONSTRUCT({array_content}),';') as {array_name}"
                modified_query1 = modified_query1[:match.start()] + replacement + modified_query1[match.end():]
            else:
                logging.warning(f"NO MATCH: Could not find ARRAY[ or ARRAY_CONSTRUCT pattern with 'as {array_name}'")
    else:
        logging.info(f"NO WRAPPING: Converting ARRAY[] to ARRAY_CONSTRUCT() without wrapping")
        # No aggregation, just convert ARRAY[] to ARRAY_CONSTRUCT()
        modified_query1 = re.sub(r'ARRAY\s*\[', 'ARRAY_CONSTRUCT(', modified_query1, flags=re.IGNORECASE)
        modified_query1 = re.sub(r'\]\s+as\s+' + re.escape(array_name), f') as {array_name}', modified_query1, flags=re.IGNORECASE)
    
    result = f"""-- Prepare CTE from original query
{macro_variable_declaration}WITH prepare AS (
{modified_query1}
)
-- Call snowflake pivot macro
{{{{snowflake_pivot({pivoted_values_str},'{array_name}', '{pivot_key}', {aantal_split},{eventuele_extra_split_str}, {id_cols_str})}}}}

SELECT * FROM draaitabel_ct"""
    
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