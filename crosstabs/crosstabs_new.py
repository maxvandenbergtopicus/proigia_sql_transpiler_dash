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
    output_match = re.search(r'\)\s*as\s+\w*\s*\(([^)]+)\)', sql, re.IGNORECASE | re.DOTALL)
    if not output_match:
        logging.error("Could not find output columns")
        return ""
    
    output_cols = [col.strip().split()[0] for col in output_match.group(1).split(',')]
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
    
    # Find the values column that contains the ARRAY
    for col_expr in columns:
        if col_expr.strip().upper().startswith('ARRAY['):
            # Extract array elements
            array_content_match = re.search(r'ARRAY\[(.*?)\]', col_expr, re.IGNORECASE | re.DOTALL)
            if array_content_match:
                array_content = array_content_match.group(1)
                
                # Count elements by splitting on commas outside of function calls
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
                logging.debug(f"Array has {aantal_split} elements")
                logging.debug(f"Array elements: {[elem[:30] + '...' if len(elem) > 30 else elem for elem in array_elements]}")
                
                # Check for nested arrays (array_agg, ARRAY constructs)
                # Position is 1-indexed as per macro requirements
                for idx, elem in enumerate(array_elements, start=1):
                    if re.search(r'\barray_agg\b', elem, re.IGNORECASE) or 'ARRAY[' in elem.upper():
                        eventuele_extra_split = idx
                        logging.debug(f"Found nested array at position {idx} (1-indexed): {elem[:50]}...")
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
    pivoted_values_str = "[" + ", ".join(f"'{c}'" for c in pivoted_values) + "]"
    eventuele_extra_split_str = str(eventuele_extra_split) if eventuele_extra_split is not None else 'none'
    
    result = f"""-- Prepare CTE from original query
WITH prepare AS (
{query1}
)
-- Call snowflake pivot macro
{{{{snowflake_pivot({pivoted_values_str},'{array_name}', '{pivot_key}', {aantal_split},{eventuele_extra_split_str}, {id_cols_str})}}}}"""
    
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