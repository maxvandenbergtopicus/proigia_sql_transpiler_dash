import re
import logging
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class CrosstabElements:
    """Structured representation of crosstab elements needed for the macro"""
    # Input elements from PostgreSQL crosstab
    cte_query: str                      # First $$ block - the source query
    pivot_query: str                    # Second $$ block - defines what to pivot
    output_columns: List[str]           # Columns in the final output ) as (cols...)
    
    # Parsed elements for snowflake_pivot_test macro
    list_with_columns: List[str]        # The distinct values to pivot into columns (or DYNAMIC marker)
    array_name: str                     # The column containing array data
    pivot_key: str                      # The column to pivot on (category column)
    number_of_splits: int               # Number of splits for array splitting
    id_columns: List[str]               # ID columns to keep (patient_id, etc.)
    
    # Optional fields (must come after required fields)
    list_with_columns_dynamic: Optional[str] = None  # Dynamic expression if needed
    optional_extra_array_position: Optional[int] = None  # Position of extra array (e.g., ICPC)
    with_statement: Optional[str] = None  # Any WITH clause before the crosstab
    from_tables: List[str] = None         # Tables referenced in the CTE
    
    def __post_init__(self):
        if self.from_tables is None:
            self.from_tables = []


def extract_crosstab_blocks(sql_content: str) -> dict:
    """
    Step 1: Extract the fundamental building blocks from crosstab SQL.
    
    PostgreSQL crosstab structure:
        [WITH ...] 
        SELECT * FROM crosstab(
            $$ <cte_query> $$,
            $$ <pivot_query> $$
        ) AS result (<output_columns>);
    
    Returns dict with:
        - cte_query: First $$ block
        - pivot_query: Second $$ block  
        - output_columns: Column list from ) AS result (...)
        - with_statement: Optional WITH clause before SELECT
    """
    logging.debug("="*80)
    logging.debug("STEP 1: Extracting Crosstab Blocks")
    logging.debug("-"*80)
    
    result = {
        'cte_query': None,
        'pivot_query': None,
        'output_columns': [],
        'with_statement': None
    }
    
    # Extract all $$ blocks
    dollar_blocks = re.findall(r'\$\$(.*?)\$\$', sql_content, re.DOTALL | re.IGNORECASE)
    
    if len(dollar_blocks) < 2:
        logging.error(f"Expected 2 $$ blocks, found {len(dollar_blocks)}")
        return result
    
    result['cte_query'] = dollar_blocks[0].strip()
    result['pivot_query'] = dollar_blocks[1].strip()
    
    logging.debug(f"CTE Query (first $$):\n{result['cte_query'][:200]}...")
    logging.debug(f"Pivot Query (second $$):\n{result['pivot_query'][:200]}...")
    
    # Extract output columns from ) AS result (col1, col2, ...)
    output_pattern = r'\)\s*as\s+(?:\w+\s*)?\(\s*(.+?)(?:\)|$)'
    output_match = re.search(output_pattern, sql_content, re.IGNORECASE | re.DOTALL)
    
    if output_match:
        output_str = output_match.group(1).strip().replace('(', '').replace(';', '').strip()
        # Split by comma, handling type declarations
        for col in output_str.split(','):
            col_name = col.strip().split()[0]  # Take first word (column name)
            result['output_columns'].append(col_name)
        
        logging.debug(f"Output columns: {result['output_columns']}")
    else:
        logging.error("Could not extract output columns from ) AS (...)")
    
    # Check for WITH statement before crosstab
    with_pattern = r'(WITH\s+.*?)\s*(?=SELECT.*FROM\s+crosstab)'
    with_match = re.search(with_pattern, sql_content, re.IGNORECASE | re.DOTALL)
    
    if with_match:
        result['with_statement'] = with_match.group(1).strip()
        logging.debug(f"WITH statement found: {result['with_statement'][:100]}...")
    
    logging.debug("="*80)
    return result


def analyze_cte_query(cte_query: str) -> dict:
    """
    Step 2: Analyze the CTE query (first $$ block) to extract:
    - SELECT columns and their aliases
    - FROM tables
    - Any WITH clause within the CTE
    
    Returns dict with:
        - select_columns: List of (expression, alias) tuples
        - from_tables: List of table names
        - inner_with: Optional WITH clause inside the CTE
    """
    logging.debug("="*80)
    logging.debug("STEP 2: Analyzing CTE Query")
    logging.debug("-"*80)
    
    result = {
        'select_columns': [],
        'from_tables': [],
        'inner_with': None
    }
    
    # Check for WITH inside the CTE
    inner_with_pattern = r'(WITH\s+.*?\))\s*(?=SELECT)'
    inner_with_match = re.search(inner_with_pattern, cte_query, re.IGNORECASE | re.DOTALL)
    
    if inner_with_match:
        result['inner_with'] = inner_with_match.group(1).strip()
        # Remove WITH block to analyze just the SELECT
        cte_query_clean = cte_query[inner_with_match.end():].strip()
        logging.debug(f"Inner WITH found: {result['inner_with'][:100]}...")
    else:
        cte_query_clean = cte_query
    
    # Extract SELECT columns (between SELECT and FROM)
    select_pattern = r'SELECT\s+(.*?)\s+FROM'
    select_match = re.search(select_pattern, cte_query_clean, re.IGNORECASE | re.DOTALL)
    
    if select_match:
        select_clause = select_match.group(1).strip()
        
        # Parse columns respecting parentheses and brackets
        columns = []
        current_col = ''
        depth = 0
        
        for char in select_clause:
            if char in '([':
                depth += 1
            elif char in ')]':
                depth -= 1
            elif char == ',' and depth == 0:
                if current_col.strip():
                    columns.append(current_col.strip())
                current_col = ''
                continue
            current_col += char
        
        if current_col.strip():
            columns.append(current_col.strip())
        
        # Extract column name/alias from each expression
        for col_expr in columns:
            # Check if there's an AS alias
            as_match = re.search(r'\s+as\s+(\w+)', col_expr, re.IGNORECASE)
            if as_match:
                alias = as_match.group(1)
            else:
                # No alias, use the base column name
                alias = col_expr.strip().split()[0].split('.')[-1]
            
            result['select_columns'].append((col_expr, alias))
        
        logging.debug(f"Select columns found: {[alias for _, alias in result['select_columns']]}")
    
    # Extract FROM tables
    from_pattern = r'\b(FROM|JOIN)\s+([a-zA-Z_][\w]*)\b(?!\s*\.|\s*\()'
    from_matches = re.findall(from_pattern, cte_query_clean, re.IGNORECASE)
    
    for _, table in from_matches:
        if table.upper() not in ['SELECT', 'TABLE', 'LATERAL']:
            result['from_tables'].append(table)
    
    logging.debug(f"FROM tables: {result['from_tables']}")
    logging.debug("="*80)
    
    return result


def analyze_pivot_query(pivot_query: str) -> dict:
    """
    Step 3: Analyze the pivot query (second $$ block) to extract:
    - The pivot key column (what becomes the column names)
    - ORDER BY clause (defines the order of pivoted columns)
    
    Returns dict with:
        - pivot_key_column: Column name from SELECT
        - order_by: ORDER BY clause
    """
    logging.debug("="*80)
    logging.debug("STEP 3: Analyzing Pivot Query")
    logging.debug("-"*80)
    
    result = {
        'pivot_key_column': None,
        'order_by': None
    }
    
    # Extract SELECT column
    select_pattern = r'SELECT\s+(.*?)\s+FROM'
    select_match = re.search(select_pattern, pivot_query, re.IGNORECASE | re.DOTALL)
    
    if select_match:
        result['pivot_key_column'] = select_match.group(1).strip()
        logging.debug(f"Pivot key column: {result['pivot_key_column']}")
    
    # Extract ORDER BY
    order_pattern = r'ORDER\s+BY\s+(.+?)(?:;|$)'
    order_match = re.search(order_pattern, pivot_query, re.IGNORECASE | re.DOTALL)
    
    if order_match:
        result['order_by'] = order_match.group(1).strip()
        logging.debug(f"ORDER BY: {result['order_by']}")
    
    logging.debug("="*80)
    
    return result


def identify_macro_parameters(blocks: dict, cte_analysis: dict, pivot_analysis: dict) -> dict:
    """
    Step 4: Map extracted elements to snowflake_pivot_test macro parameters.
    
    Macro signature:
        snowflake_pivot_test(
            list_with_columns,      # Distinct pivot values
            array_name,             # Column with array data
            pivot_key,              # Column to pivot on
            number_of_splits,       # Array split count
            optional_extra_array_position,  # Extra array position
            id_columns              # ID columns to keep
        )
    
    Logic:
        - id_columns = output_columns ∩ cte_select_columns
        - pivot columns = cte_select_columns - output_columns
        - pivot_key = one of the pivot columns (category)
        - array_name = the other pivot column (values)
    """
    logging.debug("="*80)
    logging.debug("STEP 4: Identifying Macro Parameters")
    logging.debug("-"*80)
    
    result = {
        'list_with_columns': [],
        'array_name': None,
        'pivot_key': None,
        'number_of_splits': 0,
        'optional_extra_array_position': None,
        'id_columns': [],
        'pivot_from_table': None
    }
    
    # Get column names from CTE analysis
    cte_columns = [alias for _, alias in cte_analysis['select_columns']]
    output_columns = blocks['output_columns']
    
    # ID columns = intersection of CTE and output columns
    result['id_columns'] = [col for col in cte_columns if col in output_columns]
    logging.debug(f"ID columns (kept in output): {result['id_columns']}")
    
    # Pivot columns = CTE columns not in output
    pivot_columns = [col for col in cte_columns if col not in output_columns]
    logging.debug(f"Pivot columns (will become dynamic): {pivot_columns}")
    
    if len(pivot_columns) >= 2:
        # First pivot column is typically the category/key
        result['pivot_key'] = pivot_columns[0]
        # Second is typically the values/array
        result['array_name'] = pivot_columns[1]
        
        logging.debug(f"pivot_key: {result['pivot_key']}")
        logging.debug(f"array_name: {result['array_name']}")
    else:
        logging.warning(f"Expected at least 2 pivot columns, found {len(pivot_columns)}")
    
    # Extract FROM table from pivot query (second $$ block)
    # This is where the distinct pivot values come from
    pivot_from_pattern = r'\bFROM\s+([a-zA-Z_][\w]*)\b'
    pivot_from_match = re.search(pivot_from_pattern, blocks['pivot_query'], re.IGNORECASE)
    if pivot_from_match:
        result['pivot_from_table'] = pivot_from_match.group(1)
        logging.debug(f"Pivot FROM table: {result['pivot_from_table']}")
    
    # list_with_columns needs to be determined from the data
    # Use the pivot query's table for getting distinct values
    result['list_with_columns'] = ['DYNAMIC']  # Marker for dynamic columns
    
    if result['pivot_from_table']:
        result['list_with_columns_dynamic'] = (
            f"dbt_utils.get_column_values("
            f"ref('{result['pivot_from_table']}'), "
            f"'{result['pivot_key']}')"
        )
    else:
        # Fallback to CTE table if pivot query table not found
        fallback_table = cte_analysis['from_tables'][0] if cte_analysis['from_tables'] else 'source_table'
        result['list_with_columns_dynamic'] = (
            f"dbt_utils.get_column_values("
            f"ref('{fallback_table}'), "
            f"'{result['pivot_key']}')"
        )
    
    logging.debug(f"Dynamic column expression: {result['list_with_columns_dynamic']}")
    
    # TODO: Analyze the array structure to determine number_of_splits
    # This might require looking at the CTE select expressions
    result['number_of_splits'] = 4  # Default placeholder
    
    # TODO: Detect if there's an optional extra array position
    # This might be indicated by nested SPLIT or ARRAY operations
    result['optional_extra_array_position'] = None
    
    logging.debug("="*80)
    
    return result


def generate_macro_call(elements: CrosstabElements) -> str:
    """
    Step 5: Generate the actual dbt macro call with proper parameters.
    
    Returns the generated SQL with macro call.
    """
    logging.debug("="*80)
    logging.debug("STEP 5: Generating Macro Call")
    logging.debug("-"*80)
    
    # Build the macro call
    id_cols_str = "[" + ", ".join(f"'{col}'" for col in elements.id_columns) + "]"
    
    # Handle list_with_columns - check if it's dynamic or static
    if elements.list_with_columns_dynamic:
        cols_str = elements.list_with_columns_dynamic
        logging.debug(f"Using dynamic column list: {cols_str}")
    else:
        cols_str = "[" + ", ".join(f"'{col}'" for col in elements.list_with_columns) + "]"
        logging.debug(f"Using static column list: {cols_str}")
    
    macro_call = f"""-- Prepare CTE from original query
WITH prepare AS (
{elements.cte_query}
)
-- Call snowflake pivot macro
{{{{ snowflake_pivot_test(
    {cols_str},
    '{elements.array_name}',
    '{elements.pivot_key}',
    {elements.number_of_splits},
    {elements.optional_extra_array_position if elements.optional_extra_array_position else 'none'},
    {id_cols_str}
) }}}}"""
    
    logging.debug("Generated macro call:")
    logging.debug(macro_call)
    logging.debug("="*80)
    
    return macro_call


def parse_crosstab_to_macro(sql_content: str) -> str:
    """
    Main orchestration function: Parse PostgreSQL crosstab and convert to dbt macro.
    
    Flow:
        1. Extract blocks ($$ sections, output columns)
        2. Analyze CTE query (first $$)
        3. Analyze pivot query (second $$)
        4. Identify macro parameters
        5. Generate macro call
    """
    try:
        # Step 1: Extract basic blocks
        blocks = extract_crosstab_blocks(sql_content)
        if not blocks['cte_query'] or not blocks['pivot_query']:
            logging.error("Failed to extract crosstab blocks")
            return ""
        
        # Step 2: Analyze CTE query
        cte_analysis = analyze_cte_query(blocks['cte_query'])
        
        # Step 3: Analyze pivot query
        pivot_analysis = analyze_pivot_query(blocks['pivot_query'])
        
        # Step 4: Map to macro parameters
        macro_params = identify_macro_parameters(blocks, cte_analysis, pivot_analysis)
        
        # Step 5: Create CrosstabElements dataclass
        elements = CrosstabElements(
            cte_query=blocks['cte_query'],
            pivot_query=blocks['pivot_query'],
            output_columns=blocks['output_columns'],
            list_with_columns=macro_params.get('list_with_columns', []),
            list_with_columns_dynamic=macro_params.get('list_with_columns_dynamic'),
            array_name=macro_params.get('array_name', ''),
            pivot_key=macro_params.get('pivot_key', ''),
            number_of_splits=macro_params.get('number_of_splits', 0),
            optional_extra_array_position=macro_params.get('optional_extra_array_position'),
            id_columns=macro_params.get('id_columns', []),
            with_statement=blocks['with_statement'],
            from_tables=cte_analysis.get('from_tables', [])
        )
        
        # Step 6: Generate macro call
        return generate_macro_call(elements)
        
    except Exception as e:
        logging.error(f"Error parsing crosstab: {e}")
        import traceback
        traceback.print_exc()
        return ""


# Keep backwards compatibility
def parse_crosstab_sql(sql_content: str) -> str:
    """Wrapper for backwards compatibility - calls new structured approach"""
    return parse_crosstab_to_macro(sql_content)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    # Set up logging for standalone mode
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s: %(message)s'
    )
    
    if len(sys.argv) < 2:
        print("Usage: python crosstabs_new.py <sql_file>")
        print("Example: python crosstabs_new.py crosstab1.sql")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    
    if not input_file.exists():
        print(f"Error: File not found: {input_file}")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"Processing: {input_file}")
    print(f"{'='*80}\n")
    
    # Read input SQL
    sql_content = input_file.read_text(encoding='utf-8')
    
    # Process crosstab
    result = parse_crosstab_to_macro(sql_content)
    
    if result:
        print(f"\n{'='*80}")
        print("CONVERTED SQL:")
        print(f"{'='*80}\n")
        print(result)
        print(f"\n{'='*80}\n")
        
        # Optionally save to output file
        output_file = input_file.with_suffix('.output.sql')
        output_file.write_text(result, encoding='utf-8')
        print(f"Output saved to: {output_file}")
    else:
        print("\n[ERROR] Conversion failed!")
        sys.exit(1)
