from pathlib import Path
from typing import Any, Dict, List, Tuple
import re
import logging
from code.functions.dialect_converter import convert_postgres_to_snowflake
from code.functions.functions import *
import traceback


def preserve_dbt_macros(sql: str) -> Tuple[str, List[str]]:
    """Preserve DBT macro calls by replacing with placeholders before SQL transpilation.
    
    Returns:
        Tuple of (sql_with_placeholders, list_of_original_macros)
    """
    macro_pattern = r"(\{\{\s*[a-zA-Z_][a-zA-Z0-9_]*\s*\([^}]*\)\s*\}\})"
    macros = []
    
    def replacer(match):
        macro_call = match.group(1)
        macros.append(macro_call)
        placeholder = f"__DBT_MACRO_{len(macros)-1}__"
        logging.debug(f"Preserving macro: '{macro_call}' -> '{placeholder}'")
        return placeholder
    
    result = re.sub(macro_pattern, replacer, sql)
    logging.debug(f"Found {len(macros)} macro calls to preserve")
    return result, macros


def restore_dbt_macros(sql: str, macros: List[str]) -> str:
    """Restore DBT macro calls from placeholders after SQL transpilation."""
    for idx, macro in enumerate(macros):
        placeholder = f"__DBT_MACRO_{idx}__"
        
        # Check if placeholder is in a set statement context
        pattern_in_set = rf'\{{%\-\s*set\s+\w+\s*=\s*{re.escape(placeholder)}\s*\-%\}}'
        if re.search(pattern_in_set, sql):
            # Extract just the macro call without {{ }}
            macro_without_brackets = re.sub(r'^\{\{\s*|\s*\}\}$', '', macro)
            sql = sql.replace(placeholder, macro_without_brackets)
            logging.debug(f"Restored macro in set context: '{placeholder}' -> '{macro_without_brackets}'")
        else:
            # Normal restoration with {{ }}
            sql = sql.replace(placeholder, macro)
            logging.debug(f"Restored macro: '{placeholder}' -> '{macro}'")
    return sql


def convert_includes_to_macros(sql: str) -> str:
    """Convert {% include 'blockname.pry' %} to {{ blockname() }}."""
    return re.sub(r"{%-?\s*include\s+['\"]([\w\-]+)\.pry['\"]\s*%}", r"{{ \1() }}", sql)

def convert_pry_to_dbt(pry_path: Path, output_dir: Path, config, block_tables=None, seed_tables=None) -> set:
    """Convert PRY file to dbt models.
    
    Args:
        seed_tables: Dict mapping table names to their references (e.g., {'nhg_labcodes': 'dwh.{{agb}}.nhg_labcodes'})
    
    Returns:
        set: Table/view names created by this file (for blocks)
    """
    
    # Read PRY file
    content = pry_path.read_text(encoding='utf-8')

    # If PRY is in a blocks folder (case-insensitive, anywhere in path), process as block
    if any(p.name.lower() == 'blocks' for p in pry_path.parents):
        block_name = pry_path.stem
        
        # Check if this is a column list block (contains _ct)
        if '_ct' in block_name:
            logging.info(f"Processing column list block: {block_name}")
            
            # Parse column names from the content (format: column_name type,)
            column_names = []
            for line in content.split('\n'):
                line = line.strip()
                if line and not line.startswith('--') and not line.startswith('{'):
                    # Extract column name before the type (e.g., "af_b varchar[]," -> "af_b")
                    match = re.match(r'^\s*([a-zA-Z_][\w]*)\s+', line)
                    if match:
                        column_names.append(match.group(1))
            
            logging.debug(f"Extracted {len(column_names)} columns: {column_names[:5]}...")
            
            # Create macro that returns the list
            macro_path = Path(config.get('dbt_macro_path', 'macros'))
            macro_path.mkdir(parents=True, exist_ok=True)
            
            macro_file = macro_path / f"{block_name}.sql"
            # Format each column on its own line for readability
            column_lines = ",\n  ".join(f"'{col}'" for col in column_names)
            macro_content = f"""{{% macro {block_name}() %}}
{{%- set categories = [
  {column_lines}
] -%}}
{{{{ return(categories) }}}}
{{% endmacro %}}
"""
            
            with open(macro_file, 'w', encoding='utf-8') as f:
                f.write(macro_content)
            
            logging.info(f"[OK] Column list macro generated: {macro_file} ({len(column_names)} columns)")
            return set()
        
        # Regular block processing
        preprocessed = preprocess_sql(content)
        preprocessed = convert_includes_to_macros(preprocessed)
        
        # Preserve, convert, and restore macros
        temp_sql, macros = preserve_dbt_macros(preprocessed)
        
        function_macros = config.get('function_macros', [])
        converted_sql = convert_postgres_to_snowflake(temp_sql, function_macros=function_macros)
        
        if function_macros:
            converted_sql = replace_functions_with_macros(converted_sql, function_macros)
        
        converted_sql = restore_dbt_macros(converted_sql, macros)        
        # For blocks: only replace external tables, leave all others unchanged
        model_refs = config.get('model_refs', [])
        table_mapping = config.get('table_mapping', {})
        converted_sql = replace_table_references(converted_sql, seed_tables=seed_tables, model_refs=model_refs, table_mapping=table_mapping, is_block=True)
        
        # Extract all external variables like ${varname} in the SQL and replace them
        converted_sql, external_vars = replace_template_variables(converted_sql, config)
        
        # Extract all table/view names created by this block (look for "name AS (")
        created_tables = set()
        for match in re.finditer(r'\b(\w+)\s+AS\s*\(', converted_sql, re.IGNORECASE):
            table_name = match.group(1).lower()
            if table_name not in ['select', 'insert', 'update', 'delete', 'with', 'case']:
                created_tables.add(table_name)
        
        # All blocks become macros
        macro_path = Path(config.get('dbt_macro_path', 'macros'))
        macro_path.mkdir(parents=True, exist_ok=True)
        
        macro_file = macro_path / f"{block_name}.sql"
        
        # Build dbt variable section using {% set %}
        macro_header = f"{{% macro {block_name}() %}}\n"
        variables = []
        
        # Add agb variable if needed (legacy check)
        if '{{agb}}' in converted_sql or '{{ agb }}' in converted_sql:
            variables.append("{%- set agb = var('agb', 0) %}")
            logging.debug(f"Added agb variable to macro {block_name}")
        
        # Add each external variable as a dbt variable
        for var in sorted(external_vars):
            variables.append(f"{{%- set {var} = var(\"{var}\", \" \") %}}")
        
        # Add variables to macro header
        if variables:
            macro_header += '\n'.join(variables) + '\n'
        
        macro_content = f"{macro_header}{converted_sql}\n{{% endmacro %}}\n"
        
        # Ensure file is overwritten by explicitly using open with 'w' mode
        with open(macro_file, 'w', encoding='utf-8') as f:
            f.write(macro_content)
        
        if created_tables:
            logging.info(f"[OK] Block macro generated: {macro_file} (creates: {', '.join(created_tables)})")
        else:
            logging.info(f"[OK] Block macro generated: {macro_file}")
        
        return created_tables
    else:
        # Create DBT models for each query
        metadata = parse_pry_file(content)
        report_name = metadata.get('name', 'Unknown Report')
        report_name  = re.sub(r'[^a-zA-Z0-9_]+', '_', report_name)
        report_type = metadata.get('reporttype', 'normal')
        reportviews = metadata.get('reportviews', [])
        queries = metadata.get('parsed_queries', [])

        # Normal dbt model flow
        folder_name = sanitize_folder_name(report_name)
        full_output_dir = output_dir / folder_name
        full_output_dir.mkdir(parents=True, exist_ok=True)
        for i, query in enumerate(queries):
            query = convert_includes_to_macros(query)
            view_name = extract_view_name_from_query(query)
            if not view_name:
                logging.warning(f"Could not extract view name from query {i+1}")
                continue
            view_metadata = next((rv for rv in reportviews if rv.get('name') == view_name), {})
            if view_metadata.get('external', False):
                logging.info(f"Skipping external view: {view_name}")
                continue
            generate_dbt_model(
                view_name=view_name,
                query=query,
                report_name=report_name,
                report_type=report_type,
                view_metadata=view_metadata,
                output_dir=full_output_dir,
                block_tables=block_tables,
                seed_tables=seed_tables,
                config=config
            )
        return set()

def replace_template_variables(sql: str, config: dict = None) -> tuple[str, set[str]]:
    """Replace ${varname} template variables with {{ varname }} and return external vars.
    
    Args:
        config: Configuration dict containing default_zg_agb for zg_agb variables
    
    Returns:
        tuple: (modified_sql, set_of_external_variables)
    """
    config = config or {}
    default_zg_agb = config.get('default_zg_agb', '')
    
    # Extract all external variables like ${varname} in the SQL
    external_vars = set(re.findall(r'\$\{([a-zA-Z_][\w]*)\}', sql))
    
    # Replace quoted '${varname}' or "${varname}" - always remove quotes
    def replace_quoted_var(m):
        var_name = m.group(2)
        default_value = default_zg_agb if 'zg_agb' in var_name.lower() else ""
        return f'\'{{{{var("{var_name}","{default_value}")}}}}\''
    sql = re.sub(r"(['\"])\$\{([a-zA-Z_][\w]*)\}\1", replace_quoted_var, sql)
    
    # Then replace any remaining unquoted ${varname} - always wrap in single quotes
    def replace_unquoted_var(m):
        var_name = m.group(1)
        default_value = default_zg_agb if 'zg_agb' in var_name.lower() else ""
        return f'\'{{{{var("{var_name}","{default_value}")}}}}\''
    sql = re.sub(r'\$\{([a-zA-Z_][\w]*)\}', replace_unquoted_var, sql)
    
    return sql, external_vars

def replace_functions_with_macros(sql: str, function_names: List[str]) -> str:
    """Replace SQL function calls with dbt macro calls.
    
    Example: CLEAN_ICPC(jr.icpc) -> {{ clean_icpc('jr.icpc') }}
    Example: indelingen.translate_labcode_answers(col::int, val) -> {{ translate_labcode_answers('col::int', 'val') }}
    """
    logging.info(f"Replacing functions with macros")
    def smart_split_args(argstr):
        args = []
        current = ''
        depth = 0
        in_string = False
        string_char = None
        i = 0
        while i < len(argstr):
            char = argstr[i]
            if char in ('"', "'"):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
                current += char
            elif char == '(' and not in_string:
                depth += 1
                current += char
            elif char == ')' and not in_string:
                depth -= 1
                current += char
            elif char == ',' and not in_string and depth == 0:
                args.append(current.strip())
                current = ''
            else:
                current += char
            i += 1
        if current.strip():
            args.append(current.strip())
        return args

    def find_function_calls(s, func_name):
        # Returns list of (start, end, args_str) for each function call
        results = []
        pattern = re.compile(rf'(?:(?:indelingen|public)\.)?{re.escape(func_name)}\s*\(', re.IGNORECASE)
        for m in pattern.finditer(s):
            start = m.start()
            i = m.end()
            depth = 1
            in_string = False
            string_char = None
            while i < len(s):
                c = s[i]
                if c in ('"', "'"):
                    if not in_string:
                        in_string = True
                        string_char = c
                    elif c == string_char:
                        in_string = False
                        string_char = None
                elif c == '(' and not in_string:
                    depth += 1
                elif c == ')' and not in_string:
                    depth -= 1
                    if depth == 0:
                        # Found matching close
                        args_str = s[m.end():i]
                        results.append((start, i+1, args_str))
                        break
                i += 1
        return results

    for func_name in function_names:
        # Find all function calls with correct parenthesis matching
        calls = find_function_calls(sql, func_name)
        # Replace from the end to avoid messing up indices
        for start, end, args in reversed(calls):
            macro_name = func_name.lower()
            if not args.strip():
                replacement = f"{{{{ {macro_name}() }}}}"
            else:
                arg_list = smart_split_args(args)
                quoted_args = []
                for arg in arg_list:
                    arg_strip = arg.strip()
                    # Replace ${varname} with {{ varname }}, preserving surrounding quotes if they exist
                    # First handle quoted variables: '${var}' or "${var}"
                    def replace_quoted_var(m):
                        quote = m.group(1)
                        var_name = m.group(2)
                        return f"{quote}{{{{ {var_name} }}}}{quote}"
                    arg_strip = re.sub(r"(['\"])\$\{([a-zA-Z_][\w]*)\}\1", replace_quoted_var, arg_strip)
                    # Then handle unquoted variables: ${var}
                    arg_strip = re.sub(r"\$\{([a-zA-Z_][\w]*)\}", lambda m: f"{{{{ {m.group(1)} }}}}", arg_strip)
                    # Escape single quotes with backslash (for Jinja string literals)
                    arg_strip = arg_strip.replace("'", "\\'")
                    # Quote the argument (single quotes) - Jinja will still evaluate {{ }} inside quotes
                    quoted_args.append(f"'{arg_strip}'")
                replacement = f"{{{{ {macro_name}({', '.join(quoted_args)}) }}}}"
            sql = sql[:start] + replacement + sql[end:]
    return sql


def generate_dbt_model(
    view_name: str,
    query: str,
    report_name: str,
    report_type: str,
    view_metadata: Dict[str, Any],
    output_dir: Path,
    block_tables=None,
    seed_tables=None,
    config=None
) -> None:
    """Generate a single dbt model file."
    
    Args:
        block_tables: Set of table names created by block files
        seed_tables: Dict mapping table names to their references from config
    """
    
    try:
        logging.debug(f"\n{'='*80}\nProcessing: {report_name} - {view_name}\n{'='*80}")
        
        # Preprocess, preserve macros, convert SQL, restore macros
        preprocessed = preprocess_sql(query)
        temp_sql, macros = preserve_dbt_macros(preprocessed)
        
        function_macros = config.get('function_macros', [])
        converted_sql = convert_postgres_to_snowflake(temp_sql, function_macros=function_macros)
        
        if function_macros:
            converted_sql = replace_functions_with_macros(converted_sql, function_macros)
        
        converted_sql = restore_dbt_macros(converted_sql, macros)
        
        # Convert SQL comments to Jinja comments
        converted_sql = re.sub(r'/\*', r'{#', converted_sql)
        converted_sql = re.sub(r'\*/', r'#}', converted_sql)
        converted_sql = re.sub(r'^(\s*)--(.*)$', r'\1{# \2 #}', converted_sql, flags=re.MULTILINE)
        
        # Check if actually converted
        if converted_sql == preprocessed:
            print("[WARNING] SQL was not modified during conversion")
                
        # Remove CREATE [MATERIALIZED] VIEW statement, keep only the SELECT/WITH
        converted_sql = re.sub(
            r'CREATE\s+(MATERIALIZED\s+)?VIEW\s+\w+\s+AS\s+',
            '',
            converted_sql,
            count=1,
            flags=re.IGNORECASE
        )
                
        # Replace table references with dbt macros
        model_refs = config.get('model_refs', [])
        table_mapping = config.get('table_mapping', {})
        converted_sql = replace_table_references(converted_sql, block_tables=block_tables, seed_tables=seed_tables, model_refs=model_refs, table_mapping=table_mapping)
        
        # Ensure it starts with WITH or SELECT and remove trailing semicolon
        converted_sql = converted_sql.strip()
        if converted_sql.endswith(';'):
            converted_sql = converted_sql[:-1].strip()
            
        if not re.match(r'^(WITH|SELECT)', converted_sql, re.IGNORECASE):
            print(f"[WARNING] Query doesn't start with WITH or SELECT after removing CREATE VIEW")
        # Build dbt variable section using {% set %}
        variables = [
            "{%- set report_name = '" + report_name + "' %}",
            "{%- set report_type = '" + report_type + "' %}",
            "{%- set view_name = '" + view_name + "' %}",
        ]
        # Extract all external variables like ${varname} in the SQL and replace them
        converted_sql, external_vars = replace_template_variables(converted_sql, config)
        # Exclude agb (already set)
        external_vars.discard('agb')
        # Build dbt config block
        # Check if report should be materialized as table
        materialized_view_reports = config.get('materialized_view_reports', [])
        materialization = 'table' #if report_name.lower() in materialized_view_reports else 'view'
        
        # Build vars part
        vars_part = ', '.join(f"'{var}': \"\"" for var in sorted(external_vars))
        
        # Build tags list, only add view_metadata['type'] if it exists
        tags = [f"'{report_type}'"]
        if view_metadata.get('type'):
            tags.append(f"'{view_metadata['type']}'")
        tags.append("'report'")
        tags.append(f"'{report_name.replace(' ', '_').lower()}'")
        tags_str = ', '.join(tags)
        config_lines = [
            "{{",
            "  config(",
            f"    materialized='{materialization}',",
            f"    tags=[{tags_str}],",
            f"    vars={{ 'agb': none, {vars_part} }}"
        ]
        # Add schema if needed
        if view_metadata.get('type') == 'supportview':
            config_lines.append(f"    ,alias='{view_name}'")
        config_lines.extend([
            "  )",
            "}}"
        ])
        # Combine everything
        model_content = '\n'.join(variables) + '\n\n'
        model_content += '\n'.join(config_lines) + '\n\n'
        model_content += converted_sql
        
        # Write to file - ensure parent directory exists and file is overwritten
        output_file = output_dir / f"{view_name}.sql"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(model_content)
        
        logging.info(f"[OK] Generated: {output_file}")
    except Exception as e:
        logging.error(f"Error processing {view_name}: {e}")
        traceback.print_exc()


def replace_table_references(sql: str, external_tables=None, block_tables=None, seed_tables=None, model_refs=None, table_mapping=None, is_block=False) -> str:
    """Replace table references in FROM/JOIN with dbt ref() or STG.P{{agb}}.table.
    
    Replacement priority (highest to lowest):
    1. Schema-qualified tables: indelingen.X -> DWH.REFERENCE_DATA.X
    2. Table mappings from config (schema.table -> custom target)
    3. Seed tables from config (qualified schema.table -> seed reference)
    4. CTEs (no replacement)
    5. Special tables like 'TABLE', 'draaitabel_ct' (no replacement)
    6. Model refs from config -> {{ ref('table_name') }}
    7. External tables -> STG.P{{agb}}.table
    8. Block tables (no replacement)
    9. Everything else -> {{ ref('table_name') }} (unless is_block=True)
    
    Args:
        is_block: If True, only replace external/seed/mapped tables. All others unchanged.
        model_refs: List of table names that should be replaced with {{ ref('table_name') }}
        table_mapping: Dict mapping specific schema.table patterns to target references
    """
    # Initialize defaults
    block_tables = block_tables or set()
    model_refs = model_refs or []
    model_refs_lower = [m.lower() for m in model_refs]
    table_mapping = table_mapping or {}
    table_mapping_lower = {k.lower(): v for k, v in table_mapping.items()}
    external_tables = external_tables or [
        'allergie', 'bepaling', 'contact', 'contraindicatie', 'episode', 'journaal',
        'journaalregel', 'medewerker', 'medicatie', 'metadata', 'origineel',
        'patient', 'praktijk', 'ruiter', 'verrichting', 'verwijzing', 
        'override_patientenlijst', 'functie', 'medewerker_hisnaam', 'medewerker_manual', 
    ]
    external_tables_lower = [t.lower() for t in external_tables]
    seed_table_lookup = {k.lower(): v for k, v in (seed_tables or {}).items()}
    
    # Extract CTE names from SQL
    cte_names = _extract_cte_names(sql)
    logging.debug(f"Detected CTEs: {cte_names}, Block tables: {block_tables}")
    
    # Step 1: Replace schema-qualified references (indelingen, seed tables, table mappings)
    sql = _replace_qualified_tables(sql, seed_table_lookup, table_mapping_lower)
    
    # Step 2: Replace unqualified table references
    sql = _replace_unqualified_tables(
        sql, cte_names, model_refs_lower, external_tables_lower, 
        block_tables, is_block
    )
    
    return sql


def _extract_cte_names(sql: str) -> set:
    """Extract CTE names from SQL query.
    
    Returns:
        Set of lowercase CTE names
    """
    # Remove comments to avoid false matches
    sql_clean = re.sub(r'(--[^\n]*|/\*.*?\*/|\{#.*?#\})', '', sql, flags=re.DOTALL)
    cte_names = set()
    
    # Match pattern: WITH name AS ( or , name AS (
    for match in re.finditer(r'\b(\w+)\s*(?:\([^)]+\))?\s+AS\s*\(', sql_clean, re.IGNORECASE):
        cte = match.group(1).lower()
        
        # Verify it's preceded by WITH or comma
        preceding = sql_clean[max(0, match.start()-100):match.start()]
        if re.search(r'(WITH(?:\s+RECURSIVE)?|,)\s*$', preceding, re.IGNORECASE | re.DOTALL):
            # Exclude SQL keywords that might match the pattern
            if cte not in ['select', 'insert', 'update', 'delete', 'with', 'case']:
                cte_names.add(cte)
    
    return cte_names


def _replace_qualified_tables(sql: str, seed_table_lookup: dict, table_mapping_lower: dict) -> str:
    """Replace schema-qualified table references.
    
    Handles:
    - indelingen.table -> DWH.REFERENCE_DATA.table
    - schema.table patterns from table_mapping
    - schema.table patterns from seed_tables
    """
    # Pattern for schema.table references
    schema_pattern = r'\b(FROM|JOIN(?!\s+LATERAL)|LEFT\s+JOIN(?!\s+LATERAL)|RIGHT\s+JOIN(?!\s+LATERAL)|INNER\s+JOIN(?!\s+LATERAL)|OUTER\s+JOIN(?!\s+LATERAL)|FULL\s+JOIN(?!\s+LATERAL)|CROSS\s+JOIN(?!\s+LATERAL))\s+([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b(?!\s*\(|::)'
    
    def schema_replacer(match):
        keyword, schema, table = match.group(1), match.group(2), match.group(3)
        schema_lower = schema.lower()
        
        # Priority 1: indelingen schema
        if schema_lower == 'indelingen':
            logging.debug(f"Replacing indelingen.{table} -> DWH.REFERENCE_DATA.{table}")
            return f"{keyword} DWH.REFERENCE_DATA.{table}"
        
        # Priority 2: Table mapping (e.g., dwh.public.medewerker -> ODS.{{agb}}.medewerker_manual)
        qualified_table = f"{schema}.{table}".lower()
        if qualified_table in table_mapping_lower:
            target = table_mapping_lower[qualified_table]
            logging.debug(f"Table mapping: {schema}.{table} -> {target}")
            return f"{keyword} {target}"
        
        # Priority 3: Seed tables
        if table.lower() in seed_table_lookup:
            logging.debug(f"Replacing seed: {schema}.{table} -> {seed_table_lookup[table.lower()]}")
            return f"{keyword} {seed_table_lookup[table.lower()]}"
        
        return match.group(0)
    
    return re.sub(schema_pattern, schema_replacer, sql, flags=re.IGNORECASE)


def _replace_unqualified_tables(sql: str, cte_names: set, model_refs_lower: list, 
                                 external_tables_lower: list, block_tables: set, 
                                 is_block: bool) -> str:
    """Replace unqualified table references in FROM/JOIN clauses.
    
    Args:
        cte_names: Set of CTE names (lowercase) to skip
        model_refs_lower: List of model names (lowercase) to replace with ref()
        external_tables_lower: List of external table names (lowercase)
        block_tables: Set of block table names to skip
        is_block: If True, only replace external tables
    """
    # Pattern matches FROM/JOIN followed by an unqualified table name
    # Excludes: table.x, table(, table::
    table_pattern = r'\b(FROM|JOIN(?!\s+LATERAL)|LEFT\s+JOIN(?!\s+LATERAL)|RIGHT\s+JOIN(?!\s+LATERAL)|INNER\s+JOIN(?!\s+LATERAL)|OUTER\s+JOIN(?!\s+LATERAL)|FULL\s+JOIN(?!\s+LATERAL)|CROSS\s+JOIN(?!\s+LATERAL))\s+([a-zA-Z_][\w]*)\b(?!\s*\.|\s*\(|::)'
    
    def table_replacer(match):
        keyword, table = match.group(1), match.group(2)
        table_lower = table.lower()
        
        # Check if we're inside a function call (e.g., EXTRACT(year FROM date_col))
        if _is_inside_function_call(sql, match.start()):
            logging.debug(f"Skipping {table} at {match.start()} because it is inside a function call.")
            return match.group(0)
        
        # Priority 1: Skip CTEs
        if table_lower in cte_names:
            logging.debug(f"Skipping {table} at {match.start()} because it is a CTE.")
            return match.group(0)
        
        # Priority 2: Skip special keywords/tables
        if table_lower in ['table', 'draaitabel_ct']:
            logging.debug(f"Skipping {table} at {match.start()} because it is a special keyword/table.")
            return match.group(0)
        
        # Priority 3: Model refs -> {{ ref('table_name') }}
        if table_lower in model_refs_lower:
            logging.debug(f"Model ref: {table} at {match.start()} -> ref('{table_lower}')")
            return f"{keyword} {{{{ ref('{table_lower}') }}}}"
        
        # Priority 4: External tables -> {{ source('STG', 'table') }}
        if table_lower in external_tables_lower:
            logging.debug(f"External: {table} at {match.start()} -> source('STG', '{table}')")
            return f"{keyword} {{{{ source('DWH', '{table}') }}}}"
        
        # Priority 5: Block mode - leave all non-external tables unchanged
        if is_block:
            logging.debug(f"Block mode: leaving {table} at {match.start()} unchanged.")
            return match.group(0)
        
        # Priority 6: Skip block-created tables
        if table_lower in block_tables:
            logging.debug(f"Skipping {table} at {match.start()} because it is a block-created table.")
            return match.group(0)
        
        # Priority 7: Everything else -> {{ ref('table_name') }}
        logging.debug(f"Internal table: {table} at {match.start()} -> ref('{table}') (fallback case)")
        return f"{keyword} {{{{ ref('{table}') }}}}"
    
    return re.sub(table_pattern, table_replacer, sql, flags=re.IGNORECASE | re.DOTALL)


def _is_inside_function_call(sql: str, match_pos: int) -> bool:
    """Check if a FROM/JOIN match is inside a function call (e.g., EXTRACT(year FROM date)).
    
    Args:
        sql: Full SQL string
        match_pos: Position of the FROM/JOIN match
        
    Returns:
        True if inside a function call, False otherwise
    """
    # Look at preceding context (300 chars should be enough to find context)
    start_pos = max(0, match_pos - 300)
    preceding_text = sql[start_pos:match_pos]
    
    # Count unmatched opening parentheses
    paren_depth = preceding_text.count('(') - preceding_text.count(')')
    
    # If paren_depth <= 0, we're not inside any parentheses
    if paren_depth <= 0:
        return False
    
    # Find the matching unmatched opening paren by walking backwards
    depth = 0
    matching_paren_pos = -1
    
    for i in range(len(preceding_text) - 1, -1, -1):
        char = preceding_text[i]
        if char == ')':
            depth += 1
        elif char == '(':
            if depth == 0:
                # Found the unmatched opening paren
                matching_paren_pos = i
                break
            else:
                depth -= 1
    
    if matching_paren_pos < 0:
        # Couldn't find matching paren, be conservative
        return False
    
    # Get text between the matching opening paren and our match
    text_after_paren = preceding_text[matching_paren_pos:]
    
    # If there's a SELECT/WITH after the paren, it's a subquery/CTE - allow it
    # If there's no SELECT/WITH, it's likely a function call - skip it
    has_select = re.search(r'\b(SELECT|WITH)\b', text_after_paren, re.IGNORECASE)
    
    # Return True (is inside function) if there's NO SELECT/WITH
    return not bool(has_select)