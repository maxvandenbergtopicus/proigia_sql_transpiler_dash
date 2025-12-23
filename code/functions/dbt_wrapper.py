from pathlib import Path
from typing import Any, Dict
import re
import logging
from code.functions.dialect_converter import convert_postgres_to_snowflake
from code.functions.functions import *
import traceback

def convert_pry_to_dbt(pry_path: Path, output_dir: Path, config, block_tables=None, seed_tables=None) -> set:
    """Convert PRY file to dbt models.
    
    Args:
        seed_tables: Dict mapping table names to their references (e.g., {'nhg_labcodes': 'dwh.{{praktijk_agb}}.nhg_labcodes'})
    
    Returns:
        set: Table/view names created by this file (for blocks)
    """
    
    # Read PRY file
    content = pry_path.read_text(encoding='utf-8')

    # If PRY is in a blocks folder (case-insensitive, anywhere in path), process as block
    if any(p.name.lower() == 'blocks' for p in pry_path.parents):
        block_name = pry_path.stem
        
        preprocessed = preprocess_sql(content)
        converted_sql = convert_postgres_to_snowflake(preprocessed)
        
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
        macro_content = f"{{% macro {block_name}() %}}\n{converted_sql}\n{{% endmacro %}}\n"
        
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
        report_type = metadata.get('reporttype', 'normal')
        reportviews = metadata.get('reportviews', [])
        queries = metadata.get('parsed_queries', [])

        # Normal dbt model flow
        folder_name = sanitize_folder_name(report_name)
        full_output_dir = output_dir / folder_name
        full_output_dir.mkdir(parents=True, exist_ok=True)
        for i, query in enumerate(queries):
            # Replace any {% include 'blockname.pry' %} with {{ blockname() }} even if it's part of a line
            query = re.sub(r"{%-?\s*include\s+['\"]([\w\-]+)\.pry['\"]\s*%}", r"{{ \1() }}", query)
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
                seed_tables=seed_tables
            )
        return set()

def generate_dbt_model(
    view_name: str,
    query: str,
    report_name: str,
    report_type: str,
    view_metadata: Dict[str, Any],
    output_dir: Path,
    block_tables=None,
    seed_tables=None
) -> None:
    """Generate a single dbt model file."
    
    Args:
        block_tables: Set of table names created by block files
        seed_tables: Dict mapping table names to their references from config
    """
    
    try:
        logging.debug(f"\n{'='*80}\nProcessing: {report_name} - {view_name}\n{'='*80}")
        # Preprocess SQL (handles comment conversion and includes)
        preprocessed = preprocess_sql(query)
        
        # Preserve dbt macro calls by replacing them with placeholders (to survive sqlglot)
        macro_pattern = r"(\{\{\s*[a-zA-Z_][a-zA-Z0-9_]*\s*\(\s*\)\s*\}\})"
        macros = []
        
        def macro_replacer(match):
            macro_call = match.group(1)
            macros.append(macro_call)
            placeholder = f"__DBT_MACRO_{len(macros)-1}__"
            logging.debug(f"Preserving macro: '{macro_call}' -> '{placeholder}'")
            return placeholder
        
        temp_sql = re.sub(macro_pattern, macro_replacer, preprocessed)
        logging.debug(f"Found {len(macros)} macro calls to preserve")
        
        # Convert SQL from PostgreSQL to Snowflake
        converted_sql = convert_postgres_to_snowflake(temp_sql)
        
        # Restore macro calls
        for idx, macro in enumerate(macros):
            placeholder = f"__DBT_MACRO_{idx}__"
            converted_sql = converted_sql.replace(placeholder, macro)
            logging.debug(f"Restored macro: '{placeholder}' -> '{macro}'")
        # Replace all SQL comments with Jinja comments (after SQL conversion)
        # Multi-line comments: /* ... */  ->  {# ... #}
        converted_sql = re.sub(r'/\*', r'{#', converted_sql)
        converted_sql = re.sub(r'\*/', r'#}', converted_sql)
        # Single-line comments: -- ...  ->  {# ... #}
        # Only match the first -- on each line to avoid nested comments
        converted_sql = re.sub(r'^(\s*)--(.*)$', r'\1{# \2 #}', converted_sql, flags=re.MULTILINE)
        # Check if actually converted
        if converted_sql == preprocessed:
            print("[WARNING] SQL was not modified during conversion")
        
        logging.debug(f"SQL before CREATE VIEW removal (first 500 chars): {converted_sql[:500]}")
        
        # Remove CREATE [MATERIALIZED] VIEW statement, keep only the SELECT/WITH
        converted_sql = re.sub(
            r'CREATE\s+(MATERIALIZED\s+)?VIEW\s+\w+\s+AS\s+',
            '',
            converted_sql,
            count=1,
            flags=re.IGNORECASE
        )
        
        logging.debug(f"SQL after CREATE VIEW removal (first 500 chars): {converted_sql[:500]}")
        
        # Replace table references with dbt macros
        converted_sql = replace_table_references(converted_sql, block_tables=block_tables, seed_tables=seed_tables)
        # Ensure it starts with WITH or SELECT
        converted_sql = converted_sql.strip()
        if not re.match(r'^(WITH|SELECT)', converted_sql, re.IGNORECASE):
            print(f"[WARNING] Query doesn't start with WITH or SELECT after removing CREATE VIEW")
            print(f"First 100 chars: {converted_sql[:100]}")
        # Build dbt variable section using {% set %}
        variables = [
            "{%- set report_name = '" + report_name + "' %}",
            "{%- set report_type = '" + report_type + "' %}",
            "{%- set view_name = '" + view_name + "' %}",
            "{%- set praktijk_agb = var(\"praktijk_agb\", none) %}",
        ]
        # Extract all external variables like ${varname} in the SQL
        external_vars = set(re.findall(r'\$\{([a-zA-Z_][\w]*)\}', converted_sql))
        # Exclude praktijk_agb (already set)
        external_vars.discard('praktijk_agb')
        # Add each as a dbt variable
        for var in sorted(external_vars):
            variables.append(f"{{%- set {var} = var(\"{var}\", none) %}}")
        # First replace quoted '${varname}' or "${varname}" - keep quotes for datum variables to prevent arithmetic interpretation
        def replace_quoted_var(m):
            var_name = m.group(2)
            if 'datum' in var_name.lower():
                return f"'{{{{ {var_name} }}}}'"  # Keep quotes for dates
            return f"{{{{ {var_name} }}}}"  # Remove quotes for others
        converted_sql = re.sub(r"(['\"])\$\{([a-zA-Z_][\w]*)\}\1", replace_quoted_var, converted_sql)
        # Then replace any remaining unquoted ${varname} - add quotes if it's a datum variable
        def replace_unquoted_var(m):
            var_name = m.group(1)
            if 'datum' in var_name.lower():
                return f"'{{{{ {var_name} }}}}'"  # Add quotes for dates
            return f"{{{{ {var_name} }}}}"
        converted_sql = re.sub(r'\$\{([a-zA-Z_][\w]*)\}', replace_unquoted_var, converted_sql)
        if 'type' in view_metadata:
            variables.append("{%- set view_type = '" + view_metadata['type'] + "' %}")
        if 'displayname' in view_metadata:
            variables.append("{%- set display_name = '" + view_metadata['displayname'] + "' %}")
        if 'displayorder' in view_metadata:
            variables.append("{%- set display_order = " + str(view_metadata['displayorder']) + " %}")
        if 'queryorder' in view_metadata:
            variables.append("{%- set query_order = " + str(view_metadata['queryorder']) + " %}")
        # Build dbt config block
        config_lines = [
            "{{",
            "  config(",
            f"    materialized='view',",
            f"    tags=['{report_type}', 'report', '{report_name.replace(' ', '_').lower()}']"
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


def replace_table_references(sql: str, external_tables=None, block_tables=None, seed_tables=None) -> str:
    """Replace table references in FROM/JOIN with dbt ref() or STG.P{{praktijk_agb}}.table."""
    # Set defaults
    block_tables = block_tables or set()
    external_tables = external_tables or [
        'allergie', 'bepaling', 'contact', 'contraindicatie', 'episode', 'journaal',
        'journaalregel', 'medewerker', 'medicatie', 'metadata', 'origineel',
        'patient', 'praktijk', 'ruiter', 'verrichting', 'verwijzing', 
        'override_patientenlijst', 'functie', 'medewerker_hisnaam'
    ]
    seed_table_lookup = {k.lower(): v for k, v in (seed_tables or {}).items()}
    
    # Extract CTE names from SQL (remove comments first to avoid false matches)
    sql_clean = re.sub(r'(--[^\n]*|/\*.*?\*/|\{#.*?#\})', '', sql, flags=re.DOTALL)
    cte_names = set()
    
    for match in re.finditer(r'\b(\w+)\s*(?:\([^)]+\))?\s+AS(?:\s+\w+)*\s*\(', sql_clean, re.IGNORECASE):
        cte = match.group(1).lower()
        # Check if preceded by WITH or comma (indicating CTE context)
        preceding = sql_clean[max(0, match.start()-100):match.start()]
        if re.search(r'(WITH(?:\s+RECURSIVE)?|,)\s*$', preceding, re.IGNORECASE | re.DOTALL):
            if cte not in ['select', 'insert', 'update', 'delete', 'with', 'case']:
                cte_names.add(cte)
    
    logging.debug(f"Detected CTEs: {cte_names}, Block tables: {block_tables}")
    
    # Replace schema-qualified seed tables first (schema.table -> seed reference)
    schema_pattern = r'\b(FROM|JOIN(?!\s+LATERAL)|LEFT\s+JOIN(?!\s+LATERAL)|RIGHT\s+JOIN(?!\s+LATERAL)|INNER\s+JOIN(?!\s+LATERAL)|OUTER\s+JOIN(?!\s+LATERAL)|FULL\s+JOIN(?!\s+LATERAL)|CROSS\s+JOIN(?!\s+LATERAL))\s+([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b(?!\s*\(|::)'
    
    def schema_replacer(match):
        keyword, schema, table = match.group(1), match.group(2), match.group(3)
        if table.lower() in seed_table_lookup:
            logging.debug(f"Replacing seed: {schema}.{table}")
            return f"{keyword} {seed_table_lookup[table.lower()]}"
        return match.group(0)
    
    sql = re.sub(schema_pattern, schema_replacer, sql, flags=re.IGNORECASE)
    
    # Replace unqualified table references (table -> ref() or STG prefix)
    table_pattern = r'\b(FROM|JOIN(?!\s+LATERAL)|LEFT\s+JOIN(?!\s+LATERAL)|RIGHT\s+JOIN(?!\s+LATERAL)|INNER\s+JOIN(?!\s+LATERAL)|OUTER\s+JOIN(?!\s+LATERAL)|FULL\s+JOIN(?!\s+LATERAL)|CROSS\s+JOIN(?!\s+LATERAL))\s+([a-zA-Z_][\w]*)\b(?!\s*\.|\s*\(|::)'
    
    def table_replacer(match):
        keyword, table = match.group(1), match.group(2)
        table_lower = table.lower()
        
        # Skip CTEs, TABLE keyword, or tables with dots/parens
        if (table_lower in cte_names or table.upper() == 'TABLE' or 
            '(' in table or '.' in table):
            return match.group(0)
        
        # External tables get STG prefix (priority over block_tables)
        if table_lower in [t.lower() for t in external_tables]:
            logging.debug(f"External: {table}")
            return f"{keyword} STG.P{{{{praktijk_agb}}}}.{table}"
        
        # Skip block-created tables
        if table_lower in block_tables:
            return match.group(0)
        
        # Everything else uses ref()
        logging.debug(f"Internal: {table}")
        return f"{keyword} {{{{ ref('{table}') }}}}"
    
    return re.sub(table_pattern, table_replacer, sql, flags=re.IGNORECASE)