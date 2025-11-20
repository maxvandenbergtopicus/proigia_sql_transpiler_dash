from pathlib import Path
from typing import Any, Dict
import re
from code.functions.dialect_converter import convert_postgres_to_snowflake
from code.functions.general import *
import traceback

def convert_pry_to_dbt(pry_path: Path, output_dir: Path, config, block_tables=None) -> set:
    """Convert PRY file to dbt models.
    
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
        macro_file.write_text(macro_content, encoding='utf-8')
        
        if created_tables:
            print(f"[OK] Block macro generated: {macro_file} (creates: {', '.join(created_tables)})")
        else:
            print(f"[OK] Block macro generated: {macro_file}")
        
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
                print(f"Warning: Could not extract view name from query {i+1}")
                continue
            view_metadata = next((rv for rv in reportviews if rv.get('name') == view_name), {})
            if view_metadata.get('external', False):
                print(f"Skipping external view: {view_name}")
                continue
            generate_dbt_model(
                view_name=view_name,
                query=query,
                report_name=report_name,
                report_type=report_type,
                view_metadata=view_metadata,
                output_dir=full_output_dir,
                block_tables=block_tables
            )
        return set()

def generate_dbt_model(
    view_name: str,
    query: str,
    report_name: str,
    report_type: str,
    view_metadata: Dict[str, Any],
    output_dir: Path,
    block_tables=None
) -> None:
    """Generate a single dbt model file."
    
    Args:
        block_tables: Set of table names created by block files
    """
    
    try:
        # Preprocess SQL (handles comment conversion and includes)
        preprocessed = preprocess_sql(query)
        # Preserve dbt macro calls by replacing them with placeholders
        macro_pattern = r"({{\s*[\w_]+\(\)\s*}})"
        macros = []
        def macro_replacer(match):
            macros.append(match.group(1))
            return f"__DBT_MACRO_{len(macros)-1}__"
        temp_sql = re.sub(macro_pattern, macro_replacer, preprocessed)
        # Convert SQL from PostgreSQL to Snowflake
        converted_sql = convert_postgres_to_snowflake(temp_sql)
        # Restore macro calls
        for idx, macro in enumerate(macros):
            converted_sql = converted_sql.replace(f"__DBT_MACRO_{idx}__", macro)
        # Replace all SQL comments with Jinja comments (after SQL conversion)
        # Multi-line comments: /* ... */  ->  {# ... #}
        converted_sql = re.sub(r'/\*', r'{#', converted_sql)
        converted_sql = re.sub(r'\*/', r'#}', converted_sql)
        # Single-line comments: -- ...  ->  {# ... #}
        converted_sql = re.sub(r'--([^\n]*)', r'{# \1 #}', converted_sql)
        # Check if actually converted
        if converted_sql == preprocessed:
            print("[WARNING] SQL was not modified during conversion")
        # Remove CREATE [MATERIALIZED] VIEW statement, keep only the SELECT/WITH
        converted_sql = re.sub(
            r'CREATE\s+(MATERIALIZED\s+)?VIEW\s+\w+\s+AS\s*',
            '',
            converted_sql,
            count=1,
            flags=re.IGNORECASE
        )
        # Replace table references with dbt macros
        converted_sql = replace_table_references(converted_sql, block_tables=block_tables)
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
        # First replace quoted '${varname}' or "${varname}" with unquoted dbt variable
        converted_sql = re.sub(r"(['\"])\$\{([a-zA-Z_][\w]*)\}\1", lambda m: f"{{{{ var('{m.group(2)}', none) }}}}", converted_sql)
        # Then replace any remaining unquoted ${varname}
        converted_sql = re.sub(r'\$\{([a-zA-Z_][\w]*)\}', lambda m: f"{{{{ var('{m.group(1)}', none) }}}}", converted_sql)
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
            f"    tags=['{report_type}', 'report']"
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
        # Write to file
        output_file = output_dir / f"{view_name}.sql"
        output_file.write_text(model_content, encoding='utf-8')
        print(f"[OK] Generated: {output_file}\n")
    except Exception as e:
        print(f"[ERROR] Error processing {view_name}: {e}")
        traceback.print_exc()


def replace_table_references(sql: str, external_tables=None, block_tables=None) -> str:
    """
    Replace table references in FROM/JOIN clauses with dbt ref() or source() macros.
    If table is in external_tables, use STG.P{{praktijk_agb}}.{table}, else use ref().
    Do not replace tables that are CTEs in the WITH clause or created by block files.
    
    Args:
        block_tables: Set of table names created by block files (should not be replaced)
    """
    if block_tables is None:
        block_tables = set()
    if external_tables is None:
        external_tables = [
            'allergie', 'bepaling', 'contact', 'contraindicatie', 'episode', 'journaal',
            'journaalregel', 'medewerker', 'medicatie', 'metadata', 'origineel',
            'patient', 'praktijk', 'ruiter', 'verrichting', 'verwijzing', 'override_patientenlijst', 'functie', 'medewerker_hisnaam'
        ]
    
    # Robustly extract all CTE names from the entire SQL (not just top-level WITH)
    cte_names = set()
    # Temporarily remove comments (both SQL and Jinja) to avoid false matches in CTE detection only
    sql_no_comments = re.sub(r'--[^\n]*', '', sql)
    sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_no_comments, flags=re.DOTALL)
    sql_no_comments = re.sub(r'\{#.*?#\}', '', sql_no_comments, flags=re.DOTALL)
    # Find all potential CTE definitions: word followed by AS (optionally with any number of extra words like MATERIALIZED)
    for match in re.finditer(r'\b(\w+)\s*(?:\([^)]+\))?\s+AS(?:\s+\w+)*\s*\(', sql_no_comments, re.IGNORECASE):
        potential_cte = match.group(1).lower()
        # Exclude SQL keywords that might match this pattern
        if potential_cte not in ['select', 'insert', 'update', 'delete', 'with', 'case']:
            cte_names.add(potential_cte)
    
    # Note: We only removed comments for CTE detection, the original SQL with comments is preserved
    
    # Regex to match FROM or any JOIN type, but skip if immediately followed by LATERAL (e.g., JOIN LATERAL, LEFT JOIN LATERAL, etc.)
    # Only match if the table name is not followed by a dot (schema/table or table.column),
    # not followed by an open parenthesis (function call),
    # and not immediately followed by '::' (type cast)
    pattern = r'\b(FROM|JOIN(?!\s+LATERAL)|LEFT\s+JOIN(?!\s+LATERAL)|RIGHT\s+JOIN(?!\s+LATERAL)|INNER\s+JOIN(?!\s+LATERAL)|OUTER\s+JOIN(?!\s+LATERAL)|FULL\s+JOIN(?!\s+LATERAL)|CROSS\s+JOIN(?!\s+LATERAL))\s+([a-zA-Z_][\w]*)\b(?!\s*\.|\s*\(|::)'
    
    def replacer(match):
        keyword = match.group(1)
        table = match.group(2)

        # Skip if it's a subquery (starts with parentheses) or function call
        if '(' in table:
            return match.group(0)

        # Skip if table is a CTE
        if table.lower() in cte_names:
            return match.group(0)

        # Skip if table is created by a block file
        if block_tables and table.lower() in block_tables:
            return match.group(0)

        # Skip if it's a schema-qualified table (e.g., schema.table)
        if '.' in table:
            return match.group(0)

        # Skip if the table is the literal TABLE keyword (Snowflake table function)
        if table.upper() == 'TABLE':
            return match.group(0)

        # Use correct dbt macro syntax with double curly brackets
        if table.lower() in [t.lower() for t in external_tables]:
            replacement = f"{keyword} STG.P{{{{praktijk_agb}}}}.{table}"
        else:
            replacement = f"{keyword} {{{{ ref('{table}') }}}}"
        return replacement
    
    return re.sub(pattern, replacer, sql, flags=re.IGNORECASE)