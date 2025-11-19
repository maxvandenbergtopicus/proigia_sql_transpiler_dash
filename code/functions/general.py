import re
from typing import Any, Dict
import yaml

def extract_view_name_from_query(query: str) -> str:
    """Extract view name from CREATE VIEW or CREATE MATERIALIZED VIEW statement."""
    # Try to match CREATE MATERIALIZED VIEW first
    match = re.search(r'CREATE\s+MATERIALIZED\s+VIEW\s+(\w+)', query, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Fall back to regular CREATE VIEW
    match = re.search(r'CREATE\s+VIEW\s+(\w+)', query, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None

def parse_pry_file(content: str) -> Dict[str, Any]:
    """Parse PRY file content into structured data."""
    # Replace any Jinja block (e.g., {% ... %}) with a space, preserving the rest of the line
    #filtered_lines = [re.sub(r'{%.*?%}', ' ', line) for line in content.splitlines()]
    #filtered_content = '\n'.join(filtered_lines)
    # Remove all Jinja blocks (e.g., {% ... %}) from metadata (before queries:)
    queries_split = content.split('queries:', 1)
    if len(queries_split) != 2:
        raise ValueError("Invalid PRY format: 'queries:' section not found")
    # Remove all Jinja blocks from metadata_yaml
    metadata_yaml = re.sub(r"{%-?[^%]*%}", '', queries_split[0])
    queries_section = queries_split[1]
    
    # Parse metadata
    metadata = yaml.safe_load(metadata_yaml)
    
    # Parse SQL queries (they're YAML list items starting with - |)
    queries = []
    current_query = []
    in_query = False
    
    for line in queries_section.split('\n'):
        if line.strip().startswith('- |'):
            if current_query:
                queries.append('\n'.join(current_query))
            current_query = []
            in_query = True
        elif in_query:
            # End query block only if we hit a non-indented line (new YAML key or list item)
            if line and not line.startswith(' ') and not line.startswith('\t'):
                # End of query block
                if current_query:
                    queries.append('\n'.join(current_query))
                    current_query = []
                in_query = False
            else:
                # Only remove 4 leading spaces if present, otherwise keep the line as is
                if line.startswith('    '):
                    current_query.append(line[4:])
                else:
                    current_query.append(line)
    
    # Add last query if exists
    if current_query:
        queries.append('\n'.join(current_query))
    
    metadata['parsed_queries'] = queries
    return metadata

def sanitize_folder_name(name: str) -> str:
    """Convert report name to valid folder name."""
    # Remove or replace invalid folder characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    # Remove multiple underscores
    name = re.sub(r'_+', '_', name)
    # Convert to lowercase for consistency
    name = name.lower().strip('_')
    return name

def preprocess_sql(sql: str) -> str:
    """Preprocess SQL to handle Jinja includes and other special syntax."""
    # Replace only {% include 'blockname.pry' %} with {{ blockname() }} in SQL queries
    sql = re.sub(r"{%-?\s*include\s+['\"]([\w\-]+)\.pry['\"]\s*%}", r"{{ \1() }}", sql)
    return sql
