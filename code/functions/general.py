import logging
from pathlib import Path
import re
import sys
from typing import Any, Dict
import yaml

from dbt_wrapper import convert_pry_to_dbt

def extract_view_name_from_query(query: str) -> str:
    """Extract view name from CREATE [MATERIALIZED] VIEW statement."""
    match = re.search(r'CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(\w+)', query, re.IGNORECASE)
    return match.group(1) if match else None

def parse_pry_file(content: str) -> Dict[str, Any]:
    """Parse PRY file: extract metadata and SQL queries from YAML structure."""
    queries_split = content.split('queries:', 1)
    if len(queries_split) != 2:
        raise ValueError("Invalid PRY format: 'queries:' section not found")
    
    # Parse metadata (strip Jinja blocks first)
    metadata_yaml = re.sub(r"{%-?[^%]*%}", '', queries_split[0])
    metadata = yaml.safe_load(metadata_yaml)
    
    # Parse SQL queries from YAML list (- | blocks with 4-space indentation)
    queries = []
    current_query = []
    in_query = False
    
    for line in queries_split[1].split('\n'):
        if line.strip().startswith('- |'):
            if current_query:
                queries.append('\n'.join(current_query))
            current_query = []
            in_query = True
        elif in_query:
            # Non-indented line ends query block, unless it's Jinja template syntax
            if line and not line.startswith((' ', '\t')):
                if line.strip().startswith(('{%', '{{')):
                    current_query.append(line)  # Keep Jinja includes
                else:
                    if current_query:
                        queries.append('\n'.join(current_query))
                        current_query = []
                    in_query = False
            else:
                # Strip 4-space YAML indentation
                current_query.append(line[4:] if line.startswith('    ') else line)
    
    if current_query:
        queries.append('\n'.join(current_query))
    
    metadata['parsed_queries'] = queries
    return metadata

def sanitize_folder_name(name: str) -> str:
    """Convert report name to valid folder name."""
    name = re.sub(r'[<>:"/\\|?*]', '', name)  # Remove invalid chars
    name = re.sub(r'_+', '_', name.replace(' ', '_'))  # Spaces to underscores
    return name.lower().strip('_')

def preprocess_sql(sql: str) -> str:
    """Preprocess SQL to handle Jinja includes and other special syntax."""
    # Replace only {% include 'blockname.pry' %} with {{ blockname() }} in SQL queries
    sql = re.sub(r"{%-?\s*include\s+['\"]([\w\-]+)\.pry['\"]\s*%}", r"{{ \1() }}", sql)
    return sql

def setup_logging(log_file: Path, log_level: str = 'all'):
    """Setup logging to console and file."""
    level_map = {'all': logging.INFO, 'debug': logging.DEBUG, 'warning': logging.WARNING, 'error': logging.ERROR}
    level = level_map.get(log_level.lower(), logging.INFO)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    
    formatter = logging.Formatter('%(message)s')
    
    # Console and file handlers with same config
    for handler in [logging.StreamHandler(sys.stdout), 
                    logging.FileHandler(log_file, mode='w', encoding='utf-8')]:
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def find_pry_files(repo_path: Path, ignored_keywords: list) -> list:
    """Find PRY files, excluding those with ignored keywords."""
    return [f for f in repo_path.rglob("*.pry") 
            if not any(kw.lower() in f.name.lower() for kw in ignored_keywords)]


def process_directory(input_path: Path, output_dir: Path, config: dict, seed_tables: list):
    """Process all PRY files in a directory (blocks first, then regular files)."""
    ignored_keywords = config.get("ignored_keywords", [])
    pry_files = find_pry_files(input_path, ignored_keywords)
    
    logging.info(f"Searching for PRY files in: {input_path}")
    logging.info(f"\nFound {len(pry_files)} PRY files to process\n")
    
    if not pry_files:
        logging.info("No PRY files found.")
        return
    
    # Separate blocks from regular files
    block_files = [f for f in pry_files if any(p.name.lower() == 'blocks' for p in f.parents)]
    regular_files = [f for f in pry_files if f not in block_files]
    
    # First pass: Process blocks and track created tables
    block_tables = set()
    logging.info(f"\n=== Processing {len(block_files)} block files ===")
    for pry_file in block_files:
        try:
            tables = convert_pry_to_dbt(pry_file, output_dir, config, seed_tables=seed_tables)
            if tables:
                block_tables.update(tables)
        except Exception as e:
            logging.error(f"[ERROR] Failed to process {pry_file.name}: {e}")
    
    # Second pass: Process regular files
    logging.info(f"\n=== Processing {len(regular_files)} regular files ===")
    for pry_file in regular_files:
        try:
            convert_pry_to_dbt(pry_file, output_dir, config, block_tables=block_tables, seed_tables=seed_tables)
        except Exception as e:
            logging.error(f"[ERROR] Failed to process {pry_file.name}: {e}")
