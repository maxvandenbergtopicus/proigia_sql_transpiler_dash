import logging
from pathlib import Path
import re
import sys
from typing import Any, Dict
import yaml

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


def find_pry_files(repo_path: Path, ignored_keywords: list, skip_files: list = None, skip_folders: list = None) -> list:
    """Find PRY files, excluding those with ignored keywords, in skip_files list, or in skip_folders."""
    if skip_files is None:
        skip_files = []
    if skip_folders is None:
        skip_folders = []
    skip_folders = [Path(folder).name.lower() for folder in skip_folders]
    pry_files = []
    for f in repo_path.rglob("*.pry"):
        # Skip if in a folder to skip
        if any(p.name.lower() in skip_folders for p in f.parents):
            continue
        if any(kw.lower() in f.name.lower() for kw in ignored_keywords):
            continue
        if f.name in skip_files:
            continue
        pry_files.append(f)
    return pry_files
