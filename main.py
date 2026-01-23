import re
import sys
from pathlib import Path
import yaml
import logging
from code.functions.dbt_wrapper import convert_pry_to_dbt
from code.functions.functions import setup_logging, find_pry_files


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

    # Scan all PRY files for materialized view names
    materialized_views = set()
    for pry_file in pry_files:
        try:
            content = pry_file.read_text(encoding='utf-8')
            # Only match CREATE MATERIALIZED VIEW viewname (MATERIALIZED required)
            matches = re.findall(r'CREATE\s+MATERIALIZED\s+VIEW\s+(\w+)', content, re.IGNORECASE)
            materialized_views.update([m.lower() for m in matches])
        except Exception as e:
            logging.error(f"[ERROR] Failed to scan {pry_file.name} for materialized views: {e}")

    # Add to config for downstream use
    config = dict(config)  # copy to avoid mutating original
    config['model_refs'] = list(materialized_views)
    logging.info(f"Identified {len(materialized_views)} materialized views in PRY files.\n")
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


def main():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    
    # Setup logging
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    setup_logging(logs_dir / "main.log", config.get('log_level', 'all'))
    
    # Get paths from config
    input_path = Path(config["proigia_defintion_path"])
    output_dir = Path(config["dbt_output_path"])
    seed_tables = config.get("seed_tabellen", [])
    
    if not input_path.exists():
        logging.error(f"Error: Path not found: {input_path}")
        logging.error(f"Check 'proigia_defintion_path' in config.yaml")
        sys.exit(1)
    
    if input_path.is_dir():
        process_directory(input_path, output_dir, config, seed_tables)
    else:
        logging.info(f"Processing single file: {input_path}")
        convert_pry_to_dbt(input_path, output_dir, config, seed_tables=seed_tables)
        logging.info(f"\nDone! Models generated in: {output_dir}")
        
if __name__ == '__main__':
    main()