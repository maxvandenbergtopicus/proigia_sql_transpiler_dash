import sys
from pathlib import Path
import yaml
import logging
from code.functions.dbt_wrapper import convert_pry_to_dbt
from general import *

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