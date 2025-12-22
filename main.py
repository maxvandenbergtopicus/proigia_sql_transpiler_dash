import sys
from pathlib import Path
import yaml
import datetime
import logging
from code.functions.dbt_wrapper import convert_pry_to_dbt


class DebugOnlyFilter(logging.Filter):
    """Filter that excludes INFO messages when in debug mode."""
    def filter(self, record):
        return record.levelno != logging.INFO

def setup_logging(log_file: Path, log_level: str = 'all'):
    """Setup logging to output to both console and file.
    
    Args:
        log_file: Path to log file
        log_level: 'all' (INFO+), 'error' (ERROR only), 'debug' (DEBUG, WARNING, ERROR - no INFO), 'warning' (WARNING+)
    """
    # Map config values to logging levels
    level_map = {
        'all': logging.INFO,
        'debug': logging.DEBUG,
        'warning': logging.WARNING,
        'error': logging.ERROR
    }
    
    level = level_map.get(log_level.lower(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels, handlers will filter
    
    # Clear any existing handlers
    logger.handlers = []
    
    # Create formatters
    formatter = logging.Formatter('%(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Add filter to exclude INFO when in debug mode
    if log_level.lower() == 'debug':
        console_handler.addFilter(DebugOnlyFilter())
    
    logger.addHandler(console_handler)
    
    # File handler - respects the same level as console
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Add filter to exclude INFO when in debug mode
    if log_level.lower() == 'debug':
        file_handler.addFilter(DebugOnlyFilter())
    
    logger.addHandler(file_handler)
    
    return logger


def find_pry_files(repo_path: Path, ignored_keywords: list) -> list:
    """Find all PRY files in repository, excluding files with ignored keywords in their names."""
    pry_files = []
    for pry_file in repo_path.rglob("*.pry"):
        if any(keyword.lower() in pry_file.name.lower() for keyword in ignored_keywords):
            logging.info(f"[SKIPPED] {pry_file.name} (contains ignored keyword)")
            continue
        pry_files.append(pry_file)
    return pry_files


def main():
    # Load config first to get log level
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Set up logging to both terminal and logs/main.log
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    log_file = logs_dir / "main.log"
    
    # Setup logging with configured level
    log_level = config.get('log_level', 'all')
    logger = setup_logging(log_file, log_level)
    logging.info(f"\n--- Run started at {datetime.datetime.now().isoformat()} ---\n")

    if len(sys.argv) < 2:
        logging.info("Usage: python main.py <pry_file_or_repo_path> [output_dir]")
        sys.exit(1)
    
    input_path = Path(sys.argv[1])
    # Get output directory from args or config
    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    else:
        output_dir = Path(config.get("dbt_output_path", "."))
    # Get ignored keywords from config
    ignored_keywords = config.get("ignored_keywords", [])
    if not input_path.exists():
        logging.error(f"Error: Path not found: {input_path}")
        sys.exit(1)
    if input_path.is_dir():
        logging.info(f"Searching for PRY files in: {input_path}")
        pry_files = find_pry_files(input_path, ignored_keywords)
        logging.info(f"\nFound {len(pry_files)} PRY files to process\n")
        if not pry_files:
            logging.info("No PRY files found.")
            sys.exit(0)
        
        # Separate blocks from regular files
        block_files = [f for f in pry_files if any(p.name.lower() == 'blocks' for p in f.parents)]
        regular_files = [f for f in pry_files if f not in block_files]
        
        # First pass: Process all block files and track created tables
        block_tables = set()
        logging.info(f"\n=== Processing {len(block_files)} block files ===")
        for pry_file in block_files:
            try:
                tables = convert_pry_to_dbt(pry_file, output_dir, config)
                if tables:
                    block_tables.update(tables)
            except Exception as e:
                logging.error(f"[ERROR] Failed to process {pry_file.name}: {e}")
        
        logging.info(f"\n=== Processing {len(regular_files)} regular files ===")
        # Second pass: Process regular files with knowledge of block tables
        for pry_file in regular_files:
            try:
                convert_pry_to_dbt(pry_file, output_dir, config, block_tables=block_tables)
            except Exception as e:
                logging.error(f"[ERROR] Failed to process {pry_file.name}: {e}")
    else:
        logging.info(f"Processing single file: {input_path}")
        convert_pry_to_dbt(input_path, output_dir)
        logging.info(f"\nDone! Models generated in: {output_dir}")
    logging.info(f"\n--- Run finished at {datetime.datetime.now().isoformat()} ---\n")


if __name__ == '__main__':
    main()