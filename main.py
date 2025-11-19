import sys
from pathlib import Path
import yaml
import datetime
from code.functions.dbt_wrapper import convert_pry_to_dbt


class TeeLogger:
    def __init__(self, log_path):
        self.terminal = sys.stdout
        self.log = open(log_path, "a", encoding="utf-8")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()


def find_pry_files(repo_path: Path, ignored_keywords: list) -> list:
    """Find all PRY files in repository, excluding files with ignored keywords in their names."""
    pry_files = []
    for pry_file in repo_path.rglob("*.pry"):
        if any(keyword.lower() in pry_file.name.lower() for keyword in ignored_keywords):
            print(f"[SKIPPED] {pry_file.name} (contains ignored keyword)")
            continue
        pry_files.append(pry_file)
    return pry_files


def main():
    # Set up logging to both terminal and logs/main.log
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    log_file = logs_dir / "main.log"
    # Always recreate the log file on each run
    open(log_file, "w").close()
    sys.stdout = TeeLogger(log_file)
    print(f"\n--- Run started at {datetime.datetime.now().isoformat()} ---\n")

    if len(sys.argv) < 2:
        print("Usage: python main.py <pry_file_or_repo_path> [output_dir]")
        sys.exit(1)
    
    input_path = Path(sys.argv[1])
    # Load config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    # Get output directory from args or config
    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    else:
        output_dir = Path(config.get("dbt_output_path", "."))
    # Get ignored keywords from config
    ignored_keywords = config.get("ignored_keywords", [])
    if not input_path.exists():
        print(f"Error: Path not found: {input_path}")
        sys.exit(1)
    if input_path.is_dir():
        print(f"Searching for PRY files in: {input_path}")
        pry_files = find_pry_files(input_path, ignored_keywords)
        print(f"\nFound {len(pry_files)} PRY files to process\n")
        if not pry_files:
            print("No PRY files found.")
            sys.exit(0)
        
        # Separate blocks from regular files
        block_files = [f for f in pry_files if any(p.name.lower() == 'blocks' for p in f.parents)]
        regular_files = [f for f in pry_files if f not in block_files]
        
        # First pass: Process all block files and track created tables
        block_tables = set()
        print(f"\n=== Processing {len(block_files)} block files ===")
        for pry_file in block_files:
            try:
                tables = convert_pry_to_dbt(pry_file, output_dir, config)
                if tables:
                    block_tables.update(tables)
            except Exception as e:
                print(f"[ERROR] Failed to process {pry_file.name}: {e}")
        
        print(f"\n=== Processing {len(regular_files)} regular files ===")
        # Second pass: Process regular files with knowledge of block tables
        for pry_file in regular_files:
            try:
                convert_pry_to_dbt(pry_file, output_dir, config, block_tables=block_tables)
            except Exception as e:
                print(f"[ERROR] Failed to process {pry_file.name}: {e}")
    else:
        print(f"Processing single file: {input_path}")
        convert_pry_to_dbt(input_path, output_dir)
        print(f"\nDone! Models generated in: {output_dir}")
    print(f"\n--- Run finished at {datetime.datetime.now().isoformat()} ---\n")


if __name__ == '__main__':
    main()