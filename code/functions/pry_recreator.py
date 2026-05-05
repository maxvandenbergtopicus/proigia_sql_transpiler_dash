# contains code that creates a pry from dbt models

import functions
from pathlib import Path
import re
import os
import shutil
import yaml
from functools import lru_cache

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_CANDIDATES = [PROJECT_ROOT / "config.yml", PROJECT_ROOT / "config.yaml"]


def load_config() -> dict:
    for config_path in CONFIG_CANDIDATES:
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
    raise FileNotFoundError("No config.yml or config.yaml found in project root")


config = load_config()
SF_SQL_FOLDER = "/Users/gelderloos/repos/proigia_dbt/target/compiled/proigia_dbt/models/dm_dash_new"
PROIGIA_DEFINITION = Path(config["proigia_defintion_path"])
#SF_PROIGIA_DEFINITION = # this may be needed if we ever go back to writing the generated pry files to a different location than the original ones
logger = functions.setup_logging("pry_recreator.log", log_level="warning")

@lru_cache(maxsize=1)
def get_database_prefixes() -> tuple[str, ...]:
    """Read database prefixes from `databases:` in config.yml/config.yaml without YAML parsing."""
    for config_path in CONFIG_CANDIDATES:
        if not config_path.exists():
            continue

        lines = config_path.read_text(encoding="utf-8").splitlines()
        in_databases_block = False
        database_names: list[str] = []

        for line in lines:
            stripped = line.strip()

            if not in_databases_block:
                if re.match(r"^\s*databases\s*:\s*$", line):
                    in_databases_block = True
                continue

            if not stripped or stripped.startswith("#"):
                continue

            if re.match(r"^[A-Za-z_][\w-]*\s*:", line):
                break

            match_name = re.match(r"^\s*-\s*name\s*:\s*(.+?)\s*$", line)
            if match_name:
                value = match_name.group(1).split("#", 1)[0].strip()
                if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                if value:
                    database_names.append(value)

        if database_names:
            return tuple(database_names)

    return tuple()

@lru_cache(maxsize=None)
def resolve_report_sql_folder(report: str) -> str:
    """Resolve report folder in SF_SQL_FOLDER with case-insensitive fallback."""
    direct_path = Path(SF_SQL_FOLDER) / report
    if direct_path.is_dir():
        return report

    report_lower = report.lower()
    for entry in Path(SF_SQL_FOLDER).iterdir():
        if entry.is_dir() and entry.name.lower() == report_lower:
            return entry.name

    return report

def sql_to_pry_query_block(
    view: tuple,
    report: str, # report name is needed to find the compiled SQL
    strip_schema_from_from_clause: bool = True # strip DM_DASH
) -> str:
    """
    Convert SQL text to a PRY query block.

    Args:
        view: Tuple containing the view name and the original CREATE statement
        strip_schema_from_from_clause: If True, converts
            `FROM DB.SCHEMA.table_name` to `FROM table_name`.
        include_closing_marker: If True, append trailing `- |` marker.

    Returns:
        PRY-formatted query block as a string.
    """
    # find compiled SQL
    resolved_report = resolve_report_sql_folder(report)
    sql_file = f"{SF_SQL_FOLDER}/{resolved_report}/{view[0]}.sql"
    if not os.path.exists(sql_file):
        logger.error(f"SQL file not found: {sql_file}")
        return ""
    with open(sql_file, "r") as f:
        sql_text = f.read()
    sql = sql_text.strip()

    if strip_schema_from_from_clause:
        schema_names = get_database_prefixes()
        schema_pattern = "|".join(re.escape(name) for name in schema_names if name)
        if schema_pattern:
            sql = re.sub(rf"\b(?:{schema_pattern})\.P\d{{8}}\.", "", sql, flags=re.IGNORECASE)

    lines = ["- |"]
    # Replace CREATE ... VIEW variants with CREATE TABLE
    create_statement = view[1].strip()
    create_statement = re.sub(
        r"\bCREATE\s+(?:MATERIALIZED\s+)?VIEW\b",
        "CREATE TABLE",
        create_statement,
        flags=re.IGNORECASE,
    )
    lines.append(f"    {create_statement}")
    # add indentation to each line of the SQL
    lines.extend(f"    {line}" for line in sql.splitlines())

    if not lines[-1].rstrip().endswith(";"):
        lines[-1] = f"{lines[-1].rstrip()};"
    return "\n".join(lines)

def get_views_from_pry(template_pry: str) -> list:
    """
    Extract view names from the `queries:` section of a PRY template.

    The extracted name is the identifier after `CREATE VIEW` (or
    `CREATE OR REPLACE VIEW` / `CREATE MATERIALIZED VIEW`) and before `AS`.
    """
    match_queries_section = re.search(r"(?ms)^queries:\s*\n(.*)$", template_pry)
    if not match_queries_section:
        return []

    queries_section = match_queries_section.group(1)
    create_view_pattern = re.compile(
        r"(?is)\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(.+?)\s+AS\b"
    )

    view_matches = []
    for match in create_view_pattern.finditer(queries_section):
        raw_view_expression = match.group(1).strip()
        raw_view_expression = re.split(r"\(", raw_view_expression, maxsplit=1)[0].strip()
        object_name = raw_view_expression.split(".")[-1].strip().strip('"')
        view_matches.append((object_name, match.group(0)))

    return view_matches

def format_queries_from_sf(sf_views: list, reportname: str) -> str:
    pry_queries = [sql_to_pry_query_block(query, reportname) for query in sf_views]
    return "\n".join(pry_queries)

def suffix_name_in_pry(pry_template: str, suffix_name: str) -> str:
    """
    Add a suffix to the report name in the PRY template.

    This function looks for a line in the PRY template that starts with `name:`
    and appends the provided suffix to the report name.

    Args:
        pry_template: The original PRY template as a string.
        suffix_name: The suffix to append to the report name (e.g., " SF").

    Returns:
        The modified PRY template with the suffixed report name.
    """
    lines = pry_template.splitlines()
    if not lines:
        logger.error("PRY template is empty")
        raise ValueError("PRY template is empty")

    name_line_index = None
    bom_prefix = ""
    for idx, line in enumerate(lines):
        normalized = line.lstrip("\ufeff")
        if normalized.startswith("name:"):
            name_line_index = idx
            if line.startswith("\ufeff"):
                bom_prefix = "\ufeff"
            break

    if name_line_index is None:
        logger.error("No top-level 'name:' found in pry template")
        raise ValueError("No top-level 'name:' found in pry template")

    def append_suffix_to_key(line: str, key: str, keep_bom: bool = False) -> str:
        source_line = line.lstrip("\ufeff") if keep_bom else line
        rest = source_line[len(key):]
        rest_lstripped = rest.lstrip()
        leading_ws = rest[:len(rest) - len(rest_lstripped)]
        trimmed = rest_lstripped.rstrip()
        trailing_ws = rest_lstripped[len(trimmed):]

        if trimmed.startswith('"') and trimmed.endswith('"') and len(trimmed) >= 2:
            current_value = trimmed[1:-1]
            if current_value.endswith(suffix_name):
                new_trimmed = trimmed
            else:
                new_trimmed = f'"{current_value}{suffix_name}"'
        else:
            if trimmed.endswith(suffix_name):
                new_trimmed = trimmed
            else:
                new_trimmed = f"{trimmed}{suffix_name}"

        prefix = bom_prefix if keep_bom else ""
        return f"{prefix}{key}{leading_ws}{new_trimmed}{trailing_ws}"

    lines[name_line_index] = append_suffix_to_key(lines[name_line_index], "name:", keep_bom=True)

    for idx, line in enumerate(lines):
        if line.startswith("referencedreportname:"):
            lines[idx] = append_suffix_to_key(line, "referencedreportname:")

    pry_template = "\n".join(lines)
    return pry_template

def extract_report_name_from_pry(pry_template: str) -> str:
    """Extract report name from top-level `name:` metadata line."""
    for line in pry_template.splitlines():
        normalized = line.lstrip("\ufeff")
        if normalized.startswith("name:"):
            value = normalized[len("name:"):].strip()
            if value.startswith('"') and value.endswith('"') and len(value) >= 2:
                return value[1:-1]
            return value
    return "Unknown Report"

def pry_from_pry(
    report_folder: str,
    template_pry_file: str,
    suffix_name: str = "",
    db_type: str = None) -> str:
    """
    Creates a pry format string based on the original pry and the dbt models.
    Note: this can only be used in the current conversion
    Args:
        report_folder (str): The folder containing the report.
        template_pry_file (str): The original or template pry file
        suffix_name (str): Suffix to append to the report name in the new pry (e.g. " SF"). This is optional and can be left empty if no suffix is desired. Useful if you want a separate report in the portal.
    Returns:
        str: The new pry formatted string.
    """
    # get everything up until 'queries:' from the template pry. If there is no 'queries:' section, we will 
    # output the entire template (assumign it lives in blocks etc)
    with open(f"{PROIGIA_DEFINITION}/{report_folder}/{template_pry_file}.pry", "r") as f:
        pry_template = f.read()
        if report_folder == 'blocks':
            return pry_template
        if suffix_name:
            pry_template = suffix_name_in_pry(pry_template, suffix_name)
        if db_type:
            lines = pry_template.split("\n")
            if not any(line.strip().startswith("db_type:") for line in lines):
                insertion_index = 1 if len(lines) > 1 else len(lines)
                for idx, line in enumerate(lines):
                    if re.match(r"^\s*(reportviews|queries):\s*$", line):
                        insertion_index = idx
                        break
                lines.insert(insertion_index, f"db_type: {db_type}")
            pry_template = "\n".join(lines)
        if re.search(r"^queries:\s*$", pry_template, re.MULTILINE):
            header = pry_template.split("queries:")[0]
        else:
            return pry_template
    
    # TODO: put database type in header
    
    # get the query names from the original pry    
    reportviews = get_views_from_pry(pry_template)
    logger.debug(f"Reportviews extracted from original pry:")
    for rv in reportviews:
        logger.debug(rv)
    # get the report name from top-level metadata using string parsing only
    report_name = extract_report_name_from_pry(pry_template)
    # strip off the suffix_name from report_name
    if suffix_name and report_name.endswith(suffix_name):
        report_name = report_name[:-len(suffix_name)]
    report_name = re.sub(r'[^a-zA-Z0-9_]+', '_', report_name)
    queryblock = format_queries_from_sf(reportviews, report_folder)
    # join header & querys
    new_pry = "\n".join([header.rstrip(), "queries:", queryblock])
    return new_pry

def process_proigia_definition():
    """
    Process the entire Proigia definition folder, creating new PRY files for each report
    and copying all other files to the _sf folders.
    """
    for report_folder in os.listdir(PROIGIA_DEFINITION):
        report_path = os.path.join(PROIGIA_DEFINITION, report_folder)
        if not os.path.isdir(report_path):
            continue
        if report_folder.endswith("_sf"):
            continue
        if report_folder in ("blocks", "scripts"):
            continue
        logger.info(f"Processing report folder: {report_folder}")
        # find all .pry files in the folder
        template_pry_files = functions.find_pry_files(Path(report_path), ignored_keywords=[])
        if len(template_pry_files) == 0:
            logger.warning(f"No .pry file found in {report_folder}, skipping.")
            continue
        # create output folder if it doesn't exist yet
        output_folder = f"{PROIGIA_DEFINITION}/{report_folder}_sf"
        os.makedirs(output_folder, exist_ok=True)
        
        # Copy all files from the source folder to the output folder (excluding .pry files)
        logger.info(f"Copying all files from {report_folder} to {report_folder}_sf")
        for item in os.listdir(report_path):
            src_item = os.path.join(report_path, item)
            dst_item = os.path.join(output_folder, item)
            
            # Skip .pry files - they will be generated separately with _sf suffix
            if item.endswith('.pry'):
                logger.debug(f"Skipping .pry file: {item}")
                continue
            
            if os.path.isdir(src_item):
                # Remove existing directory if it exists and copy the entire directory
                if os.path.exists(dst_item):
                    shutil.rmtree(dst_item)
                shutil.copytree(src_item, dst_item)
                logger.debug(f"Copied directory: {item}")
            else:
                # Copy individual file
                shutil.copy2(src_item, dst_item)
                logger.debug(f"Copied file: {item}")
        
        # Now write new pry files (these will overwrite copied .pry files with updated content)
        for template_pry_file in template_pry_files:
            logger.info(f"Processing template PRY file: {template_pry_file}")
            template_pry_filename = Path(template_pry_file).stem
            is_aggregate_pry = Path(template_pry_file).name.lower().endswith("aggregate.pry")
            if is_aggregate_pry:
                new_pry_content = pry_from_pry(report_folder, template_pry_filename, suffix_name=" SF")
            else:
                new_pry_content = pry_from_pry(report_folder, template_pry_filename, suffix_name=" SF", db_type="snowflake")
            output_path = f"{output_folder}/{template_pry_filename}_sf.pry"
            with open(output_path, "w") as f:
                f.write(new_pry_content)
            logger.info(f"Generated new PRY file at: {output_path}")

def main():
    process_proigia_definition()

if __name__ == "__main__":
    main()