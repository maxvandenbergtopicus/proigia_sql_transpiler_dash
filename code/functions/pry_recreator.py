# contains code that creates a pry from dbt models

import functions
from pathlib import Path
import re
import os
import yaml
from functools import lru_cache

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_CANDIDATES = [PROJECT_ROOT / "config.yml", PROJECT_ROOT / "config.yaml"]


def load_config() -> dict:
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    
    with open("env.yaml") as f:
        config_env = yaml.safe_load(f)
    
    config.update(config_env)  # Merge config with env values if present
    return config


config = load_config()
SF_SQL_FOLDER = Path(config.get("dbt_path")) / "target/compiled/proigia_dbt/models/dm_dash_new"
PROIGIA_DEFINITION = Path(config["proigia_defintion_path"])
GENERATED_AGB = config.get("generated_agb", "77775027")
#SF_PROIGIA_DEFINITION = # this may be needed if we ever go back to writing the generated pry files to a different location than the original ones
logger = functions.setup_logging("pry_recreator.log", log_level="debug")

def get_database_prefixes() -> tuple[str, ...]:
    """Return database name prefixes from the `databases:` list in config.yaml."""
    return tuple(db["name"] for db in config.get("databases", []) if db.get("name"))

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
    with open(sql_file, "r", encoding="utf-8-sig") as f:
        sql_text = f.read()
    sql = sql_text.strip()

    if strip_schema_from_from_clause:
        schema_names = get_database_prefixes()
        schema_pattern = "|".join(re.escape(name) for name in schema_names if name)
        if schema_pattern:
            sql = re.sub(rf"\b(?:{schema_pattern})\.P\d{{8}}\.", "", sql, flags=re.IGNORECASE)

    sql = sql.replace(GENERATED_AGB, "${agb}")

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

def _find_top_level_section_bounds(lines: list[str], section_key: str) -> tuple[int | None, int]:
    """Return start (line with section key) and end index of a top-level YAML section."""
    section_start = None
    for idx, line in enumerate(lines):
        normalized = line.lstrip("\ufeff")
        if normalized.startswith(f"{section_key}:"):
            section_start = idx
            break

    if section_start is None:
        return None, len(lines)

    section_end = len(lines)
    for idx in range(section_start + 1, len(lines)):
        if re.match(r"^[^\s].*:\s*$", lines[idx]):
            section_end = idx
            break

    return section_start, section_end

def _split_list_items(section_lines: list[str]) -> list[list[str]]:
    """Split a YAML list section into item blocks, preserving each item's full lines."""
    item_starts = [i for i, line in enumerate(section_lines) if line.lstrip().startswith("- ")]
    if not item_starts:
        return []

    items = []
    for pos, item_start in enumerate(item_starts):
        next_item_start = item_starts[pos + 1] if pos + 1 < len(item_starts) else len(section_lines)
        items.append(section_lines[item_start:next_item_start])
    return items

def _with_dataset_type(item_lines: list[str], dataset_type: str) -> list[str]:
    """Ensure a YAML list item contains dataset_type, replacing existing values if needed."""
    for idx, line in enumerate(item_lines):
        if re.match(r"^\s*dataset_type\s*:", line):
            indent_match = re.match(r"^(\s*)dataset_type\s*:", line)
            indent = indent_match.group(1) if indent_match else ""
            updated = item_lines.copy()
            updated[idx] = f"{indent}dataset_type: {dataset_type}"
            return updated

    item_indent = re.match(r"^(\s*)-", item_lines[0])
    base_indent = item_indent.group(1) if item_indent else ""
    dataset_line = f"{base_indent}  dataset_type: {dataset_type}"

    insert_at = len(item_lines)
    while insert_at > 0 and item_lines[insert_at - 1].strip() == "":
        insert_at -= 1

    return item_lines[:insert_at] + [dataset_line] + item_lines[insert_at:]

def add_dataset_type_to_reportviews(pry_template: str, dataset_type: str) -> str:
    """Set dataset_type on each reportview item, replacing existing values when present."""
    lines = pry_template.splitlines()
    if not lines:
        return pry_template

    reportviews_start, reportviews_end = _find_top_level_section_bounds(lines, "reportviews")
    if reportviews_start is None:
        return pry_template

    section = lines[reportviews_start + 1:reportviews_end]
    items = _split_list_items(section)
    if not items:
        return pry_template

    # Preserve any non-item lines between list items while rewriting each item block.
    new_section = []
    cursor = 0
    for item_lines in items:
        item_start = section.index(item_lines[0], cursor)
        new_section.extend(section[cursor:item_start])
        new_section.extend(_with_dataset_type(item_lines, dataset_type))
        cursor = item_start + len(item_lines)
    new_section.extend(section[cursor:])

    updated_lines = lines[:reportviews_start + 1] + new_section + lines[reportviews_end:]
    return "\n".join(updated_lines)

def pry_from_pry(
    report_folder: str,
    template_pry_file: str,
    dataset_type: str = None) -> str:
    """
    Creates a pry format string based on the original pry and the dbt models.
    Note: this can only be used in the current conversion
    Args:
        report_folder (str): The folder containing the report.
        template_pry_file (str): The original or template pry file
    Returns:
        str: The new pry formatted string.
    """
    # get everything up until 'queries:' from the template pry. If there is no 'queries:' section, we will 
    # output the entire template (assumign it lives in blocks etc)
    with open(f"{PROIGIA_DEFINITION}/{report_folder}/{template_pry_file}.pry", "r", encoding="utf-8-sig") as f:
        pry_template = f.read()
        if report_folder == 'blocks':
            return pry_template
        if dataset_type:
            pry_template = add_dataset_type_to_reportviews(pry_template, dataset_type)
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
    queryblock = format_queries_from_sf(reportviews, report_folder)
    # join header & querys
    new_pry = "\n".join([header.rstrip(), "queries:", queryblock])
    return new_pry

def process_proigia_definition():
    """
    Process the entire Proigia definition folder and generate Snowflake PRY files
    directly in each source report folder.
    """
    for report_folder in os.listdir(PROIGIA_DEFINITION):
        report_path = os.path.join(PROIGIA_DEFINITION, report_folder)
        if not os.path.isdir(report_path):
            continue
        if report_folder in ("blocks", "scripts"):
            continue
        logger.info(f"Processing report folder: {report_folder}")
        # find all .pry files in the folder
        template_pry_files = functions.find_pry_files(Path(report_path), ignored_keywords=["_sf"])
        if len(template_pry_files) == 0:
            logger.warning(f"No .pry file found in {report_folder}, skipping.")
            continue

        # Now write new pry files (these will overwrite copied .pry files with updated content)
        for template_pry_file in template_pry_files:
            logger.info(f"Processing template PRY file: {template_pry_file}")
            template_pry_filename = Path(template_pry_file).stem
            is_aggregate_pry = Path(template_pry_file).name.lower().endswith("aggregate.pry")
            if is_aggregate_pry:
                new_pry_content = pry_from_pry(report_folder, template_pry_filename)
            else:
                new_pry_content = pry_from_pry(report_folder, template_pry_filename, dataset_type="snowflake")
            output_path = f"{report_path}/{template_pry_filename}_sf.pry"
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(new_pry_content)
            logger.info(f"Generated new PRY file at: {output_path}")

def main():
    process_proigia_definition()

if __name__ == "__main__":
    main()