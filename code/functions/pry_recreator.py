# contains code that creates a pry from dbt models

import functions
from pathlib import Path
import re
import os

SF_SQL_FOLDER = "/home/coder/proigia_sql_transpiler_dash/dm_dash_new" #TODO change this to whatever it is in actuality
PROIGIA_DEFINITION = "/home/coder/proigia_definition" #TODO change this to whatever it is in actuality
SF_PROIGIA_DEFINITION = "/home/coder/proigia_sql_transpiler_dash/proigia_definition" #TODO change this to whatever it is in actuality
logger = functions.setup_logging("/home/coder/pry_recreator.log", log_level="debug")

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
    sql_file = f"{SF_SQL_FOLDER}/{report}/{view[0]}.sql"
    if not os.path.exists(sql_file):
        logger.error(f"SQL file not found: {sql_file}")
        return ""
    with open(sql_file, "r") as f:
        sql_text = f.read()
    sql = sql_text.strip()

    if strip_schema_from_from_clause:
        sql = sql.replace("FROM DM_DASH.P77775027.", "FROM ") #TODO either remove this or make it generic, depending on whether we have schema names inn the generated SQL or not

    lines = ["- |"]
    # add the original CREATE statement (can be C REATE MATERIALIZED or whatever, we just take whatever is in the original SQL)
    lines.append(f"    {view[1]}")
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

def pry_from_pry(
    report_folder: str,
    template_pry_file: str) -> str:
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
    with open(f"{PROIGIA_DEFINITION}/{report_folder}/{template_pry_file}", "r") as f:
        pry_template = f.read()
        if report_folder == 'blocks':
            return pry_template
        if re.search(r"^queries:\s*$", pry_template, re.MULTILINE):
            header = pry_template.split("queries:")[0]
        else:
            return pry_template
    # get the query names from the original pry    
    reportviews = get_views_from_pry(pry_template)
    logger.debug(f"Reportviews extracted from original pry:")
    for rv in reportviews:
        logger.debug(rv)
    # get the queries from the dbt models
    queryblock = format_queries_from_sf(reportviews, report_folder)
    # join header & querys
    new_pry = "\n".join([header, "queries:", queryblock])
    # create pry
    return new_pry

def process_proigia_definition():
    """
    Process the entire Proigia definition folder, creating new PRY files for each report.
    """
    for report_folder in os.listdir(PROIGIA_DEFINITION):
        if not os.path.isdir(os.path.join(PROIGIA_DEFINITION, report_folder)):
            continue
        logger.info(f"Processing report folder: {report_folder}")
        # find all .pry files in the folder
        template_pry_files = functions.find_pry_files(Path(os.path.join(PROIGIA_DEFINITION, report_folder)), ignored_keywords=[])
        if len(template_pry_files) == 0:
            logger.warning(f"No .pry file found in {report_folder}, skipping.")
            continue
        # create output folder if it doesn't exist yet and write new pry files
        output_folder = f"{SF_PROIGIA_DEFINITION}/{report_folder}"
        os.makedirs(output_folder, exist_ok=True) 
        for template_pry_file in template_pry_files:
            template_pry_filename = Path(template_pry_file).name
            new_pry_content = pry_from_pry(report_folder, template_pry_filename)
            output_path = f"{SF_PROIGIA_DEFINITION}/{report_folder}/{template_pry_filename}"
            with open(output_path, "w") as f:
                f.write(new_pry_content)
            logger.info(f"Generated new PRY file at: {output_path}")

def main():
    process_proigia_definition()

if __name__ == "__main__":
    main()