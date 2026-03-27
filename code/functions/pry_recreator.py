# contains code that creates a pry from dbt models

import re


def sql_to_pry_query_block(
    sql_text: str,
    view_name: str,
    report: str, # report name is needed to find the compiled SQL
    compiled_folder: str = "/home/coder/proigia_sql_transpiler_dash/dm_dash_new",
    strip_schema_from_from_clause: bool = True
) -> str:
    """
    Convert SQL text (typically a SELECT) to a PRY query block.

    Args:
        view_name: View name/name of the dbt model
        strip_schema_from_from_clause: If True, converts
            `FROM DB.SCHEMA.table_name` to `FROM table_name`.
        include_closing_marker: If True, append trailing `- |` marker.

    Returns:
        PRY-formatted query block as a string.
    """
    # find compiled SQL
    sql_file = f"{compiled_folder}/{report}/{view_name}.sql"
    with open(sql_file, "r") as f:
        sql_text = f.read()
    sql = sql_text.strip()

    if strip_schema_from_from_clause:
        sql = re.sub(
            r"(?i)\bFROM\s+(?:[A-Za-z_][\w$]*\.){1,2}([A-Za-z_][\w$]*)",
            r"FROM \1",
            sql,
        )

    # add CREATE VIEW statement to the SQL, using the provided view name
    create_view_sql = f"CREATE VIEW {view_name} AS\n{sql}"

    lines = ["- |"]
    # add indentation to each line of the SQL
    lines.extend(f"    {line}" for line in create_view_sql.splitlines())

    if not lines[-1].rstrip().endswith(";"):
        lines[-1] = f"{lines[-1].rstrip()};"
    
    return "\n".join(lines)

def format_pry(pry_content: dict) -> str:
    """
    Formats a pry content dictionary into a string format that can be used in the pry file.
    Args:        pry_content (dict): The pry content as a dictionary, containing
        * name
        * description (optional)
        * reportviews (required): list of dictionaries. per query, specifies the following:
            * name
            * queryorder
            * external (boolean)
        * queries: list of (long) strings, each containing a CREATE VIEW statement for one of the non-external views listed above
    
    this is only a dictionary so that the list of arguments doesnt become crazy if we add some type of parameters
    
    NB: both reportviews and queries are assumed to be sorted lists. We are eliberately not sorting within this function.
    The reason is that this function handles block statements as well, and those do not always explicitly 
    state the query order (i can be contained within the block statement itself, blocks can contain other 
    blocks, basically we do not want to get into that mess and just copy them over)

    Current .prys are already ordered, so we just take them as is. If we would want to create a pry from dbt models, 
    we would need to sort them by queryorder before passing them to this function.
    
    Returns:     a string formatted as a .pry (yaml)
    """
    if "name" in pry_content: # this is optional because some block .pry's do not have a name
        pry_string = f"name: {pry_content['name']}\n"
    if "description" in pry_content:
        pry_string += f"description: {pry_content['description']}\n"

    pry_string += "reportviews:\n" # TODO this should be optional as well because some block pry's do not have it
    # first sort the views by their queryorder
    # views_sorted = sorted(pry_content["reportviews"], key=lambda x: x["queryorder"])
    for view in pry_content["reportviews"]:
        # if this is a blockline, insert is as is
        if view["type"] == "block":
            pry_string += f"{view['content']}\n"
        # else create the view yaml
        elif view["type"] == "view":
            pry_string += f"  - name: {view['name']}\n"
            pry_string += f"    queryorder: {view['queryorder']}\n"
            if view['external']:
                pry_string += f"    external: true\n"

    pry_string += "queries:\n"
    for query in pry_content["queries"]:
        pry_string += "  - |\n"
        for line in query.splitlines():
            pry_string += f"    {line}\n"
        if not query.endswith("\n"):
            pry_string += "\n"
    return pry_string

def get_queryorder_from_pry(pry_file: str) -> list:
    """
    Gets the queryorder from a pry file and returns it as a dictionary.
    Args:
        pry_file (str): The path to the pry file. This can be an existing full .pry file,
        or a template that contains the queryorder
    Returns:
        list: A list of dictionaries in two formats:
        * block: containing the block reference as a string
        * view: containing the view names, their queryorder, and a boolean flag indicating if they are external.
    """
    with open(pry_file, "r") as f:
        pry_content = f.read()
    # split the content into lines and find the reportviews section
    lines = pry_content.splitlines()
    reportviews_index = lines.index("reportviews:")
    queryorder_list = []
    for line in lines[reportviews_index + 1:]:
        if line.startswith("queries:"):
            break
        if line.startswith("{%"):
            queryorder_list.append({
                "type": "block",
                "content": line
            })
        elif line.strip().startswith("- name:"):
            view_name = line.strip().split(":")[1].strip()
            queryorder = None
            external = False
        elif line.strip().startswith("queryorder:"):
            queryorder = int(line.strip().split(":")[1].strip())
        elif line.strip().startswith("external:"):
            external = line.strip().split(":")[1].strip().lower() == "true"
        if view_name and queryorder is not None:
            queryorder_list.append({
                "type": "view",
                "name": view_name,
                "queryorder": queryorder,
                "external": external
            })
    return queryorder_list

def get_queries_from_dbt(dbt_models: list) -> list:
    """
    Gets the queries from the dbt models and returns them as a list of strings.
    Args:
        dbt_models (list): A list of dbt model objects. Each object should contain the following attributes:
        * name: The name of the model, which will be used as the view name in the pry file
        * query: The SQL query that defines the model, which will be used as the query in the pry file
    Returns:
        list: A list of strings, each containing a CREATE VIEW statement for one of the dbt models. 
        The view name should be the same as the model name, and the query should be the same as the model query.
    """

def pry_from_pry(
    original_pry: str,
    block = True) -> str:
    """
    Creates a pry format string based on the original pry and the dbt models.
    Note: this can only be used in the current conversion
    Args:
        original_pry (str): The filename of the original pry
    Returns:
        str: The new pry formatted string.
    """
    if block:
        raise NotImplementedError("Blocks cannot be recreated yet")
    # get the name, description, and queryorder from the original pry    
    reportviews = get_queryorder_from_pry(original_pry)
    # create the list of queries based on the reportviews (for now we just copy the queries from the original pry, but in the future we will need to create them based on the dbt models)
    queries = get_queries_from_pry(original_pry)

def pry_from_dbt():
    # both queryorder and the list of queries should be ordered according to queryorder
    pass