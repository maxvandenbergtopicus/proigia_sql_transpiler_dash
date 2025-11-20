import re

def parse_crosstab_sql(sql_content: str):
    """
    Parse a crosstab SQL block and extract pivot information and output columns.
    Returns a dict with keys: 'pivot_col', 'from_statements', 'output_cols', 'cte_select_statement', 'pivot_statement', 'cte_statement'.
    Raises ValueError if unsupported patterns are found.
    """
    # Patterns
    pattern_1 = r'\$\$(.*?)\$\$'        # everything within $$...$$
    pattern_2 = r'\)\s*as\s+(?:\w+\s*)?\(\s*(.*?)\s*\)' # from $$) as ... ( ... )
    pattern_3 = r'SELECT(.*?)FROM'      # between SELECT and FROM
    pattern_4 = r'FROM(.*?)ORDER'       # between FROM and ORDER
    pattern_5 = r'\b(FROM|JOIN(?!\s+LATERAL)|LEFT\s+JOIN(?!\s+LATERAL)|RIGHT\s+JOIN(?!\s+LATERAL)|INNER\s+JOIN(?!\s+LATERAL)|OUTER\s+JOIN(?!\s+LATERAL)|FULL\s+JOIN(?!\s+LATERAL)|CROSS\s+JOIN(?!\s+LATERAL))\s+([a-zA-Z_][\w]*)\b(?!\s*\.|\s*\(|::)'
    pattern_6 = r'(WITH\s+.*\))\s*(?=SELECT|FROM|$)' # Ten behoeve van het WITH statement 

    # Extract statements
    statements = re.findall(pattern_1, sql_content, re.DOTALL)
    if len(statements) < 2:
            return ''
    cte_statement = statements[0]
    pivot_statement = statements[1]

    # Check for unsupported patterns
    if 'JOIN' in pivot_statement.upper():
            print("Unsupported JOIN found in pivot_statement.")
            return ''

    # Extract pivot column and from statements safely
    m_pivot_col = re.search(pattern_3, pivot_statement, re.IGNORECASE | re.DOTALL)
    if not m_pivot_col:
        return ''
    
    # Find WITH statement if present within the CROSSTAB satement
    # If so, split the statement between the WITH-block and the SELECT-block
    m_with_match = re.search(pattern_6, cte_statement, re.IGNORECASE | re.DOTALL)
    if m_with_match:
        with_statement = m_with_match.group(1)
        # Remove the WITH block from cte_statement to get just the SELECT part
        cte_statement = cte_statement[m_with_match.end():].strip()
    else:
        with_statement = None

    pivot_col = m_pivot_col.group(1).strip()
    m_from_statements = re.search(pattern_4, pivot_statement, re.IGNORECASE | re.DOTALL)
    if not m_from_statements:
        return ''
    from_statements = m_from_statements.group(1).strip()

    # Build dbt_utils.get_column_values statement (example)
    str_dbt_get_column_values = "{{ dbt_utils.pivot('<pivot_col>',\
        dbt_utils.get_column_values(ref('<input_model>'),'categorie',default=[]),\
        agg='',\
        then_value='<value_col>',\
        else_value=\"ARRAY_CONSTRUCT()\",\
        quote_identifiers=False)}} ".replace('<pivot_col>', pivot_col).replace('<input_model>', from_statements)
    # print("dbt utils get column values statement:\n", str_dbt_get_column_values)    

    ## Bepaal welke kolommen meegaan in de output
    m_statement_as = re.search(pattern_2, sql_content, re.IGNORECASE | re.DOTALL)
    if not m_statement_as:
        return ''
    statement_as = m_statement_as.group(1).strip().replace('(','').replace(';','').strip()
    # print('Statement AS: ', statement_as)
    output_cols = []
    for item in statement_as.split(','):
        output_cols.append(item.strip().split(' ')[0])
    # print('Output columns: ', output_cols)

    ## Selecteer de kolom-statements uit de cte_statement
    m_cte_select_statement = re.search(pattern_3, cte_statement, re.IGNORECASE | re.DOTALL)
    if not m_cte_select_statement:
        return ''
    cte_select_statement = m_cte_select_statement.group(1).strip()
    # print('Select columns: ', cte_select_statement)
    select_cols = []
    parent_count = 0
    current_col = ''
    for char in cte_select_statement:
        if char == '(' or char == '[':
            parent_count += 1
        elif char == ')' or char == ']':
            parent_count -= 1
        elif (char == ',') and parent_count == 0:
            current_col = current_col.strip()
            
            select_cols.append(current_col)
            current_col = ""
            continue
        current_col += char
    # Add the last column
    if current_col.strip():
        select_cols.append(current_col.strip())
    # print('Select columns parsed: ', select_cols)

    input_cols = []
    for item in select_cols:
        if re.search(r'\s+as\s+', item, re.IGNORECASE):
            # Kolom heeft een alias
            col_name = re.split(r'\s+as\s+', item, flags=re.IGNORECASE)[1].strip()
        else:
            # Geen alias, neem de originele kolomnaam
            col_name = item.strip().split(' ')[0]
        input_cols.append(col_name)

    #Kolommen voor de pivot:
    cols_to_pivot = list(set(input_cols) - set(output_cols))
    # print("Categorie naar kolom: ", cols_to_pivot[0])
    # print("Waarden in kolom: ", cols_to_pivot[1])
    str_dbt_get_column_values = str_dbt_get_column_values.replace('<value_col>', cols_to_pivot[1])
    # print("Aangepaste dbt utils get column values statement:\n", str_dbt_get_column_values)
    #Kolommen voor de select en group by:
    cols_to_select = list(set(input_cols) & set(output_cols))
    select_col = ''
    for idx, col in enumerate(cols_to_select):
        if idx == len(cols_to_select) - 1:
            select_col += col
        else:
            select_col += col + ', '

    table = re.findall(pattern_5, cte_statement, re.IGNORECASE | re.DOTALL)

    if with_statement:
        cte_statement = with_statement + "\n ,cte1 AS (\n" + cte_statement + "\n)\n"
    else: 
        for t in table:
            cte_statement = cte_statement.replace(t[1], "{{ ref('" + t[1] + "') }}")
        cte_statement= "WITH cte1 AS (\n" + cte_statement + ")\n"

    # Remove trailing comma and space from select_col and group by
    select_col_clean = select_col.rstrip(', ').strip()
    # Build the actual dbt crosstab SQL statement
    dbt_sql = (
        f"{cte_statement}"
        f"SELECT {select_col_clean}\n"
        f", {str_dbt_get_column_values}\n"
        f"FROM cte1\n"
        f"GROUP BY {select_col_clean}"
    )
    return dbt_sql