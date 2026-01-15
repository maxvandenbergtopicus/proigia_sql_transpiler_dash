import re

sql_file = 'sql_input.sql' # Dit moet de INPUT voor de functie worden

# Patroon voor de queries die gebruikt moeten worden vanuit de crosstab
# Namelijk de SELECT die tussen de $$...$$ staan
pattern_1 = r'\$\$(.*?)\$\$'        # alles binnen $$...$$ pakken
pattern_2 = r'\)\s*as\s+(?:\w+\s*)?\(\s*(.*?)\s*\)' # alles pakken vanaf $$) as tot het einde van de regel
pattern_3 = r'SELECT(.*?)FROM'      # alles pakken tussen SELECT en FROM
pattern_4 = r'FROM(.*?)ORDER'       # alles pakken tussen FROM en ORDER
pattern_5 = r'\b(FROM|JOIN(?!\s+LATERAL)|LEFT\s+JOIN(?!\s+LATERAL)|RIGHT\s+JOIN(?!\s+LATERAL)|INNER\s+JOIN(?!\s+LATERAL)|OUTER\s+JOIN(?!\s+LATERAL)|FULL\s+JOIN(?!\s+LATERAL)|CROSS\s+JOIN(?!\s+LATERAL))\s+([a-zA-Z_][\w]*)\b(?!\s*\.|\s*\(|::)'

b_alles_okay = True

# Lees de SQL in uit het bestand
with open(sql_file, 'r') as file:
    sql_content = file.read()
file.close()

# Statements 0, 1, N zjn de verschillende queries binnen de $$...$$
# statements is een lijst
statements = re.findall(pattern_1, sql_content, re.DOTALL) #DOTALL zorgt ervoor dat ook nieuwe regels meegenomen worden
cte_statement = statements[0]           # De Basis query die de data levert
pivot_statement = statements[1]         # De query die levert welke categorie uit welke tabel gebruikt moet worden voor de pivot ==> vult de dbt_utils.get_column_values()

# Als in het tweede SELECT een JOIN zit dan moet die apart behandeld worden
if 'JOIN' in pivot_statement.upper():
    print('JOIN gevonden in de pivot statement, nog niet ondersteund')
    print('MOET APART WORDEN BEHANDELD')
    b_alles_okay = False
# Als in de eerste SELECT een WITH zit dan moet die apart behandeld worden
if 'WITH' in cte_statement.upper() or 'DISTINCT ON (' in cte_statement.upper():
    print('WITH of DISTINCT ON gevonden in de select statement, nog niet ondersteund')
    print('MOET APART WORDEN BEHANDELD')
    b_alles_okay = False



if b_alles_okay:
    pivot_col = re.search(pattern_3, statements[1], re.IGNORECASE | re.DOTALL).group(1).strip()
    from_statements = re.search(pattern_4, statements[1], re.IGNORECASE | re.DOTALL).group(1).strip()
    # print('Pivot column: ', pivot_col, 'From statements: ', from_statements)
    str_dbt_get_column_values = "{{ dbt_utils.pivot('<pivot_col>',\
        dbt_utils.get_column_values(ref('<input_model>'),'categorie',default=[]),\
        agg='',\
        then_value='<value_col>',\
        else_value=\"ARRAY_CONSTRUCT()\",\
        quote_identifiers=False)}} ".replace('<pivot_col>', pivot_col).replace('<input_model>', from_statements)
    # print("dbt utils get column values statement:\n", str_dbt_get_column_values)    

    ## Bepaal welke kolommen meegaan in de output
    statement_as = re.search(pattern_2, sql_content, re.IGNORECASE | re.DOTALL).group(1).strip().replace('(','').replace(';','').strip()
    # print('Statement AS: ', statement_as)
    output_cols = []
    for item in statement_as.split(','):
        output_cols.append(item.strip().split(' ')[0])
    # print('Output columns: ', output_cols)

    ## Selecteer de kolom-statements uit de cte_statement
    cte_select_statement = re.search(pattern_3, cte_statement, re.IGNORECASE | re.DOTALL).group(1).strip()
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

    # print('Input columns: ',input_cols)

    #Kolommen voor de pivot:
    cols_to_pivot = list(set(input_cols) - set(output_cols))
    # print("Categorie naar kolom: ", cols_to_pivot[0])
    # print("Waarden in kolom: ", cols_to_pivot[1])
    str_dbt_get_column_values = str_dbt_get_column_values.replace('<value_col>', cols_to_pivot[1])
    # print("Aangepaste dbt utils get column values statement:\n", str_dbt_get_column_values)
    #Kolommen voor de select en group by:
    cols_to_select = list(set(input_cols) & set(output_cols))
    select_col = ''
    for col in cols_to_select:
        select_col += col + ', '

    table = re.findall(pattern_5, cte_statement, re.IGNORECASE | re.DOTALL)

    for t in table:
        cte_statement = cte_statement.replace(t[1], "{{ ref('" + t[1] + "') }}")    
    
    eind_resultaat = "Select statement voor dbt crosstab: WITH cte1 AS (" + cte_statement + ")\n SELECT \n" + select_col \
          + "\n" + str_dbt_get_column_values + "\n FROM cte1 \n GROUP BY " + select_col
    
    print(eind_resultaat)
    
