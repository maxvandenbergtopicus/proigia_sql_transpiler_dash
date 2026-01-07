# Proigia SQL Transpiler for dbt

A Python tool that converts Proigia `.pry` files (PostgreSQL dialect) to dbt models (Snowflake dialect).

## Features

- Converts PostgreSQL SQL to Snowflake SQL dialect
- Generates dbt models with proper configuration blocks
- Handles SQL blocks and macros
- Converts crosstab functions to dbt-compatible SQL
- Translates `unnest(ARRAY[...])` to `VALUES` clauses
- Converts `generate_series()` to Snowflake `GENERATOR()` 
- Replaces table references with dbt `ref()` or source macros
- Handles external variables (`${varname}`) conversion to dbt variables
- Logs output to both console and file

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.yaml` to configure your settings:

### dbt Output Paths
```yaml
dbt_output_path: C:\path\to\dbt\models\output
dbt_macro_path: C:\path\to\dbt\macros\output
proigia_defintion_path: C:\path\to\proigia_definition
```

### Ignored Keywords
Files containing these keywords in their names will be skipped during processing:
```yaml
ignored_keywords:
  - aggregate
  - column_properties
```

## Usage

Run the transpiler:
```bash
python main.py
```

The tool will:
1. Read input and output paths from `config.yaml`
2. Search recursively for all `.pry` files in `proigia_defintion_path`
2. Skip files with ignored keywords
3. Process block files first (from `blocks/` folders) and create macros
4. Process regular files and generate dbt models

### Output

- **dbt Models**: SQL files with dbt configuration blocks
- **Macros**: Block files are converted to dbt macros
- **Logs**: Execution logs are saved to `logs/main.log`

## Variables

The transpiler automatically handles:

### Built-in Variables
- `praktijk_agb`: Always added as a dbt variable
- External variables like `${varname}` are:
  - Extracted from SQL
  - Added to dbt variable declarations
  - Replaced with `{{ var('varname', none) }}` in SQL

### External Tables
The following tables are treated as external sources and referenced as `STG.P{{praktijk_agb}}.{table}`:
- allergie
- bepaling
- contact
- contraindicatie
- episode
- journaal
- journaalregel
- medewerker
- medicatie
- metadata
- origineel
- patient
- praktijk
- ruiter
- verrichting
- verwijzing
- override_patientenlijst
- functie
- medewerker_hisnaam

## SQL Transformations

### Dialect Conversion
- PostgreSQL → Snowflake syntax
- Interval casts
- Array operations
- Type conversions

### Crosstab Functions
- Crosstab blocks are converted to dbt-compatible SQL when possible
- Unsupported patterns (WITH, DISTINCT ON, JOIN in pivot) are skipped
- Comments are removed before processing

### Table References
- Tables are replaced with `{{ ref('table_name') }}`
- External tables use `STG.P{{praktijk_agb}}.table`
- CTEs are preserved and not converted
- LATERAL joins are left untouched

### Generate Series
Converts PostgreSQL `generate_series()` to Snowflake `GENERATOR()`:
```sql
-- Before
FROM generate_series(start, end, step) AS s(a)

-- After
FROM TABLE(GENERATOR(ROWCOUNT => ...)) AS g, 
LATERAL (SELECT DATEADD(...) AS a) AS s
```

### Unnest Array
Converts `unnest(ARRAY[...])` to VALUES clause:
```sql
-- Before
SELECT unnest(ARRAY[1,2,3]) AS num

-- After
SELECT num FROM (VALUES (1), (2), (3)) AS t(num)
```

## Project Structure

```
proigia_sql_transpiler_dash/
├── main.py                 # Entry point
├── config.yaml             # Configuration file
├── requirements.txt        # Python dependencies
├── code/
│   └── functions/
│       ├── dbt_wrapper.py      # dbt model generation
│       ├── dialect_converter.py # SQL dialect conversion
│       ├── crosstabs.py        # Crosstab handling
│       └── general.py          # Utility functions
├── logs/
│   └── main.log           # Execution logs
└── README.md              # This file
```

## Error Handling

- Errors are logged to both console and `logs/main.log`
- Failed file processing does not stop the entire run
- Unsupported crosstab patterns are safely skipped
- Invalid SQL is preserved when conversion fails

## Logging

All output is logged to:
- **Console**: Real-time progress
- **File**: `logs/main.log` (recreated on each run)

## Requirements

- Python 3.8+
- sqlglot
- PyYAML
- pathlib

See `requirements.txt` for complete list.

## Notes

- The tool assumes input files are in PostgreSQL dialect
- Output is optimized for Snowflake dialect
- Block files (in `blocks/` folders) must be processed before regular files
- CTEs within crosstab blocks are not fully supported yet
