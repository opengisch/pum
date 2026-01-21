usage: pum check [-h] [-i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} [{tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} ...]]
[-N EXCLUDE_SCHEMA] [-P EXCLUDE_FIELD_PATTERN] [-o OUTPUT_FILE] [-f {text,html,json}]
pg_connection_compared
### positional arguments:
- `pg_connection_compared`: PostgreSQL service name or connection string for the database to compare against
### options:
- `-h, --help`: show this help message and exit
- `-i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} [{tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} ...], --ignore {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} [{tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} ...]`: Elements to be ignored
- `-N EXCLUDE_SCHEMA, --exclude-schema EXCLUDE_SCHEMA`: Schema to be ignored.
- `-P EXCLUDE_FIELD_PATTERN, --exclude-field-pattern EXCLUDE_FIELD_PATTERN`: Fields to be ignored based on a pattern compatible with SQL LIKE.
- `-o OUTPUT_FILE, --output_file OUTPUT_FILE`: Output file
- `-f {text,html,json}, --format {text,html,json}`: Output format: text, html, or json. Default: text
