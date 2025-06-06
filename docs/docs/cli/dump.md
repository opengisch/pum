usage: create_cli_help.py dump [-h] [-f {DumpFormat.CUSTOM,DumpFormat.PLAIN}] [-N EXCLUDE_SCHEMA] file

### positional arguments:
- `file`: The backup file

### options:
- `-h, --help`: show this help message and exit
- `-f {DumpFormat.CUSTOM,DumpFormat.PLAIN}, --format {DumpFormat.CUSTOM,DumpFormat.PLAIN}`
- `Dump format. Choices: ['custom', 'plain']. Default: plain.`
- `-N EXCLUDE_SCHEMA, --exclude-schema EXCLUDE_SCHEMA`
- `Schema to be ignored.`
