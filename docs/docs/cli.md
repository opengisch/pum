usage: pum [-h] [-c CONFIG_FILE] -p PG_CONNECTION [-d DIR] [-v] [-q] [--version] {info,install,upgrade,role,check,dump,restore,baseline,uninstall} ...
### options:
- `-h, --help`: show this help message and exit
- `-c CONFIG_FILE, --config_file CONFIG_FILE`: set the config file. Default: .pum.yaml
- `-p PG_CONNECTION, --pg-connection PG_CONNECTION`: PostgreSQL service name or connection string (e.g., 'mydb' or 'postgresql://user:pass@host/db')
- `-d DIR, --dir DIR`: Directory or URL of the module. Default: .
- `-v, --verbose`: Increase verbosity (-v for DEBUG, -vv for SQL statements)
- `-q, --quiet`: Suppress info messages, only show warnings and errors
- `--version`: Show program's version number and exit.
### commands:
valid pum commands
{info,install,upgrade,role,check,dump,restore,baseline,uninstall}
- `info`: show info about schema migrations history.
- `install`: Installs the module.
- `upgrade`: Upgrade the database.
- `role`: manage roles in the database
- `check`: check the differences between two databases
- `dump`: dump a Postgres database
- `restore`: restore a Postgres database from a dump file
- `baseline`: Create upgrade information table and set baseline
- `uninstall`: Uninstall the module by executing uninstall hooks
