usage: update_cli_docs.py [-h] [-c CONFIG_FILE] -s PG_SERVICE [-d DIR] [-v] [--version]
{info,install,upgrade,role,check,dump,restore,baseline} ...
### options:
- `-h, --help`: show this help message and exit
- `-c CONFIG_FILE, --config_file CONFIG_FILE`: set the config file. Default: .pum.yaml
- `-s PG_SERVICE, --pg-service PG_SERVICE`: Name of the postgres service
- `-d DIR, --dir DIR`: Directory or URL of the module. Default: .
- `-v, --verbose`: Increase output verbosity (e.g. -v, -vv)
- `--version`: Show program's version number and exit.
### commands:
valid pum commands
{info,install,upgrade,role,check,dump,restore,baseline}
- `info`: show info about schema migrations history.
- `install`: Installs the module.
- `upgrade`: Upgrade the database.
- `role`: manage roles in the database
- `check`: check the differences between two databases
- `dump`: dump a Postgres database
- `restore`: restore a Postgres database from a dump file
- `baseline`: Create upgrade information table and set baseline
