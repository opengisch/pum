usage: pum db [-h] [--template TEMPLATE] [--force] {create,drop} dbname
### positional arguments:
- `{create,drop}`: Action to perform: create (create a new database), drop (drop an existing database)
- `dbname`: Name of the database to create or drop
### options:
- `-h, --help`: show this help message and exit
- `--template TEMPLATE`: Template database to use when creating (for duplicating an existing database)
- `--force`: Skip confirmation prompt for drop action
