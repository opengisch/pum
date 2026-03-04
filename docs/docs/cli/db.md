usage: pum db [-h] [--template TEMPLATE] [--force] {create,drop} dbname
### positional arguments:
- `{create,drop}`: Action to perform: create (create a new database), drop (drop an existing database)
- `dbname`: Name of the database to create or drop
### options:
- `-h, --help`: show this help message and exit
- `--template TEMPLATE`: Template database to use when creating (for duplicating an existing database)
- `--force`: Skip confirmation prompt for drop action

## Examples

### Create a new database
```bash
pum -p my_service db create my_new_database
```

### Create a database from a template
```bash
# Create a database using an existing database as a template
pum -p my_service db create my_copy_database --template my_existing_database
```

### Drop a database (with confirmation)
```bash
# You will be prompted to confirm
pum -p my_service db drop my_old_database
```

### Drop a database (skip confirmation)
```bash
# Skip the confirmation prompt with --force
pum -p my_service db drop my_old_database --force
```

## Notes

- The `create` action creates a new PostgreSQL database.
- When using `--template`, you can duplicate an existing database including its schema and data.
- The `drop` action will terminate active connections to the database before dropping it.
- The `drop` action requires confirmation unless the `--force` flag is used.
- Both actions require appropriate PostgreSQL privileges.
- The `-p` connection should point to a database other than the one you're creating/dropping (typically `postgres`).
