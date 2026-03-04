# Database Management

PUM provides utilities for creating and dropping PostgreSQL databases programmatically or via the command line.

## How It Works

The database management functions provide safe wrappers around PostgreSQL's `CREATE DATABASE` and `DROP DATABASE` commands.

### Create Database

1. **Connection**: Connects to a PostgreSQL server (typically to the `postgres` database)
2. **Template Support**: Optionally uses an existing database as a template for duplication
3. **Database Creation**: Executes `CREATE DATABASE` with proper SQL identifiers
4. **Logging**: Reports progress and success

### Drop Database

1. **Connection Termination**: First terminates all active connections to the target database
2. **Safety Check**: When using the CLI, prompts for confirmation (unless `--force` is used)
3. **Database Deletion**: Executes `DROP DATABASE` with proper SQL identifiers
4. **Logging**: Reports progress and success

## Safety Features

- **Identifier Escaping**: All database names are properly escaped using PostgreSQL identifiers to prevent SQL injection
- **Connection Termination**: The drop operation automatically terminates active connections before attempting to drop the database
- **Confirmation Prompts**: When using the CLI, the drop command requires confirmation unless the `--force` flag is used
- **Error Handling**: Provides clear error messages if operations fail

## Command-Line Usage

For detailed command-line options and examples, see the [db command documentation](../cli/db.md).

### Create a Database

```bash
# Create a new database
pum -p my_service db create my_new_database

# Create a database from a template (duplicate an existing database)
pum -p my_service db create my_copy_database --template my_existing_database
```

### Drop a Database

```bash
# Drop a database (with confirmation prompt)
pum -p my_service db drop my_old_database

# Drop a database without confirmation
pum -p my_service db drop my_old_database --force
```

## Programmatic Usage

You can also use the database management functions directly in your Python code.

### Import the Functions

```python
from pum import create_database, drop_database
```

### Create a Database

```python
# Basic database creation
connection_params = {"service": "my_service", "dbname": "postgres"}
create_database(connection_params, "my_new_database")

# Create from template
create_database(
    connection_params,
    "my_copy_database",
    template="my_existing_database"
)
```

### Drop a Database

```python
# Drop a database
connection_params = {"service": "my_service", "dbname": "postgres"}
drop_database(connection_params, "my_old_database")
```

## Use Cases

### Database Duplication

Create an exact copy of a database for testing or development:

```bash
pum -p production_service db create test_database --template production_database
```

### Test Database Management

Programmatically create and clean up test databases:

```python
import psycopg
from pum import create_database, drop_database

# In setUp
connection_params = {"service": "test_service", "dbname": "postgres"}
create_database(connection_params, "test_db")

# Run tests...

# In tearDown
drop_database(connection_params, "test_db")
```

### Environment Setup

Quickly create new databases for different environments:

```bash
pum -p my_service db create dev_database
pum -p my_service db create staging_database
pum -p my_service db create test_database
```

## API Reference

For detailed API documentation, see:

::: pum.database.create_database
    options:
      show_root_heading: true
      show_source: true

::: pum.database.drop_database
    options:
      show_root_heading: true
      show_source: true

## Notes

- **Privileges**: You need appropriate PostgreSQL privileges to create and drop databases (typically `CREATEDB` role attribute)
- **Connection Target**: When creating or dropping a database, you must be connected to a **different** database (not the one you're creating/dropping)
- **Template Database**: When using a template, the template database must exist and must not have any active connections
- **Active Connections**: The drop operation will forcefully terminate all active connections to the target database before dropping it
