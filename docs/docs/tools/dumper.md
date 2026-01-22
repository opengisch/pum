# Database Dump & Restore

The PUM Dumper provides convenient wrappers around PostgreSQL's `pg_dump` and `pg_restore` utilities for backing up and restoring databases.

## How It Works

The Dumper is a thin wrapper around PostgreSQL's native backup utilities, configured with PUM's connection settings and conventions. It ensures consistent backup creation and restoration with appropriate flags for schema management.

### Dump Process

1. **Connection**: Uses PostgreSQL service names or connection strings from your configuration
2. **Schema Exclusion**: Optionally excludes specific schemas from the backup
3. **Format Selection**: Supports both custom (compressed binary) and plain (SQL text) formats
4. **Backup Creation**: Executes `pg_dump` with standardized flags (no owner, no privileges)
5. **File Output**: Saves the backup to your specified file path

### Restore Process

1. **Connection**: Connects to the target database using service names or connection strings
2. **Schema Exclusion**: Optionally excludes specific schemas from restoration
3. **Error Handling**: Can ignore restoration errors if needed (useful for partial restores)
4. **Database Restoration**: Executes `pg_restore` with standardized flags (no owner)

## Backup Formats

PUM supports two backup formats:

- **Custom Format (Binary)**: Compressed, efficient format (default and recommended)
- **Plain Format (SQL Text)**: Human-readable SQL statements

## Usage

For detailed command-line options and examples, see:

- [dump command documentation](../cli/dump.md)
- [restore command documentation](../cli/restore.md)

### Basic Dump Example

```bash
# Dump database to custom format (compressed)
pum -s my_database dump backup.dump

# Dump to plain SQL format
pum -s my_database dump backup.sql -f plain
```

### Basic Restore Example

```bash
# Restore from custom format dump
pum -s target_database restore backup.dump

# Restore ignoring errors (useful for partial restores)
pum -s target_database restore backup.dump -x
```

## Database Ownership and Privileges

PUM's dump and restore operations intentionally exclude ownership and privilege information:

- **`--no-owner`**: Objects are created without specific ownership
- **`--no-privileges`**: Access permissions are not included

This design choice ensures:

- **Portability**: Dumps can be restored to different environments with different users
- **Flexibility**: You can manage permissions separately through PUM's role management
- **Consistency**: Works with PUM's role-based permission system

If you need to manage permissions, use PUM's [role management](../roles.md) features in conjunction with dump/restore operations.

## Error Handling

### Dump Errors

If `pg_dump` fails, PUM raises a `PgDumpFailed` exception with the error details. Common causes:

- Database connection issues
- Insufficient permissions
- Disk space problems
- Invalid schema names

### Restore Errors

If `pg_restore` fails, PUM raises a `PgRestoreFailed` exception. You can use the `-x` flag to continue despite errors, useful when:

- Restoring partial data
- Dealing with version differences
- Expected conflicts with existing data
