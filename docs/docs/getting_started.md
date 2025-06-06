# Getting Started

## Prerequisites
- Python 3.10 or newer
- PostgreSQL server (tested with 12+)
- pg_service.conf configured for your database connections (see [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-pgservice.html))

## Installation

```bash
pip install pum
```

## Changelogs

The changelogs directory in a PUM project is typically used to store SQL scripts or migration files that define incremental changes to your database schema.

```
project/
└── changelogs/
    ├── 1.0.0/
    │   ├── 01_create_schema.sql
    │   └── 02_create_tables.sql
    └── 1.0.1/
        ├── 01_rename_column.sql
        └── 02_do_something_else.sql
```



`changelogs` is the root directory for all migration scripts.
Each subfolder corresponds to a version of the datamodel.
Each file inside represents an upgrade step in a SQL file.
Files are usually named with a sequential number or timestamp and a short description.

The order of files determines the order of execution.
Each file contains SQL statements for a single migration step.

Changelog should not try to commit.

### Best Practices
* Keep each migration atomic—one logical change per file.
* Never modify a changelog file after it has been applied to any environment.
* Add new changes as new files.
* Store the changelogs directory in version control (e.g., Git).


## Installing the datamodel

Once the code is organized, the datamodel can be installed from the command line:

```sh
pum -s {pg_service} install
```

`pg_service` is the service to be used to perform the installation specifying an existing database.
