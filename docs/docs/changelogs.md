# Changelogs

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

## Application-only releases

A release may contain no database migration at all (e.g., only application code changed).
In that case, create the version directory without any SQL file and add an empty marker file named `APP_ONLY_RELEASE` (this also ensures the directory is tracked in Git):

```
project/
└── changelogs/
    └── 1.0.2/
        └── APP_ONLY_RELEASE
```

The version is still recorded in the migration table when installing or upgrading, but no SQL is executed.
A version directory containing both the marker file and SQL files is invalid.

## Best Practices
* Keep each migration atomic—one logical change per file.
* Never modify a changelog file after it has been applied to any environment.
* Add new changes as new files.
* Store the changelogs directory in version control (e.g., Git).
