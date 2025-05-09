
# Getting Started

## Installation
  ```sh title="install PUM"
  pip install pum
  ```

## Code organization

### Schema separation

We recommend to isolate data (tables) from business logic (e.g., views, triggers) into distinct schemas for easier upgrades.



```
project/
├── changelogs/
│   ├── 1.0.0/
│   │   ├── 01_create_schema.sql
│   │   └── 02_create_tables.sql
│   ├── 1.0.1/
│   │   ├── 01_rename_column.sql
│   │   └── 02_do_something_else.sql
├── app/
│   └── create_views_and_triggers.sql
└── .pum.yaml
```

### Changelogs

  The changelogs directory in a PUM project is typically used to store SQL scripts or migration files that define incremental changes to your database schema.

`changelogs` is the root directory for all migration scripts.
Each subfolder corresponds to a version of the datamodel.
Each file inside represents an upgrade step in a SQL file.
Files are usually named with a sequential number or timestamp and a short description.

The order of files determines the order of execution.
Each file contains SQL statements for a single migration step.

### Best Practices
* Keep each migration atomic—one logical change per file.
* Never modify a changelog file after it has been applied to any environment.
* Add new changes as new files.
* Store the changelogs directory in version control (e.g., Git).


3. **Apply Migrations**:
  Use the `upgrade` command to apply SQL delta files and keep your database up-to-date.
