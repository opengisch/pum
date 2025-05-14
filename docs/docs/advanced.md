## Data and application isolation

We recommend to isolate data (tables) from business logic (e.g., views, triggers) into distinct schemas for easier upgrades.
This will facilitate the migrations but also the code management: you will not have to write diff files for views and triggers.

To achieve this you can organize the code in such a manner:
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


## Migration hooks

Migration hooks allow you to define actions to be executed before or after a migration. These hooks are defined in the `.pum.yaml` configuration file under the `migration_hooks` section.

There are two types of migration hooks:

- `pre`: Executed before the migration.
- `post`: Executed after the migration.

Hooks are defined as a list of files or plain SQL code to be executed. For example:

```yaml
migration_hooks:
  pre:
    - code: DROP VIEW IF EXISTS pum_test_app.some_view;

  post:
    - file: post/create_view.sql
```

Python hooks can also be defined in a Python module.
A method `run_hook` must be defined,
using a `psycopg.Connection` as argument.
It can have extra arguments that will be the parameters.
It should not commit the transaction or rollback in case of a future error would fail.

The configuration is then:

```yaml
migration_hooks:
  pre:
    - file: pre/drop_view.sql

  post:
    - file: post/create_schema.sql
    - file: post/create_view.py
```

With `post/create_view.py`:

```py
from pirogue.utils import select_columns
from psycopg import Connection
from pum.utils.execute_sql import execute_sql

def run_hook(connection: Connection):
    sql_code = """
    CREATE OR REPLACE VIEW pum_test_app.some_view AS
    SELECT {columns}
    FROM pum_test_data.some_table
    WHERE is_active = TRUE;
    """.format(
        columns=select_columns(
            pg_cur=connection.cursor(),
            table_schema="pum_test_data",
            table_name="some_table"  
        )
    )
    execute_sql(connection=connection, sql=sql_code, commit=False)
```
