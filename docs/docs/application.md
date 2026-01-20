# Application

## Application/data isolation

Separating application logic from object (business) data is recommended. Place application-owned objects—such as views, functions, procedures, and triggers—into dedicated, application-specific schemas, while keeping persistent business tables in one or several separate data schemas.

This isolation makes application artifacts clearly distinct from long-lived data, so application schemas can be dropped and recreated on demand (for rebuilds, upgrades, or environment resets) without impacting the underlying object data.

Application hooks allow you to define actions to be executed during the migration process.
They allow the creation and removal of the application part of the data model.
These hooks are defined in the `.pum.yaml` configuration file under the `application` section.

There are two types of migration hooks:

- `drop`: Hooks to drop the application before applying migrations.
- `create`: Hooks to create the application after applying migrations.

## SQL hooks

Hooks are defined as a list of files or plain SQL code to be executed. For example:

```yaml
application:
  drop:
    - code: DROP VIEW IF EXISTS pum_test_app.some_view;

  create:
    - file: create_app/create_view.sql
```

## Python hooks

Python hooks can also be defined in a Python module.
A method `run_hook` must be defined,
using a `psycopg.Connection` as argument.
It can have extra arguments that will be the parameters.
It should not commit the transaction or rollback would fail in case of a later error.
You can use `pum.utils.execute_sql` to execute the SQL code without committing.

The configuration is then:

```yaml
application:
  drop:
    - file: drop_app/drop_view.sql

  create:
    - file: create_app/create_schema.sql
    - file: create_app/create_view.py
```

With `create_app/create_view.py`:

```py
from pirogue.utils import select_columns
from psycopg import Connection
from pum import HookBase

class Hook(HookBase):
  def run_hook(connection: Connection):
      columns=select_columns(
          pg_cur=connection.cursor(),
          table_schema="pum_test_data",
          table_name="some_table"  
      )
      sql_code = f"""
          CREATE OR REPLACE VIEW pum_test_app.some_view AS
          SELECT {columns}
          FROM pum_test_data.some_table
          WHERE is_active = TRUE;
      """
      self.execute(sql=sql_code)
```

or with extra parameters:

```py
from psycopg import Connection
from pum.utils.execute_sql import execute_sql

class Hook(HookBase):
    def run_hook(connection: Connection, srid: int):
        sql_code = """
            CREATE OR REPLACE VIEW pum_test_app.some_view AS
            SELECT ST_Tramsform(geometry, {srid})
            FROM pum_test_data.some_table;
        """
        self.execute(sql=sql_code)
```


> **Note:**  
> Local imports within the hook file are supported. The parent directory of the hook file is temporarily added to `sys.path` during execution, so you can use local imports in your hook scripts. Ensure your hook files and their dependencies are structured accordingly.


## Data and application isolation

We recommend to isolate data (tables) from business logic (e.g., views, triggers) into distinct schemas for easier upgrades.
This will facilitate the migrations but also the code management: you will not have to write diff files for views and triggers.
This is easily achievable thanks to migration hooks.

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
│   ├── drop_app.sql
│   └── create_app.sql
└── .pum.yaml
```
