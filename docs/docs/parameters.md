# Parameters

Parameters allow you to make your changelogs and application hooks configurable. Instead of hard-coding values like coordinate systems, default texts, or feature flags, you declare named parameters in your `.pum.yaml` and reference them in your SQL and Python code.

## Configuration

Parameters are defined in the `parameters` section of your `.pum.yaml` [configuration file](configuration/configuration.md), using the [`ParameterDefinitionModel`](configuration/models/parameter_definition_model.md).

Each parameter accepts:

- **name** *(required)*: The parameter name, used as a placeholder in SQL and as a keyword argument in Python hooks.
- **type** *(optional, default: `text`)*: One of `boolean`, `integer`, `text`, `decimal`, `path`.
- **default** *(optional)*: A default value used for validation.
- **description** *(optional)*: A human-readable description.
- **values** *(optional)*: A list of allowed values. When set, the parameter value (including the default) must be one of the listed entries. PUM validates this both at configuration load time and when values are passed from the CLI.
- **app_only** *(optional, default: `false`)*: If `true`, the parameter can be changed freely when recreating the app. Standard parameters (`app_only: false`) must keep the same value across the entire module lifecycle (install, upgrade, create-app).

```yaml
parameters:
  - name: SRID
    type: integer
    default: 2056
    description: Coordinate Reference System to use for geometry columns.

  - name: default_text_value
    type: text
    default: hi there

  - name: my_flag
    type: boolean
    default: true

  - name: view_comment
    type: text
    default: my comment
    description: Comment used when creating application views.
    app_only: true

  - name: lang_code
    type: text
    default: en
    description: Language for the application.
    values: [en, de, fr, it]
    app_only: true
```

### Restricting parameter values

Use the `values` field to restrict a parameter to a fixed set of allowed entries:

```yaml
parameters:
  - name: lang_code
    type: text
    default: en
    values: [en, de, fr, it]

  - name: SRID
    type: integer
    default: 2056
    values: [2056, 4326, 21781]
```

PUM validates allowed values at two points:

1. **Configuration load** — the `default` must be in `values` (if both are set).
2. **CLI** — when a user passes `--parameter lang_code es`, PUM rejects the value with a clear error message.

### Standard vs. app-only parameters

PUM stores the parameter values used during each migration in the `pum_migrations` table.
On every subsequent **upgrade**, **create app**, or **recreate app** call, PUM compares the
provided parameter values against the stored ones:

- **Standard parameters** (`app_only: false`, the default) must remain unchanged. If a
  different value is provided, PUM raises an error. This protects structural values
  (e.g. an SRID) that would cause data inconsistencies if changed after installation.
- **App-only parameters** (`app_only: true`) are allowed to change between calls. Use
  this for values that only affect application-level objects such as views or comments,
  which are dropped and recreated anyway.

## Passing values from the CLI

Parameter values are provided at runtime via the `-p` / `--parameter` flag, which takes a name and a value:

```bash
pum upgrade -p SRID 2056 -p default_text_value "hello world"
```

## Using parameters in SQL

In SQL files (changelogs or hooks), reference parameters using curly-brace placeholders `{parameter_name}`. PUM substitutes them as properly escaped SQL literals via psycopg.

```sql
CREATE TABLE myschema.points (
    id INT PRIMARY KEY,
    geom geometry(Point, {SRID})
);

CREATE TABLE myschema.items (
    id INT PRIMARY KEY,
    label TEXT NOT NULL DEFAULT {default_text_value}
);
```

With `SRID=2056` and `default_text_value="hello world"`, this produces:

```sql
CREATE TABLE myschema.points (
    id INT PRIMARY KEY,
    geom geometry(Point, 2056)
);

CREATE TABLE myschema.items (
    id INT PRIMARY KEY,
    label TEXT NOT NULL DEFAULT 'hello world'
);
```

## Using parameters in Python hooks

In Python hooks, parameters can be accessed in two ways:

### As keyword arguments

Declare parameters by name in your `run_hook()` method signature. PUM will pass matching values automatically:

```python
from pum import HookBase

class Hook(HookBase):
    def run_hook(self, connection, SRID: int = None, my_flag: bool = None):
        if my_flag:
            connection.execute(f"SELECT ST_SetSRID(geom, {SRID}) FROM myschema.points")
```

### Via `self.execute()` for SQL within Python

When executing SQL inside a Python hook with `self.execute()`, curly-brace placeholders are substituted automatically from the full parameters dict — no need to declare them as arguments:

```python
from pum import HookBase

class Hook(HookBase):
    def run_hook(self, connection) -> None:
        self.execute(sql="""
            COMMENT ON TABLE myschema.points IS {default_text_value};
        """)
```
