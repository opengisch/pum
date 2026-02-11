# Parameters

Parameters allow you to make your changelogs and application hooks configurable. Instead of hard-coding values like coordinate systems, default texts, or feature flags, you declare named parameters in your `.pum.yaml` and reference them in your SQL and Python code.

## Configuration

Parameters are defined in the `parameters` section of your `.pum.yaml` [configuration file](configuration/configuration.md), using the [`ParameterDefinitionModel`](configuration/models/parameter_definition_model.md).

Each parameter accepts:

- **name** *(required)*: The parameter name, used as a placeholder in SQL and as a keyword argument in Python hooks.
- **type** *(optional, default: `text`)*: One of `boolean`, `integer`, `text`, `decimal`, `path`.
- **default** *(optional)*: A default value used for validation.
- **description** *(optional)*: A human-readable description.

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
```

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
