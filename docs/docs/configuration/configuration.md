# Configuration file .pum.yaml

## Introduction

In the config file `.pum.yaml`, you can define, with the YAML syntax:

* `changelogs_directory`: the directory with the changelogs files.
* `parameters`: the definition of parameters for the migration.
* `application`: the `drop` and `create` application hooks.

For example:  
```yaml

pum:
  module: my_module

changelogs_directory: my_custom_directory

parameters:
  - name: SRID
    type: integer
    default: 2056
    description: Coordinate Reference System (CRS) to use for the data. This is used for the geometry column in the database. Default is 2056 (CH1903+ / LV95).

app:
  drop:
    - code: DROP SCHEMA my_app CASCADE;

  create:
    - file: app/create.py

```  

## Complete documentation

For detailed configuration options, see the [Configuration Model](models/config_model).
