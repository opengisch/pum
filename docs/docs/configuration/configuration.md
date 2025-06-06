# Configuration file .pum.yaml

## Introduction

In the config file `.pum.yaml`, you can define, with the YAML syntax:

* `changelogs_directory`: the directory with the changelogs files.
* `parameters`: the definition of parameters for the migration.
* `migrations_hooks`: the `pre` and `post` migrations hooks.

For example:  
```yaml

changelogs_directory: my_custom_directory

parameters:
  - name: SRID
    type: integer
    default: 2056
    description: Coordinate Reference System (CRS) to use for the data. This is used for the geometry column in the database. Default is 2056 (CH1903+ / LV95).

```  

## Complete documentation

For detailed configuration options, see the [Configuration Model](configuration/config_model.md).
