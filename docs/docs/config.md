# Config

## Config file .pum.yaml

In the config file `.pum.yaml`, you can define, with the YAML syntax:

* `changelogs_directory`: the directory with the changelogs files.
* `pum_migrations_schema`: the schema where is stored the `pum_migrations` table.
* `parameters`: the definition of parameters for the migration.
* `migrations_hooks`: the `pre` and `post` migrations hooks.

For example:  
```yaml

pum_migrations_schema: my_sys

changelogs_directory: my_custom_directory

parameters:
  - name: SRID
    type: integer
    default: 2056
    description: Coordinate Reference System (CRS) to use for the data. This is used for the geometry column in the database. Default is 2056 (CH1903+ / LV95).

```  

## Migration hooks

Migration hooks allow you to define actions to be executed before or after a migration. These hooks are defined in the `.pum.yaml` configuration file under the `migration_hooks` section.

There are two types of migration hooks:

- `pre`: Executed before the migration.
- `post`: Executed after the migration.

Hooks are defined as a list of files to be executed. For example:

```yaml
migration_hooks:
  pre:
    - file: pre/drop_view.sql

  post:
    - file: post/create_view.sql
```
