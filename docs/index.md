** - PostgreSQL Upgrades Manager (PUM)**

PUM is a robust database migration management tool designed to streamline the process of managing PostgreSQL database upgrades. Inspired by tools like FlywayDB and Liquibase, PUM leverages metadata tables to ensure seamless database versioning and migration.

### Key Features

- **Command-line and Python Integration**: Use PUM as a standalone CLI tool or integrate it into your Python projects.
- **Database Versioning**: Automatically manage database versioning with metadata tables.
- **Database Comparison**: Compare two databases to identify differences in tables, columns, constraints, and more.
- **Backup and Restore**: Create and restore database backups with ease.
- **Changelog Management**: Apply and track SQL delta files for database upgrades.

### Why PUM?

Managing database migrations in a Version Control System (VCS) can be challenging, especially for production databases. PUM simplifies this process by embedding version metadata directly into the database, enabling efficient tracking and application of migrations.

### Getting Started

1. **Install PUM**:
  ```sh
  pip install pum
  ```
  Ensure you have Python 3 and PostgreSQL utilities (`pg_restore` and `pg_dump`) installed.

2. **Baseline Your Database**:
  Use the `baseline` command to initialize metadata in your database.

3. **Apply Migrations**:
  Use the `upgrade` command to apply SQL delta files and keep your database up-to-date.

### Best Practices

- **Separate Schemas**: Isolate data from business logic (e.g., views, triggers) into distinct schemas for easier upgrades.
- **Read More**: Explore articles like [Get Your Database Under Version Control](https://blog.codinghorror.com/get-your-database-under-version-control/) for insights into database versioning.

### Learn More

- [Command Line Usage](#command-line)
- [Delta Files](#delta-files)
- [Configuration](#config-file)

PUM was developed to address challenges in the [TEKSI](https://github.com/TESKI) project, an open-source GIS for network management based on [QGIS](http://qgis.org/fr/site/).




## Command line

### pum

The usage of the pum command is:
```commandline
usage: pum [-h] [-v] [-c CONFIG_FILE]
       {check,dump,restore,baseline,info,upgrade,test}
       ...

optional arguments:
  -h, --help            Show this help message and exit.
  -v, --version         Print the version and exit.
  -c CONFIG_FILE, --config_file CONFIG_FILE
            Specify the configuration file.

commands:
  {check,dump,restore,baseline,info,upgrade,test}
  check               Compare two databases and display differences.
  dump                Create a backup of a PostgreSQL database.
  restore             Restore a PostgreSQL database from a backup file.
  baseline            Initialize upgrade information table and set baseline.
  info                Display the status of applied or pending upgrades.
  upgrade             Apply SQL delta files to upgrade the database.
  test                Run tests to validate database migrations.
```

Pum is using [postgres connection service file](https://www.postgresql.org/docs/current/static/libpq-pgservice.html) to define the database connection parameters.

### check

The `check` command compares 2 databases and shows the
differences. It compares the following elements and tells if they are different:

- tables
- columns
- constraints
- views
- sequences
- indexes
- triggers
- functions
- rules

It's possible to ignore one or more of these elements.

The usage of the `check` command is:

```commandline
usage: pum check [-h] -p1 PG_SERVICE1 -p2 PG_SERVICE2 [-v LEVEL]
                 [-i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}]

optional arguments:
  -h, --help            show this help message and exit
  -p1 PG_SERVICE1, --pg_service1 PG_SERVICE1
                        Name of the first postgres service
  -p2 PG_SERVICE2, --pg_service2 PG_SERVICE2
                        Name of the second postgres service
  -i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}, --ignore {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}
                        Elements to be ignored
  -N SCHEMA [SCHEMA...], --exclude-schema SCHEMA [SCHEMA...]
                        Schema to be skipped.
  -P PATTERN, --exclude-field-pattern PATTERN
                        A field pattern which should be ignored in column checking
  -v VERBOSE_LEVEL, --verbose_level VERBOSE_LEVEL
                        Verbose level (0, 1 or 2)
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        Output file
```

For example if we want to check if a database connected to the [postgres service](https://www.postgresql.org/docs/current/static/libpq-pgservice.html) `pg_service1` is equal to the
database connected to the postgres service `pg_service_2`, we can do the following command:

```commandline
pum check -p1 pg_service1 -p2 pg_service2
```

If we want to run the same command but ignoring the different views and triggers, we do:

```commandline
pum check -p1 pg_service1 -p2 pg_service2 -i views triggers
```

### dump
The `dump` command is used to create a dump (backup) of a postgres db.

The usage of the command is:

```commandline
usage: pum dump [-h] -p PG_SERVICE file

positional arguments:
  file                  The backup file

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -N SCHEMA [SCHEMA...], --exclude-schema SCHEMA [SCHEMA...]
                        Schema to be skipped.
```

For example, the command to backup the database connected to the [postgres service](https://www.postgresql.org/docs/current/static/libpq-pgservice.html) `pg_service1` is into the file
`/tmp/bak`:

```commandline

pum dump -p pg_service1 /tmp/bak
```

### restore

The `restore` command is used to restore a backup of a postgres db.

The usage is similar to the `dump` command:

```commandline
usage: pum restore [-h] -p PG_SERVICE file

positional arguments:
  file                  The backup file

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -N SCHEMA [SCHEMA...], --exclude-schema SCHEMA [SCHEMA...]
                        Schema to be skipped.
                        When used, this shall be followed by `--` to prevent
                        argument parsing error (file eing grabbed as a schema).
  -x                    ignore pg_restore errors
```

If we want to restore the backup from the `/tmp/bak` into the database connected to the [postgres service](https://www.postgresql.org/docs/current/static/libpq-pgservice.html) `pg_service2`:

```commandline
pum restore -p pg_service2 /tmp/bak
```

### upgrade

The `upgrade` command is used to upgrade an existing database using sql delta files. The command applies
one or more delta files to an existing database and stores in a table the information about the applied
deltas. Only the delta files with version greater or equal than the current version are applied.

The usage of the command is:

```commandline
usage: pum upgrade [-h] -p PG_SERVICE -t TABLE -d DIR [DIR ...]
                   [-u MAX_VERSION] [-v VAR VAR VAR] [-vv]

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR [DIR ...], --dir DIR [DIR ...]
                        Delta directories (space-separated)
  -u MAX_VERSION, --max-version MAX_VERSION
                        upper bound limit version
  -v VAR VAR VAR, --var VAR VAR VAR
                        Assign variable for running SQL deltas.Format is:
                        (string|float|int) name value.
  -vv, --verbose        Display extra information
```

### info
The `info` command prints the status of the already or not applied delta files.

The usage of the command is:

```commandline
usage: pum info [-h] -p PG_SERVICE -t TABLE -d DIR [DIR ...]

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR [DIR ...], --dir DIR [DIR ...]
                        Delta directories (space-separated)
```

### baseline

The `baseline` command creates the upgrades information table and sets the current version.

The usage of the command is:

```commandline
usage: pum baseline [-h] -p PG_SERVICE -t TABLE -d DIR [DIR ...] -b BASELINE

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR [DIR ...], --dir DIR [DIR ...]
                        Delta directories (space-separated)
  -b BASELINE, --baseline BASELINE
                        Set baseline  in the format x.x.x
```

The baseline argument receives the version number to be set in the upgrades information table. The version must match
with the `^\d+\.\d+\.\d+$` regular expression, i.e. must be in the format x.x.x

## Delta files

A delta file must be a SQL file containing one or more SQL statements.

There are 5 kind of delta files:
- **pre-all**, is executed first thing in an upgrade command. The file must have the name `pre-all.sql`.
The pre-all files doesn't have a version because they are executed for all upgrade
command regardless of the current db version.
- **pre delta**, is executed before the normal delta file. The file's name must be in the form
`delta_x.x.x_deltaname.pre.py` or `delta_x.x.x_deltaname.pre.sql`
- **delta**, this is the normal delta file. The file's name must be in the form
`delta_x.x.x_deltaname.py` or `delta_x.x.x_deltaname.sql`. The name of the file is used as an unique
key to identify the delta. This means that it must be unique (including among separate delta directories)
otherwise only one of them will be applied (which could likely be used for overriding a specific delta, but
this is currently not tested/supported).
- **post delta**, is executed after the normal delta file. The file's name must be in the form
`delta_x.x.x_deltaname.post.py` or `delta_x.x.x_deltaname.post.sql`
- **post all**, is executed last thing in an upgrade command. The file must have the name `post-all.sql`.
The pre-all files doesn't have a version because they are executed for all upgrade
command regardless of the current db version.

A Python file is executed before the sql file with the same kind and version.

In summary the upgrade workflow is:

```text
execute pre-all.sql if exists

for each file delta_x.x.x_deltaname.* ordered by version number:
    execute delta_x.x.x_deltaname.pre.sql if exists

    execute delta_x.x.x_deltaname.sql if exists

    execute delta_x.x.x_deltaname.post.sql if exists

execute post-all.sql if exists
```

### SQL deltas

If SQL code contains `%` characater, it must be escaped by using `%%`.

If variables are used, it follows `psycopg` rules to [pass Parameters to SQL queries](http://initd.org/psycopg/docs/usage.html#query-parameters).
For example, a string variable named SRID, will be called in a SQL delta as `%(SRID)s`.


## Config file

In the config file db_manager_config.yaml, you have to define, with the YAML syntax:
- **upgrades_table**: the name (and schema) of the table with the migration information
- **delta_dir**: the directory with the delta files.
- **backup_file**: the temporary db dump file used to copy the prod db to a test db
- **ignore_elements**: list of elements to ignore in db compare. Valid elements: tables, columns,
constraints, views, sequences, indexes, triggers, functions or rules
- **pg_dump_exe**: the command to run pg_dump, needs to be adjusted if the executable is not in your path
- **pg_restore_exe**: the command to run pg_restore, needs to be adjusted if the executable is not in your path

For example:  
```yaml

upgrades_table: qwat_sys.upgrades
delta_dir: ../update/delta/
backup_file: /tmp/backup.dump
ignore_elements:
  - columns
  - constraints
  - views
  - sequences
  - indexes
  - triggers
  - functions
  - rules
pg_dump_exe: pg_dump
pg_restore_exe: pg_restore
```  

On Windows, pg_dump and pg_restore aren't on your path by default, so the last two lines would look like this (adjust path to the installation path of PostreSQL) :
```yaml
pg_dump_exe: C:\Program Files\PostgreSQL\9.3\bin\pg_dump.exe
pg_restore_exe: C:\Program Files\PostgreSQL\9.3\bin\pg_restore.exe
```
