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
