# pum
Postgres Upgrades Manager

## Features
Pum is python program that can be used via command line or directly from another python program.

Pum permits the followings operations on Postgres databases:

- check the differencese between two databases
- create a backup (dump file) of a database
- restore a database from a backup
- upgrade a database applying delta files

and some other useful operations.

## Command line 

### pum

The usage of the pun command is:
```
usage: pum.py [-h] [-v] [-c CONFIG_FILE]
              {check,dump,restore,baseline,info,upgrade,test-and-upgrade,test}
              ...

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         print the version and exit
  -c CONFIG_FILE, --config_file CONFIG_FILE
                        set the config file

commands:
  valid pum commands

  {check,dump,restore,baseline,info,upgrade,test-and-upgrade,test}
    check               check the differences between two databases
    dump                dump a Postgres database
    restore             restore a Postgres database from a dump file
    baseline            Create upgrade information table and set baseline
    info                show info about upgrades
    upgrade             upgrade db
    test-and-upgrade    try the upgrade on a test db and if all it's ok, do upgrade
                        the production db

```
### check

The `check` command compares 2 databases and shows the
differences. It compares the following elements and tells if they are diffents:

- tables
- columns
- constraints
- views
- sequences
- indexes
- triggers
- functions
- rules

Its' possible to ignore one or more of these elements.

The usage of the `check` command is:

```
usage: pum.py check [-h] -p1 PG_SERVICE1 -p2 PG_SERVICE2 [-s SILENT]
                    [-i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}]

optional arguments:
  -h, --help            show this help message and exit
  -p1 PG_SERVICE1, --pg_service1 PG_SERVICE1
                        Name of the first postgres service
  -p2 PG_SERVICE2, --pg_service2 PG_SERVICE2
                        Name of the second postgres service
  -s SILENT, --silent SILENT
                        Don't print lines with differences
  -i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}, --ignore {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}
                        Elements to be ignored
  -v VERBOSE_LEVEL, --verbose_level VERBOSE_LEVEL
                        Verbose level (0, 1 or 2)
```

For example if we want to check if a database connected to the postgres service `pg_service1` is equal to the 
database connected to the postgres service `pg_service_2`, we can do the following command:

```./pum.py check -p1 pg_service1 -p2 pg_service2```

If we want to run the same command but ignoring the different views and triggers, we do: 

```./pum.py check -p1 pg_service1 -p2 pg_service2 -i views triggers``` 

### dump
The `dump` command is used to create a dump (backup) of a postgres db.

The usage of the command is:

```usage: pum.py dump [-h] -p PG_SERVICE file

positional arguments:
  file                  The backup file

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
```

For example, the command to to backup the database connected to the postgres service `pg_service1` is into the file 
`/tmp/bak`:

```./pum.py dump -p pg_service1 /tmp/bak```

### restore

The `restore` command is used to restore a backup of a postgres db.

The usage is similar to the `dump` command:

```usage: pum.py restore [-h] -p PG_SERVICE file

positional arguments:
  file                  The backup file

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
```

If we want to restore the backup from the `/tmp/bak` into the database connected to the postgres service `pg_service2`:

```./pum.py restore -p pg_service2 /tmp/bak```

### upgrade

The `upgrade` command is used to upgrade an existing database using sql delta files. The command apply 
one or more delta files to an existing database and stores in a table the informations about the applied 
deltas. Only the delta files with version greater or equal than the current version are applied

The usage of the command is:

```
usage: pum.py upgrade [-h] -p PG_SERVICE -t TABLE -d DIR

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR, --dir DIR     Set delta directory
```

### info
The `info` command print the status of the already or not applied delta files.

The usage of the command is:
```
usage: pum.py info [-h] -p PG_SERVICE -t TABLE -d DIR

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR, --dir DIR     Set delta directory
```

### baseline

The `baseline` command creates the upgrades information table and sets the current version.

The usage of the command is:

```
usage: pum.py baseline [-h] -p PG_SERVICE -t TABLE -d DIR -b BASELINE

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR, --dir DIR     Delta directory
  -b BASELINE, --baseline BASELINE
                        Set baseline  in the format x.x.x
```

The baseline argument receives a the version number to be set in the upgrades information table. The version must match 
with the `^\d+\.\d+\.\d+$` regular expression, i.e. must be in the format x.x.x

### test-and-upgrade

The `test-and-upgrade` command does the following steps:

- creates a dump of the production db
- restores the db dump into a test db
- applies the delta files found in the delta directory to the test db. 
- checks if there are differences between the test db and a comparison db
- if no significant differences are found, after confirmation, applies the delta files to the production dbD.
Only the delta files with version greater or equal than the current version are applied

The usage of the command is:
```
usage: pum.py test-and-upgrade [-h] [-pp PG_SERVICE_PROD]
                               [-pt PG_SERVICE_TEST] [-pc PG_SERVICE_COMP]
                               [-t TABLE] [-d DIR] [-f FILE]
                               [-i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} 

optional arguments:
  -h, --help            show this help message and exit
  -pp PG_SERVICE_PROD, --pg_service_prod PG_SERVICE_PROD
                        Name of the pg_service related to production db
  -pt PG_SERVICE_TEST, --pg_service_test PG_SERVICE_TEST
                        Name of the pg_service related to a test db used to
                        test the migration
  -pc PG_SERVICE_COMP, --pg_service_comp PG_SERVICE_COMP
                        Name of the pg_service related to a db used to compare
                        the updated db test with the last version of the db
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR, --dir DIR     Set delta directory
  -f FILE, --file FILE  The backup file
  -i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} , 
  --ignore {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}
                        Elements to be ignored
```

## Config file

In the config file db_manager_config.yaml, you have to define, with the YAML syntax:
- **upgrades_table**: the name (and schema) of the table with the migration informations 
- **delta_dir**: the directory with the delta files.
- **backup_file**: the temporary db dump file used to copy the prod db to a test db
- **ignore_elements**: list of elements to ignore in db compare. Valid elements: tables, columns, 
constraints, views, sequences, indexes, triggers, functions or rules

For example:               
```upgrades_table: qwat_sys.upgrades
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
pg_restore_exe: pg_restore```                                                                              