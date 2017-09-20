# pum
Acronym stands for "Postgres Upgrades Manager". It is a Database migration management tool very similar to flyway-db or Liquibase, based on metadata tables.


## Features
Pum is python program that can be used via command line or directly from another python program.

Pum permits the followings operations on Postgres databases:

- check the differences between two databases
- create a backup (dump file) of a database
- restore a database from a backup
- upgrade a database applying delta files

and some other useful operations.

## General purpose and workflow

Good practices regarding database versioning and migration are not so easy to handle in a CVS code management system. Initial developpement is easy, using pure git, and sometimes some meta SQL generation scripts. But when it comes to maintaining databases already in production, good practices differ a lot since SQL patchs can be handled the same way as git diffs.

We recommend reading somes of those great articles to get a clearer view on what could, and should (or not) be done:

- https://blog.codinghorror.com/get-your-database-under-version-control/
- http://petereisentraut.blogspot.fr/2012/05/my-anti-take-on-database-schema-version.html

The worklow consists in having version metadata written INSIDE the database and use that to check current state, old migrations, new migrations to apply, etc..

The first thing to do is use the "baseline" command to create metadata in your database, and then you are good to go.

## History

PUM has been developped to solve issues encountered in the [QWAT](https://github.com/qwat) and [QGEP](https://github.com/QGEP/QGEP) project, which are open source Geographic Information System for network management based on [QGIS](http://qgis.org/fr/site/).
QWAT already developped a dedicated migration tool, allowing to both work on the data model using git AND use delta file for migrations. QGEP needed something also so the group decided to make a more generic tool, yet a simple one to handle that.  

## Command line

### pum

The usage of the pum command is:
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

For example, the command to backup the database connected to the postgres service `pg_service1` is into the file
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
deltas. Only the delta files with version greater or equal than the current version are applied.

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
                        Set baseline
```

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
