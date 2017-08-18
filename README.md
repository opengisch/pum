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

It's possible to call pum in the following way:

```
python pum.py

```
or 

```
./pum.py

```


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
    test-and-upgrade    try the upgrade on a test db and if ok, do upgrade
                        prod db

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

**TODO esempio check semplice**
**TODO esempio check con ignore_list**
**TODO copiare l'help del check**

```
python pum.py check -p1 db1 -p2 db2 
```

### dump
The `dump` command is used to create a dump (backup) of a postgres db.

**TODO esempio**
**TODO copiare l'help del check**

### restore

**TODO esempio**
**TODO copiare l'help del check**

### upgrade

The upgrade command is used to upgrade an existing database using sql delta files. The command apply 
one or more delta files to an existing database and stores in a table the informations about the applied 
deltas.

**TODO esempio**
**TODO copiare l'help del check**

### info

**TODO esempio**
**TODO copiare l'help del check**

### baseline

**TODO esempio**
**TODO copiare l'help del check**

### test-and-upgrade

**TODO correggere la parte dell'init_qwat.sh**

This procedure makes the following steps:
- checks if the upgrades table exists in PG_SERVICE_PROD, if not, it asks if you want to create it
and set the baseline of the table with the current version founded in *qwat_sys.versions*
- creates a dump of the PG_SERVICE_PROD db
- restores the db dump into PG_SERVICE_TEST
- applies the delta files found in the delta directory to the PG_SERVICE_TEST db. Only the delta 
files with version greater or equal than the current version are applied
- creates PG_SERVICE_COMP whit the last qwat db version, using init_qwat.sh script
- checks if there are differences between PG_SERVICE_TEST and PG_SERVICE_COMP
- if no significant differences are found, applies the delta files to PG_SERVICE_PROD. Only the delta 
files with version greater or equal than the current version are applied

**TODO esempio**
**TODO copiare l'help del check**
                                                                           
## Config file

**TODO formato**
**TODO esempio**

In the config file db_manager_config.yaml, you have to define:
- **upgrades_table**: the name (and schema) of the table with the migration informations 
- **delta_dir**: the directory with the delta files.
- **backup_file**: the temporary db dump file used to copy the prod db to a test db
- **ignore_elements**: list of elements to ignore in db compare. Valid elements: tables, columns, 
constraints, views, sequences, indexes, triggers, functions or rules
                                                                                             