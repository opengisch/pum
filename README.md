# pum
Postgres Upgrades Manager

#TODO remove qwat references
pum is composed by some parts:

- **Manager** an interface for managing specific QWAT upgrade procedure
- **Checker** a script that compares 2 databases and shows the differences
- **Dumper** a script to backup and restore a Postgres db
- **Upgrader** a script that apply one or more delta files to an existing database 
and stores in a table the informations about the applied delta

## Manager

### Requirements
To upgrade an existing QWAT db you need to have 2 other empty db in addition to the db to be 
upgraded, with related pg_services defined.  
                             
#### Configurations
In the config file db_manager_config.yaml, you have to define:
- **upgrades_table**: the name (and schema) of the table with the migration informations 
- **delta_dir**: the directory with the delta files.
- **backup_file**: the temporary db dump file used to copy the prod db to a test db
- **ignore_elements**: list of elements to ignore in db compare. Valid elements: tables, columns, 
constraints, views, sequences, indexes, triggers, functions or rules
   
### QWAT upgrade procedure

To upgrade the QWAT datamodel, you can run the manager.py script that runs the upgrade procedure. 

```
usage: manager.py [-h] -pp PG_SERVICE_PROD -pt PG_SERVICE_TEST -pc PG_SERVICE_COMP
    
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
```

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

## Checker
The checker.py script is a script called by the manager.py that compares 2 databases and shows the
differences. This script can be used independently from QWAT to check 2 databases. 
It compares the following elements and tells if they are diffents:

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

```
usage: checker.py [-h] -p1 PG_SERVICE1 -p2 PG_SERVICE2 [-s SILENT]
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

## Dumper
The dumper.py script is a script called by the manager.py that backups and restores a Postgres database. 
This script can be used independently from QWAT.
 
 ```
usage: dumper.py [-h] -p PG_SERVICE [-d] [-r] file

positional arguments:
  file                  The backup file

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -d, --dump            Make a backup file of the database
  -r, --restore         Restore the db from the backup file
 ```
 
## Upgrader
The upgrader.py script is used to upgrade an existing database using sql delta files. The script apply 
one or more delta files to an existing database and stores in a table the informations about the applied 
deltas.
    
```
usage: upgrader.py [-h] -p PG_SERVICE -t TABLE -d DIR [-i] [-b BASELINE]

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Version table
  -d DIR, --dir DIR     Delta directory
  -i, --info            Show only info
  -b BASELINE, --baseline BASELINE
                        Create baseline
```
