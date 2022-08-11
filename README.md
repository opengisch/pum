# Pum

Pum stands for "Postgres Upgrades Manager". It is a Database migration management tool very similar to flyway-db or Liquibase, based on metadata tables.


## Features
Pum is python program that can be used via command line or directly from another python program.

Pum permits the followings operations on Postgres databases:

- check the differences between two databases
- create a backup (dump file) of a database
- restore a database from a backup
- upgrade a database applying delta files

and some other useful operations.

## General purpose and workflow

Good practices regarding database versioning and migration are not so easy to handle in a VCS (Version Control System). Initial development is easy, using pure Git, and sometimes some meta SQL generation scripts. But when it comes to maintaining databases already in production, good practices differ a lot since SQL patches can't be handled the same way as Git diffs.

We recommend reading some of those great articles to get a clearer view on what could, and should (or not) be done:

- https://blog.codinghorror.com/get-your-database-under-version-control/
- http://petereisentraut.blogspot.fr/2012/05/my-anti-take-on-database-schema-version.html

The worklow involves having version metadata written INSIDE the database and using that to check current state, old migrations, new migrations to apply, etc..

The first thing to do is use the "baseline" command to create metadata in your database, and then you are good to go.

## Installation

### System-wide

This is the easiest way to install pum for every user on a system

```sh
sudo pip3 install pum
```

### Local user

Alternatively, to install pum to a virtual environment without requiring sudo access

```sh
mkdir -p ~/.venv
virtualenv -p python3 ~/.venv/pum
source ~/.venv/pum/bin/activate
pip install pum
```

Whenever you use pum you will need to activate the virtual environment prior to using pum


```sh
source ~/.venv/pum/bin/activate
```

### Dependencies

Pum depends on python3 and postgresql (specifically `pg_restore` and `pg_dump`). Make sure to read the [config file section](#config-file) below if you're on Windows.

## History

Pum has been developed to solve issues encountered in the [QWAT](https://github.com/qwat) and [QGEP](https://github.com/QGEP/QGEP) project, which are open source Geographic Information System for network management based on [QGIS](http://qgis.org/fr/site/).
QWAT already developed a dedicated migration tool, allowing to both work on the data model using git AND use delta file for migrations. QGEP needed something also so the group decided to make a more generic tool, yet a simple one to handle that.

## Command line

### pum

The usage of the pum command is:
```commandline

usage: pum [-h] [-v] [-c CONFIG_FILE]
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

optional arguments:
  -h, --help            show this help message and exit
  -p PG_SERVICE, --pg_service PG_SERVICE
                        Name of the postgres service
  -t TABLE, --table TABLE
                        Upgrades information table
  -d DIR [DIR ...], --dir DIR [DIR ...]
                        Delta directories (space-separated)
  -u VERSION, --max-version VERSION
                        Upper bound limit version to run the deltas up to.
  -v TYPE VARIABLE VALUE, --var TYPE VARIABLE VALUE
                        Assign variable for running SQL deltas. TYPE is one of int, float, str.
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

### test-and-upgrade

The `test-and-upgrade` command does the following steps:

- creates a dump of the production db
- restores the db dump into a test db
- applies the delta files found in the delta directories to the test db.
- checks if there are differences between the test db and a comparison db
- if no significant differences are found, after confirmation, applies the delta files to the production dbD.
Only the delta files with version greater or equal than the current version are applied.

The usage of the command is:
```commandline
usage: pum test-and-upgrade [-h] [-pp PG_SERVICE_PROD]
                            [-pt PG_SERVICE_TEST] [-pc PG_SERVICE_COMP]
                            [-t TABLE] -d DIR [DIR ...] [-f FILE]
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
  -d DIR [DIR ...], --dir DIR [DIR ...]
                        Delta directories (space-separated)
  -f FILE, --file FILE  The backup file
  -x                    ignore pg_restore errors
  -i {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules} ,
  --ignore {tables,columns,constraints,views,sequences,indexes,triggers,functions,rules}
                        Elements to be ignored
  -u VERSION, --max-version VERSION
                        Upper bound limit version to run the deltas up to.
  -v TYPE VARIABLE VALUE, --var TYPE VARIABLE VALUE
                        Assign variable for running SQL deltas. TYPE is one of int, float, str.
  -N SCHEMA, --exclude-schema SCHEMA
                        Schema to be skipped.
  -P PATTERN, --exclude-field-pattern PATTERN
                        A field pattern which should be ignored in column checking
```

## Delta files

A delta file can be a SQL file containing one or more SQL statements or a Python module containing a
class that is a subclass of DeltaPy.

There are 5 kind of delta files:
- **pre-all**, is executed first thing in an upgrade command. The file must have the name `pre-all.py`
or `pre-all.sql`. The pre-all files doesn't have a version because they are executed for all upgrade
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
- **post all**, is executed last thing in an upgrade command. The file must have the name `post-all.py`
or `post-all.sql`. The pre-all files doesn't have a version because they are executed for all upgrade
command regardless of the current db version.

A Python file is executed before the sql file with the same kind and version.

In summary the upgrade workflow is:

```text
execute pre-all.py if exists
execute pre-all.sql if exists

for each file delta_x.x.x_deltaname.* ordered by version number:
	execute delta_x.x.x_deltaname.pre.py if exists
	execute delta_x.x.x_deltaname.pre.sql if exists

	execute delta_x.x.x_deltaname.py if exists
	execute delta_x.x.x_deltaname.sql if exists

	execute delta_x.x.x_deltaname.post.py if exists
	execute delta_x.x.x_deltaname.post.sql if exists

execute post-all.py if exists
execute post-all.sql if exists
```

### SQL deltas

If SQL code contains `%` characater, it must be escaped by using `%%`.

If variables are used, it follows `psycopg` rules to [pass Parameters to SQL queries](http://initd.org/psycopg/docs/usage.html#query-parameters).
For example, a string variable named SRID, will be called in a SQL delta as `%(SRID)s`.

### Python delta files

A Python delta file must be a subclass of the DeltaPy class. The DeltaPy class has the following methods:

```python
    @abstractmethod
    def run(self):
        """This method must be implemented in the subclasses. It is called
        when the delta.py file is runned by Upgrader class"""
        
    @property
    def variables(self):
        """Return the dictionary of variables"""
        
    @property
    def current_db_version(self):
        """Return the current db version"""

    @property
    def delta_dir(self):
        """Return the path of the delta directory including this delta"""

    @property
    def delta_dirs(self):
        """Return the paths of the delta directories"""

    @property
    def pg_service(self):
        """Return the name of the postgres service"""

    @property
    def upgrades_table(self):
        """Return the name of the upgrades information table"""

    def write_message(self, text):
        """Print a message from the subclass.

        Parameters
        ----------
        text: str
            The message to print
        """
```

An example of implementation is:
```python
from core.deltapy import DeltaPy


class Prova(DeltaPy):

    def run(self):

        # if you want to get the current db version
        version = self.current_db_version

        # if you want to get the path to the delta directory including that delta file
        delta_dir = self.delta_dir

        # if you want to get the paths to the delta directories
        delta_dirs = self.delta_dirs

        # if you want to get the pg_service name
        pg = self.pg_service

        # if you want to get the upgrade information table name
        table = self.upgrades_table
        
        # access to a variable given in command line
        srid = self.variables.get('srid', 4326)

        # if you want to print a message
        self.write_message('foo')

        # Here goes all the code of the delta file
        some_cool_python_command()
```


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
