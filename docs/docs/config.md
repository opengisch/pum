
# Config

In the config file .pum-config.yaml, you can define, with the YAML syntax:
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
