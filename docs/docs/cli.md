# pum Command Line Interface (CLI) Documentation

`pum` is a command-line tool for managing PostgreSQL schema migrations, upgrades, and checks.

## Usage

```sh
pum [OPTIONS] <command> [ARGS]
```

## Global Options

- `-c, --config_file <file>`  
  Set the config file. Default: `.pum.yaml`
- `-s, --pg-service <service>`  
  **(Required)** Name of the postgres service.
- `-d, --dir <directory>`  
  Directory or URL of the module. Default: `.`
- `-v, --verbose`  
  Increase output verbosity (e.g. `-v`, `-vv`).
- `--version`  
  Show program's version number and exit.

## Commands

### info

Show info about schema migrations history.

```sh
pum info [OPTIONS]
```

---

### install

Installs the module.

```sh
pum install [OPTIONS]
```

**Options:**

- `-p, --parameter <name> <value>`  
  Assign variable for running SQL deltas. Can be used multiple times.

---

### check

Check the differences between two databases.

```sh
pum check [OPTIONS]
```

**Options:**

- `-i, --ignore <elements>`  
  Elements to be ignored. Choices: `tables`, `columns`, `constraints`, `views`, `sequences`, `indexes`, `triggers`, `functions`, `rules`.
- `-N, --exclude-schema <schema>`  
  Schema to be ignored. Can be used multiple times.
- `-P, --exclude-field-pattern <pattern>`  
  Fields to be ignored based on a pattern compatible with SQL LIKE. Can be used multiple times.
- `-o, --output_file <file>`  
  Output file for differences.

---

### dump

Dump a Postgres database.

```sh
pum dump [OPTIONS] <file>
```

**Options:**

- `-N, --exclude-schema <schema>`  
  Schema to be ignored. Can be used multiple times.
- `<file>`  
  The backup file.

---

### restore

Restore a Postgres database from a dump file.

```sh
pum restore [OPTIONS] <file>
```

**Options:**

- `-x`  
  Ignore pg_restore errors.
- `-N, --exclude-schema <schema>`  
  Schema to be ignored. Can be used multiple times.
- `<file>`  
  The backup file.

---

### baseline

Create upgrade information table and set baseline.

```sh
pum baseline [OPTIONS]
```

**Options:**

- `-t, --table <table>`  
  **(Required)** Upgrades information table.
- `-d, --dir <dir> [<dir> ...]`  
  **(Required)** Delta directories (space-separated).
- `-b, --baseline <version>`  
  **(Required)** Set baseline in the format x.x.x.

---

### upgrade

Upgrade the database.

```sh
pum upgrade [OPTIONS]
```

**Options:**

- `-u, --max-version <version>`  
  Upper bound limit version.
- `-p, --parameter <name> <value>`  
  Assign variable for running SQL deltas. Can be used multiple times.

---

## Help

For help on any command, use:

```sh
pum <command> --help
```
