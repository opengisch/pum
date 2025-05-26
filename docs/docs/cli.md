# Command Line Interface (CLI) documentation

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

- [info](cli/info.md)
- [install](cli/install.md)
- [check](cli/check.md)
- [dump](cli/dump.md)
- [restore](cli/restore.md)
- [baseline](cli/baseline.md)
- [upgrade](cli/upgrade.md)
- [help](cli/help.md)
