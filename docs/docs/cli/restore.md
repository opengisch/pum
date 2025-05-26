# restore

Restore a Postgres database from a dump file.

```sh
pum [GLOBAL_OPTIONS] restore [OPTIONS] <file>
```

**Options:**

- `-x`  
  Ignore pg_restore errors.
- `-N, --exclude-schema <schema>`  
  Schema to be ignored. Can be used multiple times.
- `<file>`  
  The backup file.

---

> **Note:**
> All commands accept the global options. See the [CLI Overview](../cli.md) for details on global options.
