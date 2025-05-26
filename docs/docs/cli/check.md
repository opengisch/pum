# check

Check the differences between two databases.

```sh
pum [GLOBAL_OPTIONS] check [OPTIONS]
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

> **Note:**
> All commands accept the global options. See the [CLI Overview](../cli.md) for details on global options.
