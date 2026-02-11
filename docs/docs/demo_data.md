# Demo Data

Demo data in pum allows you to provide sample datasets that can be loaded into your database for testing, development, or demonstration purposes.

## Overview

Demo data is defined in your [configuration file](configuration/configuration.md) using the `demo_data` section. You basically provide an SQL file (or multiple SQL files) to fill the data. These SQL files are typically generated using `pg_dump` with specific options to export only the data you want to include.

## Configuration

Demo data is configured using the [`DemoDataModel`](configuration/models/demo_data_model) in your `.pum.yaml` configuration file. Each demo dataset requires:

- **name**: A descriptive name for the demo dataset
- **file**: Path to a single SQL file containing the demo data
- **files**: List of paths to multiple SQL files (use either `file` or `files`, not both)

### Single File Example

```yaml
demo_data:
  - name: some cool demo dataset
    file: demo_data/demo_data.sql
```

### Multiple Files Example

```yaml
demo_data:
  - name: some cool demo dataset
    files:
      - demo_data/demo_data_1.sql
      - demo_data/demo_data_2.sql
```

## Creating Demo Data Files

To create demo data SQL files, use `pg_dump` with the following options:

```bash
pg_dump --inserts --data-only --no-owner --no-privileges --schema=YOUR_DATA_SCHEMA > demo_data.sql
```

**Options explained:**
- `--inserts`: Use INSERT commands instead of COPY (more portable)
- `--data-only`: Export only data, not schema definitions
- `--no-owner`: Don't include ownership information
- `--no-privileges`: Don't include privilege information
- `--schema=YOUR_DATA_SCHEMA`: Export data only from the specified schema

Replace `YOUR_DATA_SCHEMA` with the name of your schema containing the demo data you want to export.
