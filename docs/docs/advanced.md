

### Schema separation

We recommend to isolate data (tables) from business logic (e.g., views, triggers) into distinct schemas for easier upgrades.

```
project/
├── changelogs/
│   ├── 1.0.0/
│   │   ├── 01_create_schema.sql
│   │   └── 02_create_tables.sql
│   ├── 1.0.1/
│   │   ├── 01_rename_column.sql
│   │   └── 02_do_something_else.sql
├── app/
│   └── create_views_and_triggers.sql
└── .pum.yaml
```
