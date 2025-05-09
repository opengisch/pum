

### Schema separation

We recommend to isolate data (tables) from business logic (e.g., views, triggers) into distinct schemas for easier upgrades.
This will facilitate the migrations but also the code management: you will not have to write diff files for views and triggers.

To achieve this you can organize the code in such a manner:
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

TODO: write about how to define pre/post
