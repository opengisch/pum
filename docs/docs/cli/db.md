usage: pum db [-h] [--template TEMPLATE] [--force] [--grant-connect GRANT_CONNECT [GRANT_CONNECT ...]] [--revoke-connect REVOKE_CONNECT [REVOKE_CONNECT ...]] [--keep-public]
{create,drop,access} dbname
### positional arguments:
- `{create,drop,access}`: Action to perform: create (new DB), drop (existing DB), access (configure CONNECT privileges)
- `dbname`: Name of the database to create or drop
### options:
- `-h, --help`: show this help message and exit
- `--template TEMPLATE`: Template database to use when creating (for duplicating an existing database)
- `--force`: Skip confirmation prompt for drop action
- `--grant-connect GRANT_CONNECT [GRANT_CONNECT ...]`: Role names to grant CONNECT on the target database
- `--revoke-connect REVOKE_CONNECT [REVOKE_CONNECT ...]`: Role names to explicitly revoke CONNECT from on the target database
- `--keep-public`: Do not revoke CONNECT from PUBLIC before applying role grants/revokes
