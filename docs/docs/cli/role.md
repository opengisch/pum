usage: pum role [-h] [--suffix SUFFIX] [--roles ROLES [ROLES ...]] [--include-superusers] {create,grant,revoke,drop,check}
### positional arguments:
- `{create,grant,revoke,drop,check}`: Action to perform
### options:
- `-h, --help`: show this help message and exit
- `--suffix SUFFIX`: Create DB-specific roles by appending this suffix to each role name (e.g. 'lausanne' creates 'role_lausanne')
- `--roles ROLES [ROLES ...]`: Restrict the action to specific configured role names (space-separated). When omitted, all configured roles are affected.
- `--include-superusers`: Include superusers in the unknown-roles list when checking (they are hidden by default)
