usage: pum role [-h] [--suffix SUFFIX] [--roles ROLES [ROLES ...]] [--to TO_ROLE] [--from FROM_ROLE] [--include-superusers] {create,grant,revoke,drop,list}
### positional arguments:
- `{create,grant,revoke,drop,list}`: Action to perform
### options:
- `-h, --help`: show this help message and exit
- `--suffix SUFFIX`: Create DB-specific roles by appending this suffix to each role name (e.g. 'lausanne' creates 'role_lausanne')
- `--roles ROLES [ROLES ...]`: Restrict the action to specific configured role names (space-separated). When omitted, all configured roles are affected.
- `--to TO_ROLE`: Target database user to grant role membership to (used with 'grant' action)
- `--from FROM_ROLE`: Target database user to revoke role membership from (used with 'revoke' action)
- `--include-superusers`: Include superusers in the role listing (they are hidden by default)
