usage: pum role [-h] [--suffix SUFFIX] [--no-create-generic] {create,grant,revoke,drop}
### positional arguments:
- `{create,grant,revoke,drop}`: Action to perform
### options:
- `-h, --help`: show this help message and exit
- `--suffix SUFFIX`: Create DB-specific roles by appending this suffix to each role name (e.g. 'lausanne' creates 'role_lausanne')
- `--no-create-generic`: When using --suffix, skip creating the generic (base) roles and granting inheritance
