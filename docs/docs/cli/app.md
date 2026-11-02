```text
usage: pum app [-h] [-p PARAMETER PARAMETER] [--skip-grant] [--suffix SUFFIX] {create,drop,recreate}
```

### positional arguments:

- `{create,drop,recreate}`: Action to perform: create (run create_app handlers), drop (run drop_app handlers), recreate (run drop then create)

### options:

- `-h, --help`: show this help message and exit
- `-p PARAMETER PARAMETER, --parameter PARAMETER PARAMETER`: Assign variable for running SQL handlers. Format is name value.
- `--skip-grant`: Skip granting permissions to roles
- `--suffix SUFFIX`: Grant the permissions to the DB-specific roles with this suffix instead of the generic ones
