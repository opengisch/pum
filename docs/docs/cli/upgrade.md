usage: pum upgrade [-h] [-p PARAMETER PARAMETER] [-u MAX_VERSION] [--skip-grant] [--beta-testing]
[--skip-drop-app] [--skip-create-app]
### options:
- `-h, --help`: show this help message and exit
- `-p PARAMETER PARAMETER, --parameter PARAMETER PARAMETER`: Assign variable for running SQL deltas. Format is name value.
- `-u MAX_VERSION, --max-version MAX_VERSION`: maximum version to upgrade
- `--skip-grant`: Skip granting permissions to roles
- `--beta-testing`: Install in beta testing mode.
- `--skip-drop-app`: Skip drop app handlers during upgrade.
- `--skip-create-app`: Skip create app handlers during upgrade.
