usage: pum install [-h] [-p PARAMETER PARAMETER] [--max-version MAX_VERSION] [--skip-roles] [--skip-grant] [-d DEMO_DATA]
[--beta-testing] [--skip-drop-app] [--skip-create-app]
### options:
- `-h, --help`: show this help message and exit
- `-p PARAMETER PARAMETER, --parameter PARAMETER PARAMETER`: Assign variable for running SQL deltas. Format is name value.
- `--max-version MAX_VERSION`: maximum version to install
- `--skip-roles`: Skip creating roles
- `--skip-grant`: Skip granting permissions to roles
- `-d DEMO_DATA, --demo-data DEMO_DATA`: Load demo data with the given name
- `--beta-testing`: This will install the module in beta testing, meaning that it will not be possible to receive any future updates.
- `--skip-drop-app`: Skip drop app handlers during installation.
- `--skip-create-app`: Skip create app handlers during installation.
