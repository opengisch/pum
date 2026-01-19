usage: update_cli_docs.py install [-h] [-p PARAMETER PARAMETER]
[--max-version MAX_VERSION] [-r]
[-g] [-d DEMO_DATA]
[--beta-testing]
### options:
- `-h, --help`: show this help message and exit
- `-p PARAMETER PARAMETER, --parameter PARAMETER PARAMETER`: Assign variable for running SQL deltas. Format is name value.
- `--max-version MAX_VERSION`: maximum version to install
- `-r, --roles`: Create roles
- `-g, --grant`: Grant permissions to roles
- `-d DEMO_DATA, --demo-data DEMO_DATA`: Load demo data with the given name
- `--beta-testing`: This will install the module in beta testing, meaning that it will not be possible to receive any future updates.
