# Database Checker

The PUM Checker is a powerful tool for comparing two PostgreSQL databases and identifying structural differences. This is particularly useful for:

- **Quality Assurance**: Verify that development and production databases are in sync
- **Migration Validation**: Confirm that database upgrades were applied correctly
- **Environment Comparison**: Compare staging, testing, and production environments
- **CI/CD Integration**: Automatically validate database schemas in continuous integration pipelines

## How It Works

The Checker compares two PostgreSQL databases by analyzing their metadata from `information_schema` tables. It performs systematic checks across multiple database elements and generates a detailed report of any differences found.

### Comparison Process

1. **Connection**: Establishes connections to both databases using PostgreSQL service names
2. **Element Scanning**: Queries database metadata for each structural element
3. **Comparison**: Compares the results between the two databases
4. **Difference Detection**: Identifies added (+) or removed (-) elements
5. **Report Generation**: Creates a comprehensive report in text, HTML, or JSON format

### Checked Elements

The Checker examines the following database elements:

- **Tables**: Table definitions and their schemas
- **Columns**: Column names, types, and attributes
- **Constraints**: Primary keys, foreign keys, unique constraints, and check constraints
- **Views**: View definitions
- **Sequences**: Sequence definitions and configurations
- **Indexes**: Index definitions and types
- **Triggers**: Trigger functions and configurations
- **Functions**: Stored procedures and functions
- **Rules**: Database rules

### Filtering and Exclusions

You can customize what gets compared:

- **Ignore specific elements**: Skip certain types of objects (e.g., `--ignore views triggers`)
- **Exclude schemas**: Ignore specific schemas (e.g., `-N audit -N logging`)
- **Exclude field patterns**: Filter out fields matching SQL LIKE patterns (e.g., `-P '%_backup'`)

System schemas (`information_schema` and `pg_%`) are automatically excluded from checks.

## Output Formats

The Checker supports three output formats:

### Text Format (Default)
Simple, readable text output showing differences with `+` (added) and `-` (removed) markers.

```
Tables: OK
Columns: 2 differences found
+ public.users.created_at
- public.users.updated_by
```

### HTML Format
Interactive HTML report with styling and collapsible sections, ideal for sharing with teams or embedding in documentation.

### JSON Format
Structured JSON output for programmatic processing and integration with other tools.

## Usage

For detailed command-line usage and examples, see the [check command documentation](cli/check.md).

### Basic Example

```bash
pum -s my_database check production_database
```

This compares the `my_database` service against `production_database` and shows any structural differences.

### Advanced Example

```bash
pum -s dev_db check prod_db \
  --ignore triggers functions \
  --exclude-schema audit \
  --format html \
  --output_file comparison_report.html
```

This creates an HTML report comparing databases while ignoring triggers and functions, and excluding the audit schema.

## Exit Codes

- **0**: Databases are identical (or only ignored differences were found)
- **1**: Differences were detected

This makes the Checker ideal for CI/CD pipelines where you need to fail builds if databases are out of sync.

## Programmatic Usage

You can also use the Checker in Python code:

```python
from pum.checker import Checker
from pum.report_generator import ReportGenerator

checker = Checker(
    pg_service1="development",
    pg_service2="production",
    exclude_schema=["audit"],
    ignore_list=["triggers"]
)

report = checker.run_checks()

if report.passed:
    print("✓ Databases are in sync")
else:
    print(f"✗ Found {report.total_differences} differences")

# Generate HTML report
html = ReportGenerator.generate_html(report)
with open("report.html", "w") as f:
    f.write(html)
```

## See Also

- [CLI Reference](cli/check.md) - Complete command-line options and examples
- [Getting Started](getting_started.md) - Initial setup and configuration
- [Application](application.md) - Understanding the PUM architecture
