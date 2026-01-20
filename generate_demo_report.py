#!/usr/bin/env python3
"""Generate a demo HTML report to showcase the new check feature."""

from datetime import datetime
from pum.checker import ComparisonReport, CheckResult, DifferenceItem, DifferenceType
from pum.report_generator import ReportGenerator

# Create check results
check_results = [
    CheckResult(name="Tables", key="tables", passed=True, differences=[]),
    CheckResult(
        name="Columns",
        key="columns",
        passed=False,
        differences=[
            DifferenceItem(
                type=DifferenceType.REMOVED,
                content="('public', 'users', 'email', 'character varying', 100)",
            ),
            DifferenceItem(
                type=DifferenceType.ADDED,
                content="('public', 'users', 'email', 'character varying', 255)",
            ),
            DifferenceItem(
                type=DifferenceType.ADDED,
                content="('public', 'users', 'created_at', 'timestamp', None)",
            ),
        ],
    ),
    CheckResult(name="Constraints", key="constraints", passed=True, differences=[]),
    CheckResult(
        name="Views",
        key="views",
        passed=False,
        differences=[
            DifferenceItem(
                type=DifferenceType.REMOVED,
                content="('user_stats', 'SELECT id, COUNT(*) FROM users GROUP BY id')",
            ),
            DifferenceItem(
                type=DifferenceType.ADDED,
                content="('user_stats', 'SELECT id, name, COUNT(*) FROM users GROUP BY id, name')",
            ),
        ],
    ),
    CheckResult(
        name="Indexes",
        key="indexes",
        passed=False,
        differences=[
            DifferenceItem(
                type=DifferenceType.ADDED, content="('users', 'idx_users_email', 'email', 'public')"
            ),
            DifferenceItem(
                type=DifferenceType.ADDED,
                content="('orders', 'idx_orders_created_at', 'created_at', 'public')",
            ),
        ],
    ),
    CheckResult(name="Functions", key="functions", passed=True, differences=[]),
]

# Create comparison report
report = ComparisonReport(
    pg_service1="database_production",
    pg_service2="database_staging",
    timestamp=datetime.now(),
    check_results=check_results,
)

# Generate HTML
html = ReportGenerator.generate_html(report)
output_file = "/tmp/pum_demo_report.html"
with open(output_file, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Demo HTML report generated at {output_file}")
print("Open it in your browser to see the new design!")
print()
print("Report summary:")
print(f"  Total checks: {report.total_checks}")
print(f"  Passed: {report.passed_checks}")
print(f"  Failed: {report.failed_checks}")
print(f"  Total differences: {report.total_differences}")
print(f"  Overall result: {'PASSED' if report.passed else 'FAILED'}")
