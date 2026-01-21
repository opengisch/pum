"""Report generation for database comparison results."""

import json

try:
    from jinja2 import Template

    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

from .checker import ComparisonReport


class ReportGenerator:
    """Generates reports from ComparisonReport objects."""

    @staticmethod
    def _format_difference(content: dict | str, check_name: str = "") -> dict:
        """Format difference for better readability.

        Args:
            content: The structured dict or raw string from the difference
            check_name: The name of the check (e.g., "Views", "Triggers", "Columns")

        Returns:
            dict with formatting information
        """
        # If already structured data (dict), format it appropriately
        if isinstance(content, dict):
            # Determine object type based on available keys
            # IMPORTANT: Check order matters! Constraints, Indexes, and Columns all have 'column_name'
            # Check most specific first
            if check_name == "Constraints" or "constraint_name" in content:
                # Constraint format
                schema = content.get("constraint_schema", "") or "public"
                table = content.get("table_name", "")
                constraint_name = content.get("constraint_name", "")
                constraint_type = content.get("constraint_type", "")
                constraint_def = content.get("constraint_definition", "")

                # Build details - only include non-empty values
                details = []
                if constraint_type:
                    details.append(f"Type: {constraint_type}")
                if content.get("column_name") and content.get("column_name") != "":
                    details.append(f"Column: {content['column_name']}")
                if content.get("foreign_table_name") and content.get("foreign_table_name") != "":
                    details.append(f"References: {content['foreign_table_name']}")
                if content.get("foreign_column_name") and content.get("foreign_column_name") != "":
                    details.append(f"Foreign column: {content['foreign_column_name']}")

                return {
                    "is_structured": True,
                    "type": "constraint",
                    "schema_object": f"{schema}.{table}",
                    "detail": constraint_name,
                    "extra": " | ".join(details) if details else None,
                    "sql": constraint_def if constraint_def and constraint_def != "" else None,
                }
            elif check_name == "Indexes" or "index_name" in content:
                # Index format - check BEFORE columns since indexes also have column_name
                schema = content.get("schema_name", "") or "public"
                table = content.get("table_name", "")
                index_name = content.get("index_name", "")
                column = content.get("column_name", "")
                index_def = content.get("index_definition", "")

                # Build details
                details = []
                if column and column != "":
                    details.append(f"Column: {column}")

                return {
                    "is_structured": True,
                    "type": "index",
                    "schema_object": f"{schema}.{table}",
                    "detail": index_name,
                    "extra": " | ".join(details) if details else None,
                    "sql": index_def if index_def and index_def != "" else None,
                }
            elif check_name == "Columns" or "column_name" in content:
                # Column format
                schema = content.get("table_schema", "")
                table = content.get("table_name", "")

                # Build details
                details = []
                if content.get("data_type"):
                    details.append(f"Type: {content['data_type']}")
                if content.get("is_nullable"):
                    details.append(f"Nullable: {content['is_nullable']}")
                if (
                    content.get("column_default")
                    and str(content.get("column_default")).lower() != "none"
                ):
                    details.append(f"Default: {content['column_default']}")
                if (
                    content.get("character_maximum_length")
                    and str(content.get("character_maximum_length")).lower() != "none"
                ):
                    details.append(f"Max length: {content['character_maximum_length']}")
                if (
                    content.get("numeric_precision")
                    and str(content.get("numeric_precision")).lower() != "none"
                ):
                    details.append(f"Precision: {content['numeric_precision']}")

                return {
                    "is_structured": True,
                    "type": "column",
                    "schema_object": f"{schema}.{table}",
                    "detail": content.get("column_name", ""),
                    "extra": " | ".join(details) if details else None,
                    "sql": None,
                }
            elif check_name == "Views" or "view_definition" in content:
                # View format
                schema = content.get("table_schema", "") or "public"
                view_name = content.get("table_name", "")
                # Handle both old and new column names
                sql = content.get("view_definition") or content.get("replace", "")
                return {
                    "is_structured": True,
                    "type": "view",
                    "schema_object": f"{schema}.{view_name}",
                    "detail": None,
                    "sql": sql,
                }
            elif check_name == "Triggers" or "prosrc" in content:
                # Trigger format
                schema = content.get("schema_name", "")
                table = content.get("relname", "")
                trigger = content.get("tgname", "")
                sql = content.get("prosrc", "")
                return {
                    "is_structured": True,
                    "type": "trigger",
                    "schema_object": f"{schema}.{table}",
                    "detail": trigger,
                    "sql": sql,
                }
            elif check_name == "Functions" or "routine_definition" in content:
                # Function format - show SQL in scrollable widget
                schema = content.get("routine_schema", "")
                function_name = content.get("routine_name", "")
                sql = content.get("routine_definition", "")
                param_type = content.get("data_type", "")

                # Only add parameter type if it's a real value
                extra = None
                if param_type and param_type not in ["", "None", None]:
                    extra = f"Parameter type: {param_type}"

                return {
                    "is_structured": True,
                    "type": "function",
                    "schema_object": f"{schema}.{function_name}",
                    "detail": "",
                    "extra": extra,
                    "sql": sql if sql and sql != "" else None,
                }
            elif check_name == "Rules" or "rule_name" in content:
                # Rule format
                schema = content.get("rule_schema", "") or "public"
                table = content.get("rule_table", "")
                rule_name = content.get("rule_name", "")
                event = content.get("rule_event", "")

                # Build details
                details = []
                if event:
                    details.append(f"Event: {event}")

                return {
                    "is_structured": True,
                    "type": "rule",
                    "schema_object": f"{schema}.{table}",
                    "detail": rule_name,
                    "extra": " | ".join(details) if details else None,
                    "sql": None,
                }
            else:
                # Generic object format (tables, sequences, etc.)
                # Try to find schema and name keys
                schema = (
                    content.get("table_schema")
                    or content.get("constraint_schema")
                    or content.get("schema_name", "")
                    or "public"
                )
                name = (
                    content.get("table_name")
                    or content.get("relname")
                    or content.get("routine_name", "")
                )
                if not name and len(content) >= 2:
                    # Fallback: use first two values as schema.name
                    values = list(content.values())
                    schema = str(values[0]) if values else "public"
                    name = str(values[1]) if len(values) > 1 else ""

                return {
                    "is_structured": True,
                    "type": "object",
                    "schema_object": f"{schema}.{name}" if schema and name else str(content),
                    "detail": None,
                    "extra": None,
                    "sql": None,
                }

        # Fallback: old string-based parsing (for backward compatibility)
        return {"is_structured": False, "content": str(content), "sql": None}

    @staticmethod
    def _group_columns_by_table(differences):
        """Group column differences by table.

        Args:
            differences: List of DifferenceItem objects

        Returns:
            dict: Grouped columns by table and type
        """
        from collections import defaultdict

        grouped = defaultdict(lambda: {"removed": [], "added": []})

        for diff in differences:
            if isinstance(diff.content, dict) and "column_name" in diff.content:
                # Structured column data
                table = (
                    f"{diff.content.get('table_schema', '')}.{diff.content.get('table_name', '')}"
                )
                diff_type = "removed" if diff.type.value == "removed" else "added"

                # Format column details
                details = []
                if diff.content.get("data_type"):
                    details.append(f"Type: {diff.content['data_type']}")
                if diff.content.get("is_nullable"):
                    details.append(f"Nullable: {diff.content['is_nullable']}")
                if (
                    diff.content.get("column_default")
                    and str(diff.content["column_default"]).lower() != "none"
                ):
                    details.append(f"Default: {diff.content['column_default']}")
                if (
                    diff.content.get("character_maximum_length")
                    and str(diff.content["character_maximum_length"]).lower() != "none"
                ):
                    details.append(f"Max length: {diff.content['character_maximum_length']}")
                if (
                    diff.content.get("numeric_precision")
                    and str(diff.content["numeric_precision"]).lower() != "none"
                ):
                    details.append(f"Precision: {diff.content['numeric_precision']}")

                grouped[table][diff_type].append(
                    {
                        "column": diff.content.get("column_name", ""),
                        "extra": " | ".join(details),
                        "diff": diff,
                    }
                )
            else:
                # Fallback for old string format
                formatted = ReportGenerator._format_difference(diff.content, "Columns")
                if formatted.get("is_structured") and formatted.get("type") == "column":
                    table = formatted["schema_object"]
                    diff_type = "removed" if diff.type.value == "removed" else "added"
                    grouped[table][diff_type].append(
                        {
                            "column": formatted["detail"],
                            "extra": formatted.get("extra", ""),
                            "diff": diff,
                        }
                    )

        return dict(grouped)

    @staticmethod
    def generate_text(report: ComparisonReport) -> str:
        """Generate a text report.

        Args:
            report: The comparison report

        Returns:
            Text report as a string

        """
        lines = []
        for result in report.check_results:
            lines.append(result.name)
            for diff in result.differences:
                lines.append(str(diff))
        return "\n".join(lines)

    @staticmethod
    def generate_json(report: ComparisonReport) -> str:
        """Generate a JSON report.

        Args:
            report: The comparison report

        Returns:
            JSON report as a string

        """

        def serialize_diff(diff):
            """Serialize a DifferenceItem to a dict."""
            return {
                "type": diff.type.value,
                "content": diff.content if isinstance(diff.content, dict) else str(diff.content),
            }

        def serialize_result(result):
            """Serialize a CheckResult to a dict."""
            return {
                "name": result.name,
                "key": result.key,
                "passed": result.passed,
                "difference_count": result.difference_count,
                "differences": [serialize_diff(diff) for diff in result.differences],
            }

        data = {
            "pg_connection1": report.pg_connection1,
            "pg_connection2": report.pg_connection2,
            "timestamp": report.timestamp.isoformat(),
            "passed": report.passed,
            "total_checks": report.total_checks,
            "passed_checks": report.passed_checks,
            "failed_checks": report.failed_checks,
            "total_differences": report.total_differences,
            "check_results": [serialize_result(result) for result in report.check_results],
        }

        return json.dumps(data, indent=2)

    @staticmethod
    def generate_html(report: ComparisonReport) -> str:
        """Generate an HTML report.

        Args:
            report: The comparison report

        Returns:
            HTML report as a string

        Raises:
            ImportError: If Jinja2 is not installed

        """
        if not JINJA2_AVAILABLE:
            raise ImportError(
                "Jinja2 is required for HTML report generation. "
                "Install it with: pip install 'pum[html]'"
            )
        template_str = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Comparison Report</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }

        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
        }

        .header h1 {
            font-size: 28px;
            margin-bottom: 10px;
        }

        .header .meta {
            opacity: 0.9;
            font-size: 14px;
        }

        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
            border-bottom: 1px solid #e9ecef;
        }

        .summary-card {
            background: white;
            padding: 20px;
            border-radius: 6px;
            border-left: 4px solid #667eea;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        .summary-card.success {
            border-left-color: #28a745;
        }

        .summary-card.error {
            border-left-color: #dc3545;
        }

        .summary-card.warning {
            border-left-color: #ffc107;
        }

        .summary-card .value {
            font-size: 32px;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }

        .summary-card.success .value {
            color: #28a745;
        }

        .summary-card.error .value {
            color: #dc3545;
        }

        .summary-card.warning .value {
            color: #ffc107;
        }

        .summary-card .label {
            font-size: 14px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .content {
            padding: 30px;
        }

        .check-section {
            margin-bottom: 30px;
            border: 1px solid #e9ecef;
            border-radius: 6px;
            overflow: hidden;
        }

        .check-header {
            background: #f8f9fa;
            padding: 15px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            user-select: none;
        }

        .check-header:hover {
            background: #e9ecef;
        }

        .check-header h2 {
            font-size: 18px;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }

        .badge.success {
            background: #d4edda;
            color: #155724;
        }

        .badge.error {
            background: #f8d7da;
            color: #721c24;
        }

        .diff-count {
            font-size: 14px;
            color: #6c757d;
        }

        .check-body {
            padding: 20px;
            background: white;
        }

        .check-body.collapsed {
            display: none;
        }

        .no-differences {
            color: #28a745;
            font-style: italic;
            padding: 10px;
            text-align: center;
        }

        .diff-list {
            list-style: none;
        }

        .diff-item {
            padding: 12px 15px;
            margin-bottom: 8px;
            border-radius: 4px;
            font-family: 'Courier New', Courier, monospace;
            font-size: 13px;
            line-height: 1.5;
            border-left: 3px solid;
            word-break: break-all;
            position: relative;
        }

        .diff-item.removed {
            background: #fff5f5;
            border-left-color: #dc3545;
            color: #721c24;
        }

        .diff-item.added {
            background: #f0f9ff;
            border-left-color: #28a745;
            color: #155724;
        }

        .diff-item .diff-marker {
            display: inline-block;
            width: 30px;
            font-weight: bold;
            margin-right: 10px;
        }

        .diff-item .db-label {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 11px;
            font-weight: bold;
            margin-right: 10px;
            background: rgba(0,0,0,0.1);
        }

        .diff-item.removed .db-label {
            background: #dc3545;
            color: white;
        }

        .diff-item.added .db-label {
            background: #28a745;
            color: white;
        }

        .diff-explanation {
            font-size: 12px;
            font-style: italic;
            color: #6c757d;
            margin-bottom: 5px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
        }

        .tooltip {
            position: relative;
            cursor: help;
            border-bottom: 1px dotted #999;
        }

        .tooltip .tooltiptext {
            visibility: hidden;
            width: 300px;
            background-color: #555;
            color: #fff;
            text-align: left;
            border-radius: 6px;
            padding: 8px 12px;
            position: absolute;
            z-index: 1;
            bottom: 125%;
            left: 50%;
            margin-left: -150px;
            opacity: 0;
            transition: opacity 0.3s;
            font-size: 12px;
            font-style: normal;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }

        .tooltip .tooltiptext::after {
            content: "";
            position: absolute;
            top: 100%;
            left: 50%;
            margin-left: -5px;
            border-width: 5px;
            border-style: solid;
            border-color: #555 transparent transparent transparent;
        }

        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }

        .diff-content {
            margin-top: 5px;
            padding-left: 40px;
        }

        .schema-object {
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 3px 10px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            margin-right: 8px;
            font-family: 'Courier New', Courier, monospace;
        }

        .object-detail {
            font-weight: bold;
            color: #333;
            font-size: 14px;
        }

        .object-extra {
            color: #6c757d;
            font-size: 12px;
            margin-top: 3px;
            padding-left: 40px;
        }

        .object-content {
            font-family: 'Courier New', Courier, monospace;
            font-size: 13px;
        }

        .collapsible-toggle {
            background: #007bff;
            color: white;
            border: none;
            padding: 4px 12px;
            margin-left: 8px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 11px;
            font-weight: 600;
            transition: background 0.2s;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            display: inline-block;
            vertical-align: middle;
        }

        .collapsible-toggle:hover {
            background: #0056b3;
        }

        .collapsible-toggle::before {
            content: '\u25b6 ';
            font-size: 10px;
            transition: transform 0.2s;
            display: inline-block;
        }

        .collapsible-toggle:not(.collapsed)::before {
            content: '\u25bc ';
        }

        .collapsible-content {
            max-height: 500px;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
            display: block;
            width: 100%;
        }

        .collapsible-content.collapsed {
            max-height: 0;
        }

        .sql-widget {
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 4px;
            padding: 12px;
            margin-top: 4px;
            max-height: 300px;
            overflow-y: auto;
            overflow-x: auto;
            font-family: 'Courier New', Courier, monospace;
            font-size: 12px;
            line-height: 1.4;
            white-space: pre-wrap;
            word-wrap: break-word;
        }

        .sql-widget::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }

        .sql-widget::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 4px;
        }

        .sql-widget::-webkit-scrollbar-thumb {
            background: #888;
            border-radius: 4px;
        }

        .sql-widget::-webkit-scrollbar-thumb:hover {
            background: #555;
        }

        .footer {
            padding: 20px 30px;
            background: #f8f9fa;
            border-top: 1px solid #e9ecef;
            text-align: center;
            color: #6c757d;
            font-size: 14px;
        }

        .expand-collapse {
            color: #667eea;
            font-size: 14px;
            margin-bottom: 20px;
        }

        .expand-collapse button {
            background: none;
            border: none;
            color: #667eea;
            cursor: pointer;
            text-decoration: underline;
            font-size: 14px;
            padding: 5px 10px;
        }

        .expand-collapse button:hover {
            color: #764ba2;
        }
    </style>
    <script>
        function toggleCollapsible(element) {
            // Check if element is a button or a table header div
            if (element.tagName === 'BUTTON') {
                element.classList.toggle('collapsed');
                // Find the corresponding collapsible-content div
                let content = null;
                const parent = element.parentElement;

                // Check if collapsible-content is a sibling of the button (constraints/indexes/views/etc)
                // or a sibling of the parent div (columns)
                let hasSiblingContent = false;
                let sibling = element.nextElementSibling;
                while (sibling) {
                    if (sibling.classList && sibling.classList.contains('collapsible-content')) {
                        hasSiblingContent = true;
                        break;
                    }
                    sibling = sibling.nextElementSibling;
                }

                if (hasSiblingContent) {
                    // Content divs are siblings of buttons - match by index
                    const allButtons = [];
                    const allContents = [];
                    sibling = parent.firstElementChild;

                    while (sibling) {
                        if (sibling.tagName === 'BUTTON' && sibling.classList.contains('collapsible-toggle')) {
                            allButtons.push(sibling);
                        } else if (sibling.classList && sibling.classList.contains('collapsible-content')) {
                            allContents.push(sibling);
                        }
                        sibling = sibling.nextElementSibling;
                    }

                    const buttonIndex = allButtons.indexOf(element);
                    if (buttonIndex >= 0 && buttonIndex < allContents.length) {
                        content = allContents[buttonIndex];
                    }
                } else {
                    // Content div is sibling of parent (columns case)
                    content = parent.nextElementSibling;
                }

                if (content && content.classList.contains('collapsible-content')) {
                    content.classList.toggle('collapsed');
                }
            } else if (element.tagName === 'DIV') {
                // Handle table header clicks
                const schemaObject = element.querySelector('.schema-object');
                if (schemaObject) {
                    const isExpanded = schemaObject.textContent.startsWith('▼');
                    schemaObject.textContent = (isExpanded ? '▶' : '▼') + schemaObject.textContent.substring(1);
                }
                const content = element.nextElementSibling;
                if (content && content.classList.contains('collapsible-content')) {
                    content.classList.toggle('collapsed');
                }
            }
        }
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Database Comparison Report</h1>
            <div class="meta">
                <div>Database 1: <strong>{{ report.pg_connection1|e }}</strong></div>
                <div>Database 2: <strong>{{ report.pg_connection2|e }}</strong></div>
                <div>Generated: {{ report.timestamp.strftime('%Y-%m-%d %H:%M:%S')|e }}</div>
            </div>
        </div>

        <div class="summary">
            <div class="summary-card">
                <div class="label">Total Checks</div>
                <div class="value">{{ report.total_checks }}</div>
            </div>
            <div class="summary-card success">
                <div class="label">Passed</div>
                <div class="value">{{ report.passed_checks }}</div>
            </div>
            <div class="summary-card error">
                <div class="label">Failed</div>
                <div class="value">{{ report.failed_checks }}</div>
            </div>
            <div class="summary-card warning">
                <div class="label">Total Differences</div>
                <div class="value">{{ report.total_differences }}</div>
            </div>
        </div>

        <div class="content">
            <div class="expand-collapse">
                <button onclick="expandAll()">Expand All</button> |
                <button onclick="collapseAll()">Collapse All</button>
            </div>

            {% for result in report.check_results %}
            <div class="check-section">
                <div class="check-header" onclick="toggleSection('{{ result.key }}')">
                    <h2>
                        {{ result.name|e }}
                        <span class="badge {{ 'success' if result.passed else 'error' }}">
                            {{ 'PASSED' if result.passed else 'FAILED' }}
                        </span>
                    </h2>
                    <span class="diff-count">
                        {{ result.difference_count }} difference{{ 's' if result.difference_count != 1 else '' }}
                    </span>
                </div>
                <div id="{{ result.key }}" class="check-body{{ '' if not result.passed else ' collapsed' }}">
                    {% if not result.differences %}
                    <div class="no-differences">✓ No differences found</div>
                    {% else %}
                    {% if result.name == 'Columns' %}
                        {# Special handling for columns - group by table #}
                        {% set grouped = group_columns(result.differences) %}
                        {% for table, columns in grouped.items() %}
                        <div style="margin-bottom: 20px;">
                            <div style="margin-bottom: 10px; cursor: pointer;" onclick="toggleCollapsible(this)">
                                <span class="schema-object">▼ {{ table }}</span>
                            </div>
                            <div class="collapsible-content">
                                <ul class="diff-list">
                                    {% for col in columns.removed %}
                                    <li class="diff-item removed">
                                        <div class="diff-content">
                                            <span class="diff-marker">-</span>
                                            <span class="db-label tooltip">
                                                DB1
                                                <span class="tooltiptext">
                                                    ⚠️ Missing in <strong>{{ report.pg_connection2|e }}</strong><br>
                                                    Only exists in {{ report.pg_connection1|e }}
                                                </span>
                                            </span>
                                            <span class="object-detail">{{ col.column }}</span>
                                            {% if col.extra %}
                                                <button class="collapsible-toggle collapsed" onclick="toggleCollapsible(this); event.stopPropagation();">Details</button>
                                            {% endif %}
                                        </div>
                                        {% if col.extra %}
                                        <div class="collapsible-content collapsed">
                                            <div class="object-extra">{{ col.extra|e }}</div>
                                        </div>
                                        {% endif %}
                                    </li>
                                    {% endfor %}
                                    {% for col in columns.added %}
                                    <li class="diff-item added">
                                        <div class="diff-content">
                                            <span class="diff-marker">+</span>
                                            <span class="db-label tooltip">
                                                DB2
                                                <span class="tooltiptext">
                                                    ⚠️ Extra in <strong>{{ report.pg_connection2|e }}</strong><br>
                                                    Not present in {{ report.pg_connection1|e }}
                                                </span>
                                            </span>
                                            <span class="object-detail">{{ col.column }}</span>
                                            {% if col.extra %}
                                                <button class="collapsible-toggle collapsed" onclick="toggleCollapsible(this); event.stopPropagation();">Details</button>
                                            {% endif %}
                                        </div>
                                        {% if col.extra %}
                                        <div class="collapsible-content collapsed">
                                            <div class="object-extra">{{ col.extra|e }}</div>
                                        </div>
                                        {% endif %}
                                    </li>
                                    {% endfor %}
                                </ul>
                            </div>
                        </div>
                        {% endfor %}
                    {% else %}
                    <ul class="diff-list">
                        {% for diff in result.differences %}
                        {% set formatted = format_diff(diff.content, result.name) %}
                        <li class="diff-item {{ diff.type.value }}">
                            <div class="diff-content">
                                <span class="diff-marker">{{ '-' if diff.type.value == 'removed' else '+' }}</span>
                                <span class="db-label tooltip">
                                    {{ 'DB1' if diff.type.value == 'removed' else 'DB2' }}
                                    <span class="tooltiptext">
                                        {% if diff.type.value == 'removed' %}
                                        ⚠️ Missing in <strong>{{ report.pg_connection2|e }}</strong><br>
                                        Only exists in {{ report.pg_connection1|e }}
                                        {% else %}
                                        ⚠️ Extra in <strong>{{ report.pg_connection2|e }}</strong><br>
                                        Not present in {{ report.pg_connection1|e }}
                                        {% endif %}
                                    </span>
                                </span>
                                {% if formatted.is_structured %}
                                    <span class="schema-object">{{ formatted.schema_object }}</span>
                                    {% if formatted.detail %}
                                        <span class="object-detail">{{ formatted.detail }}</span>
                                    {% endif %}
                                    {% if formatted.extra %}
                                        <button class="collapsible-toggle collapsed" onclick="toggleCollapsible(this)">Details</button>
                                    {% endif %}
                                    {% if formatted.sql %}
                                        <button class="collapsible-toggle collapsed" onclick="toggleCollapsible(this)">Definition</button>
                                    {% endif %}
                                    {% if formatted.extra %}
                                        <div class="collapsible-content collapsed">
                                            <div class="object-extra">{{ formatted.extra|e }}</div>
                                        </div>
                                    {% endif %}
                                    {% if formatted.sql %}
                                        <div class="collapsible-content collapsed">
                                            <div class="sql-widget">{{ formatted.sql|e }}</div>
                                        </div>
                                    {% endif %}
                                {% else %}
                                    <span class="object-content">{{ formatted.content|e }}</span>
                                {% endif %}
                            </div>
                        </li>
                        {% endfor %}
                    </ul>
                    {% endif %}
                    {% endif %}
                </div>
            </div>
            {% endfor %}
        </div>

        <div class="footer">
            Generated by PUM Database Checker
        </div>
    </div>

    <script>
        function toggleSection(id) {
            const element = document.getElementById(id);
            element.classList.toggle('collapsed');
        }

        function expandAll() {
            document.querySelectorAll('.check-body').forEach(el => {
                el.classList.remove('collapsed');
            });
        }

        function collapseAll() {
            document.querySelectorAll('.check-body').forEach(el => {
                el.classList.add('collapsed');
            });
        }
    </script>
</body>
</html>"""

        template = Template(template_str)
        return template.render(
            report=report,
            format_diff=ReportGenerator._format_difference,
            group_columns=ReportGenerator._group_columns_by_table,
        )
