"""Report generation for database comparison results."""

try:
    from jinja2 import Environment, BaseLoader, Template

    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

from .checker import ComparisonReport


class ReportGenerator:
    """Generates reports from ComparisonReport objects."""

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
            width: 20px;
            font-weight: bold;
            margin-right: 10px;
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
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Database Comparison Report</h1>
            <div class="meta">
                <div>Database 1: <strong>{{ report.pg_service1|e }}</strong></div>
                <div>Database 2: <strong>{{ report.pg_service2|e }}</strong></div>
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
                    <ul class="diff-list">
                        {% for diff in result.differences %}
                        <li class="diff-item {{ diff.type.value }}">
                            <span class="diff-marker">{{ '+' if diff.type.value == 'added' else '-' }}</span>{{ diff.content|e }}
                        </li>
                        {% endfor %}
                    </ul>
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
        return template.render(report=report)
