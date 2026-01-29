#!/usr/bin/env bash

# Script to test the PUM checker CLI and generate example reports in all formats
# This script sets up two test databases with different schemas and runs the checker

# Don't exit on error for the checker command (it returns 1 when differences found)
set -e

# Configuration
PG_SERVICE1="pum_test"
PG_SERVICE2="pum_test_2"
TEST_DIR="test/data/checker_test"
DOCS_DIR="docs/docs/assets/examples"
OUTPUT_TEXT="$DOCS_DIR/checker_report.txt"
OUTPUT_JSON="$DOCS_DIR/checker_report.json"
OUTPUT_HTML="$DOCS_DIR/checker_report.html"

# Create docs directory if it doesn't exist
mkdir -p "$DOCS_DIR"

echo "🔧 Setting up test databases..."

# Clean databases
psql service=$PG_SERVICE1 -c "DROP SCHEMA IF EXISTS pum_test_checker CASCADE; DROP TABLE IF EXISTS public.pum_migrations;" 2>/dev/null || true
psql service=$PG_SERVICE2 -c "DROP SCHEMA IF EXISTS pum_test_checker CASCADE; DROP TABLE IF EXISTS public.pum_migrations;" 2>/dev/null || true

# Install version 1.0.0 on both databases
echo "📦 Installing version 1.0.0 on both databases..."
pum -p $PG_SERVICE1 -d $TEST_DIR install --max-version 1.0.0
pum -p $PG_SERVICE2 -d $TEST_DIR install --max-version 1.0.0

# Upgrade first database to 1.1.0 to create differences (second stays at 1.0.0)
echo "⬆️  Upgrading $PG_SERVICE1 to version 1.1.0..."
pum -p $PG_SERVICE1 -d $TEST_DIR upgrade

# Run checker and generate reports in all formats
echo "🔍 Running checker and generating reports in all formats..."
set +e  # Don't exit on error for this command

# Text format
echo "  📝 Generating text report..."
pum -p $PG_SERVICE1 -d $TEST_DIR check $PG_SERVICE2 \
    -N public \
    -f text \
    -o $OUTPUT_TEXT
CHECKER_EXIT=$?

# JSON format
echo "  📊 Generating JSON report..."
pum -p $PG_SERVICE1 -d $TEST_DIR check $PG_SERVICE2 \
    -N public \
    -f json \
    -o $OUTPUT_JSON

# HTML format
echo "  🌐 Generating HTML report..."
pum -p $PG_SERVICE1 -d $TEST_DIR check $PG_SERVICE2 \
    -N public \
    -f html \
    -o $OUTPUT_HTML

set -e

if [ $CHECKER_EXIT -eq 0 ]; then
    echo "✅ No differences found (unexpected!)"
elif [ $CHECKER_EXIT -eq 1 ]; then
    echo "✅ Done! Differences found and reports generated."
    echo "📄 Reports saved to:"
    echo "   - Text: $OUTPUT_TEXT"
    echo "   - JSON: $OUTPUT_JSON"
    echo "   - HTML: $OUTPUT_HTML"
    echo "🌐 Open HTML with: open $OUTPUT_HTML"
else
    echo "❌ Checker failed with exit code $CHECKER_EXIT"
    exit $CHECKER_EXIT
fi
