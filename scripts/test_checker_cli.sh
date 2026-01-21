#!/usr/bin/env bash

# Script to test the PUM checker CLI and generate HTML report
# This script sets up two test databases with different schemas and runs the checker

# Don't exit on error for the checker command (it returns 1 when differences found)
set -e

# Configuration
PG_SERVICE1="pum_test"
PG_SERVICE2="pum_test_2"
TEST_DIR="test/data/checker_test"
OUTPUT_FILE="checker_report.html"

echo "🔧 Setting up test databases..."

# Clean databases
psql service=$PG_SERVICE1 -c "DROP SCHEMA IF EXISTS pum_test_checker CASCADE; DROP TABLE IF EXISTS public.pum_migrations;" 2>/dev/null || true
psql service=$PG_SERVICE2 -c "DROP SCHEMA IF EXISTS pum_test_checker CASCADE; DROP TABLE IF EXISTS public.pum_migrations;" 2>/dev/null || true

# Install version 1.0.0 on both databases
echo "📦 Installing version 1.0.0 on both databases..."
pum -s $PG_SERVICE1 -d $TEST_DIR install --max-version 1.0.0
pum -s $PG_SERVICE2 -d $TEST_DIR install --max-version 1.0.0

# Upgrade first database to 1.1.0 to create differences (second stays at 1.0.0)
echo "⬆️  Upgrading $PG_SERVICE1 to version 1.1.0..."
pum -s $PG_SERVICE1 -d $TEST_DIR upgrade

# Run checker and generate HTML report (allow exit code 1 for differences found)
echo "🔍 Running checker and generating HTML report..."
set +e  # Don't exit on error for this command
pum -s $PG_SERVICE1 -d $TEST_DIR check $PG_SERVICE2 \
    -N public \
    -f html \
    -o $OUTPUT_FILE
CHECKER_EXIT=$?
set -e

if [ $CHECKER_EXIT -eq 0 ]; then
    echo "✅ No differences found (unexpected!)"
elif [ $CHECKER_EXIT -eq 1 ]; then
    echo "✅ Done! Differences found and HTML report generated."
    echo "📄 HTML report saved to: $OUTPUT_FILE"
    echo "🌐 Open it with: open $OUTPUT_FILE"
else
    echo "❌ Checker failed with exit code $CHECKER_EXIT"
    exit $CHECKER_EXIT
fi
