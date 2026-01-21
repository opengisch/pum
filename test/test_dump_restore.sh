#!/usr/bin/env bash

# Script to test the PUM dump and restore CLI commands
# This script sets up a test database, dumps it, restores to another database,
# and checks that they are identical

set -e

# Configuration
PG_SERVICE1="pum_test"
PG_SERVICE2="pum_test_2"
TEST_DIR="test/data/checker_test"
DUMP_FILE="/tmp/pum_test_dump.backup"

echo "🔧 Testing PUM dump and restore commands..."
echo ""

# Clean databases
echo "🧹 Cleaning test databases..."
psql service=$PG_SERVICE1 -c "DROP SCHEMA IF EXISTS pum_test_checker CASCADE; DROP TABLE IF EXISTS public.pum_migrations;" 2>/dev/null || true
psql service=$PG_SERVICE2 -c "DROP SCHEMA IF EXISTS pum_test_checker CASCADE; DROP TABLE IF EXISTS public.pum_migrations;" 2>/dev/null || true

# Install version 1.1.0 on first database
echo "📦 Installing version 1.1.0 on $PG_SERVICE1..."
pum -p $PG_SERVICE1 -d $TEST_DIR install
echo "✅ Installation complete"
echo ""

# Dump the first database
echo "💾 Dumping $PG_SERVICE1 to $DUMP_FILE..."
pum -p $PG_SERVICE1 -d $TEST_DIR dump -f custom -N public "$DUMP_FILE"
echo "✅ Dump complete"
echo ""

# Check that dump file exists and has content
if [ ! -f "$DUMP_FILE" ]; then
    echo "❌ Dump file not created!"
    exit 1
fi

FILE_SIZE=$(stat -f%z "$DUMP_FILE" 2>/dev/null || stat -c%s "$DUMP_FILE" 2>/dev/null)
echo "📊 Dump file size: $FILE_SIZE bytes"
echo ""

# Restore to the second database
echo "📥 Restoring dump to $PG_SERVICE2..."
pum -p $PG_SERVICE2 -d $TEST_DIR restore -N public "$DUMP_FILE"
echo "✅ Restore complete"
echo ""

# Run checker to verify databases are identical
echo "🔍 Running checker to verify databases are identical..."
set +e  # Don't exit on error for this command

pum -p $PG_SERVICE1 -d $TEST_DIR check $PG_SERVICE2 -N public
CHECKER_EXIT=$?

set -e

if [ $CHECKER_EXIT -eq 0 ]; then
    echo "✅ SUCCESS! Databases are identical after dump and restore."
    # Clean up dump file
    rm -f "$DUMP_FILE"
    exit 0
elif [ $CHECKER_EXIT -eq 1 ]; then
    echo "❌ FAIL! Differences found between databases after dump and restore."
    # Keep dump file for inspection
    echo "⚠️  Dump file kept at: $DUMP_FILE"
    exit 1
else
    echo "❌ Checker failed with exit code $CHECKER_EXIT"
    exit $CHECKER_EXIT
fi
