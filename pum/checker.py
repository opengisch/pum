from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import re

import psycopg

from .connection import format_connection_string


class DifferenceType(Enum):
    """Type of difference found."""

    ADDED = "added"
    REMOVED = "removed"


@dataclass
class DifferenceItem:
    """Represents a single difference between databases."""

    type: DifferenceType
    content: dict | str  # dict for structured data, str for backward compatibility

    def __str__(self) -> str:
        """String representation with marker."""
        marker = "+" if self.type == DifferenceType.ADDED else "-"
        if isinstance(self.content, dict):
            # For structured content, create a readable string
            return f"{marker} {self.content}"
        return f"{marker} {self.content}"


@dataclass
class CheckResult:
    """Result of a single check (e.g., tables, columns)."""

    name: str
    key: str
    passed: bool
    differences: list[DifferenceItem] = field(default_factory=list)

    @property
    def difference_count(self) -> int:
        """Number of differences found."""
        return len(self.differences)


@dataclass
class ComparisonReport:
    """Complete database comparison report."""

    pg_connection1: str
    pg_connection2: str
    timestamp: datetime
    check_results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Whether all checks passed."""
        return all(result.passed for result in self.check_results)

    @property
    def total_checks(self) -> int:
        """Total number of checks performed."""
        return len(self.check_results)

    @property
    def passed_checks(self) -> int:
        """Number of checks that passed."""
        return sum(1 for result in self.check_results if result.passed)

    @property
    def failed_checks(self) -> int:
        """Number of checks that failed."""
        return self.total_checks - self.passed_checks

    @property
    def total_differences(self) -> int:
        """Total number of differences across all checks."""
        return sum(result.difference_count for result in self.check_results)


class Checker:
    """This class is used to compare 2 Postgres databases and show the
    differences.
    """

    def __init__(
        self,
        pg_connection1,
        pg_connection2,
        exclude_schema=None,
        exclude_field_pattern=None,
        ignore_list=None,
    ):
        """Initialize the Checker.

        Args:
            pg_connection1: PostgreSQL service name or connection string for the first database.
                Can be a service name (e.g., 'mydb') or a full connection string
                (e.g., 'postgresql://user:pass@host/db' or 'host=localhost dbname=mydb').
            pg_connection2: PostgreSQL service name or connection string for the second database.
            exclude_schema: List of schemas to be ignored in check.
            exclude_field_pattern: List of field patterns to be ignored in check.
            ignore_list: List of elements to be ignored in check (ex. tables, columns,
                views, ...).
        """
        self.pg_connection1 = pg_connection1
        self.pg_connection2 = pg_connection2

        self.conn1 = psycopg.connect(format_connection_string(pg_connection1))
        self.cur1 = self.conn1.cursor()

        self.conn2 = psycopg.connect(format_connection_string(pg_connection2))
        self.cur2 = self.conn2.cursor()

        self.ignore_list = ignore_list or []
        self.exclude_schema = "('information_schema'"
        if exclude_schema is not None:
            for schema in exclude_schema:
                self.exclude_schema += f", '{schema}'"
        self.exclude_schema += ")"
        self.exclude_field_pattern = exclude_field_pattern or []

    def run_checks(self) -> ComparisonReport:
        """Run all the checks functions.

        Returns:
            Complete comparison report with all check results.
        """
        checks = [
            ("tables", "Tables", self.check_tables),
            ("columns", "Columns", lambda: self.check_columns("views" not in self.ignore_list)),
            ("constraints", "Constraints", self.check_constraints),
            ("views", "Views", self.check_views),
            ("sequences", "Sequences", self.check_sequences),
            ("indexes", "Indexes", self.check_indexes),
            ("triggers", "Triggers", self.check_triggers),
            ("functions", "Functions", self.check_functions),
            ("rules", "Rules", self.check_rules),
        ]

        check_results = []
        for check_key, check_name, check_func in checks:
            if check_key not in self.ignore_list:
                passed, differences = check_func()
                check_results.append(
                    CheckResult(
                        name=check_name,
                        key=check_key,
                        passed=passed,
                        differences=differences,
                    )
                )

        return ComparisonReport(
            pg_connection1=self.pg_connection1,
            pg_connection2=self.pg_connection2,
            timestamp=datetime.now(),
            check_results=check_results,
        )

    def check_tables(self):
        """Check if the tables are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the tables are the same, False otherwise.
                - list: A list with the differences.
        """
        query = rf"""SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN {self.exclude_schema}
                    AND table_schema NOT LIKE 'pg\_%'
                    AND table_type NOT LIKE 'VIEW'
                ORDER BY table_schema, table_name
                """

        return self.__check_equals(query)

    def check_columns(self, check_views=True):
        """Check if the columns in all tables are equals.

        Args:
            check_views: If True, check the columns of all the tables and views,
                if False check only the columns of the tables.

        Returns:
            tuple: A tuple containing:
                - bool: True if the columns are the same, False otherwise.
                - list: A list with the differences.
        """
        # First, get the list of tables that exist in BOTH databases
        # to avoid reporting columns from tables that don't exist in one DB
        if check_views:
            table_query = rf"""SELECT table_schema, table_name
                         FROM information_schema.tables
                         WHERE table_schema NOT IN {self.exclude_schema}
                            AND table_schema NOT LIKE 'pg\_%'
                         ORDER BY table_schema,table_name
                         """
        else:
            table_query = rf"""SELECT table_schema, table_name
                         FROM information_schema.tables
                         WHERE table_schema NOT IN {self.exclude_schema}
                            AND table_schema NOT LIKE 'pg\_%'
                            AND table_type NOT LIKE 'VIEW'
                         ORDER BY table_schema,table_name
                         """

        # Get tables from both databases
        self.cur1.execute(table_query)
        tables1 = set(self.cur1.fetchall())

        self.cur2.execute(table_query)
        tables2 = set(self.cur2.fetchall())

        # Only check columns for tables that exist in both databases
        common_tables = tables1.intersection(tables2)

        if not common_tables:
            # No common tables, so no columns to compare
            return True, []

        # Build the WHERE clause to only include common tables
        table_conditions = " OR ".join(
            [
                f"(isc.table_schema = '{schema}' AND isc.table_name = '{table}')"
                for schema, table in common_tables
            ]
        )

        query = f"""
                SELECT isc.table_schema, isc.table_name, column_name,
                    column_default, is_nullable, data_type,
                    character_maximum_length::text, numeric_precision::text,
                    numeric_precision_radix::text, datetime_precision::text
                FROM information_schema.columns isc
                WHERE ({table_conditions})
                    {("".join([f" AND column_name NOT LIKE '{pattern}'" for pattern in self.exclude_field_pattern]))}
                ORDER BY isc.table_schema, isc.table_name, column_name
                """

        return self.__check_equals(query)

    def check_constraints(self):
        """Check if the constraints are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the constraints are the same, False otherwise.
                - list: A list with the differences.
        """
        # Get tables from both databases to filter constraints
        table_query = f"""SELECT table_schema, table_name
                         FROM information_schema.tables
                         WHERE table_schema NOT IN {self.exclude_schema}
                            AND table_schema NOT LIKE 'pg\\_%'
                            AND table_type NOT LIKE 'VIEW'
                         ORDER BY table_schema,table_name
                         """

        self.cur1.execute(table_query)
        tables1 = set(self.cur1.fetchall())

        self.cur2.execute(table_query)
        tables2 = set(self.cur2.fetchall())

        # Only check constraints for tables that exist in both databases
        common_tables = tables1.intersection(tables2)

        if not common_tables:
            return True, []

        # Build the WHERE clause to only include common tables
        table_conditions = " OR ".join(
            [
                f"(tc.constraint_schema = '{schema}' AND tc.table_name = '{table}')"
                for schema, table in common_tables
            ]
        )

        # Build WHERE clause for CHECK constraints
        check_table_conditions = " OR ".join(
            [
                f"(n.nspname = '{schema}' AND cl.relname = '{table}')"
                for schema, table in common_tables
            ]
        )

        # Query for KEY constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE)
        key_query = f"""
                    SELECT
                        tc.constraint_name,
                        tc.constraint_schema || '.' || tc.table_name || '.' ||
                            kcu.column_name as physical_full_name,
                        tc.constraint_schema,
                        tc.table_name,
                        kcu.column_name,
                        ccu.table_name as foreign_table_name,
                        ccu.column_name as foreign_column_name,
                        tc.constraint_type,
                        pg_get_constraintdef((
                            SELECT con.oid FROM pg_constraint con
                            JOIN pg_namespace nsp ON con.connamespace = nsp.oid
                            WHERE con.conname = tc.constraint_name
                            AND nsp.nspname = tc.constraint_schema
                            LIMIT 1
                        )) as constraint_definition
                    FROM information_schema.table_constraints as tc
                    JOIN information_schema.key_column_usage as kcu ON
                        (tc.constraint_name = kcu.constraint_name AND
                        tc.table_name = kcu.table_name)
                    JOIN information_schema.constraint_column_usage as ccu ON
                        ccu.constraint_name = tc.constraint_name
                    WHERE ({table_conditions})
                    ORDER BY tc.constraint_schema, physical_full_name,
                        tc.constraint_name, foreign_table_name,
                        foreign_column_name
                    """

        # Query for CHECK constraints (they don't appear in key_column_usage)
        check_query = f"""
                    SELECT
                        c.conname as constraint_name,
                        n.nspname || '.' || cl.relname as physical_full_name,
                        n.nspname as constraint_schema,
                        cl.relname as table_name,
                        '' as column_name,
                        '' as foreign_table_name,
                        '' as foreign_column_name,
                        'CHECK' as constraint_type,
                        pg_get_constraintdef(c.oid) as constraint_definition
                    FROM pg_constraint c
                    JOIN pg_class cl ON c.conrelid = cl.oid
                    JOIN pg_namespace n ON cl.relnamespace = n.oid
                    WHERE c.contype = 'c'
                        AND ({check_table_conditions})
                    ORDER BY n.nspname, cl.relname, c.conname
                    """

        # Normalization function for constraint records
        def normalize_constraint_record(record_dict, col_names):
            """Normalize constraint definitions in a record."""
            normalized = record_dict.copy()
            if "constraint_definition" in normalized and normalized["constraint_definition"]:
                normalized["constraint_definition"] = self.__normalize_constraint_definition(
                    normalized["constraint_definition"]
                )
            return normalized

        # Execute both queries and combine results
        passed_keys, diffs_keys = self.__check_equals(
            key_query, normalize_func=normalize_constraint_record
        )
        passed_checks, diffs_checks = self.__check_equals(
            check_query, normalize_func=normalize_constraint_record
        )

        return (passed_keys and passed_checks, diffs_keys + diffs_checks)

    def check_views(self):
        """Check if the views are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the views are the same, False otherwise.
                - list: A list with the differences.
        """
        query = rf"""
        SELECT table_schema, table_name, REPLACE(view_definition,'"','')
        FROM INFORMATION_SCHEMA.views
        WHERE table_schema NOT IN {self.exclude_schema}
        AND table_schema NOT LIKE 'pg\_%'
        AND table_name not like 'vw_export_%'
        ORDER BY table_schema, table_name
        """

        return self.__check_equals(query)

    def check_sequences(self):
        """Check if the sequences are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the sequences are the same, False otherwise.
                - list: A list with the differences.
        """
        query = f"""
        SELECT c.relname,
               ns.nspname as schema_name
        FROM pg_class c
        JOIN pg_namespace ns ON c.relnamespace = ns.oid
        WHERE c.relkind = 'S'
              AND ns.nspname NOT IN {self.exclude_schema}
        ORDER BY c.relname"""

        return self.__check_equals(query)

    def check_indexes(self):
        """Check if the indexes are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the indexes are the same, False otherwise.
                - list: A list with the differences.
        """
        # Get tables from both databases to filter indexes
        table_query = f"""SELECT table_schema, table_name
                         FROM information_schema.tables
                         WHERE table_schema NOT IN {self.exclude_schema}
                            AND table_schema NOT LIKE 'pg\\_%'
                            AND table_type NOT LIKE 'VIEW'
                         ORDER BY table_schema,table_name
                         """

        self.cur1.execute(table_query)
        tables1 = set(self.cur1.fetchall())

        self.cur2.execute(table_query)
        tables2 = set(self.cur2.fetchall())

        # Only check indexes for tables that exist in both databases
        common_tables = tables1.intersection(tables2)

        if not common_tables:
            return True, []

        # Build the WHERE clause to only include common tables
        table_conditions = " OR ".join(
            [
                f"(ns.nspname = '{schema}' AND t.relname = '{table}')"
                for schema, table in common_tables
            ]
        )

        query = rf"""
        SELECT
            ns.nspname as schema_name,
            t.relname as table_name,
            i.relname as index_name,
            a.attname as column_name,
            pg_get_indexdef(i.oid) as index_definition
        FROM
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a,
            pg_namespace ns
        WHERE
            t.oid = ix.indrelid
            AND i.oid = ix.indexrelid
            AND a.attrelid = t.oid
            AND t.relnamespace = ns.oid
            AND a.attnum = ANY(ix.indkey)
            AND t.relkind = 'r'
            AND ({table_conditions})
        ORDER BY
            ns.nspname,
            t.relname,
            i.relname,
            a.attname
        """
        return self.__check_equals(query)

    def check_triggers(self):
        """Check if the triggers are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the triggers are the same, False otherwise.
                - list: A list with the differences.
        """
        query = f"""
        WITH trigger_list AS (
            select tgname, tgisinternal from pg_trigger
            GROUP BY tgname, tgisinternal
        )
        select ns.nspname as schema_name, p.relname, t.tgname, pp.prosrc
        from pg_trigger t, pg_proc pp, trigger_list tl, pg_class p, pg_namespace ns
        where pp.oid = t.tgfoid
            and t.tgname = tl.tgname
            AND t.tgrelid = p.oid
            AND p.relnamespace = ns.oid
            AND NOT tl.tgisinternal
            AND ns.nspname NOT IN {self.exclude_schema}
        ORDER BY p.relname, t.tgname, pp.prosrc"""

        return self.__check_equals(query)

    def check_functions(self):
        """Check if the functions are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the functions are the same, False otherwise.
                - list: A list with the differences.
        """
        query = rf"""
        SELECT routines.routine_schema, routines.routine_name, parameters.data_type,
            routines.routine_definition
        FROM information_schema.routines
        LEFT JOIN information_schema.parameters
        ON routines.specific_name=parameters.specific_name
        WHERE routines.specific_schema NOT IN {self.exclude_schema}
            AND routines.specific_schema NOT LIKE 'pg\_%'
            AND routines.specific_schema <> 'information_schema'
        ORDER BY routines.routine_name, parameters.data_type,
            routines.routine_definition, parameters.ordinal_position
            """

        return self.__check_equals(query)

    def check_rules(self):
        """Check if the rules are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the rules are the same, False otherwise.
                - list: A list with the differences.
        """
        query = rf"""
        select n.nspname as rule_schema,
        c.relname as rule_table,
        r.rulename as rule_name,
        case r.ev_type
            when '1' then 'SELECT'
            when '2' then 'UPDATE'
            when '3' then 'INSERT'
            when '4' then 'DELETE'
            else 'UNKNOWN'
        end as rule_event
        from pg_rewrite r
        join pg_class c on r.ev_class = c.oid
        left join pg_namespace n on n.oid = c.relnamespace
        left join pg_description d on r.oid = d.objoid
        WHERE n.nspname NOT IN {self.exclude_schema}
            AND n.nspname NOT LIKE 'pg\_%'
        ORDER BY n.nspname, c.relname, r.rulename, rule_event
        """

        return self.__check_equals(query)

    @staticmethod
    def __normalize_constraint_definition(definition: str) -> str:
        """Normalize a constraint definition for comparison.

        PostgreSQL may represent functionally equivalent constraints differently,
        especially after dump/restore operations. This function normalizes common
        variations to enable accurate comparison.

        Args:
            definition: The constraint definition string from pg_get_constraintdef()

        Returns:
            Normalized constraint definition
        """
        if not definition:
            return definition

        # Normalize different ARRAY representations:
        # Before: (ARRAY['a'::type, 'b'::type])::type[] OR ARRAY[('a'::type)::text, ...]
        # After: Canonical form based on sorted elements

        # Strategy: Extract the constraint type and key values, ignoring formatting details
        # For ANY/ALL with arrays, extract just the operator and the array values

        # Remove extra parentheses around ARRAY expressions
        # (ARRAY[...])::type[] -> ARRAY[...]::type[]
        definition = re.sub(r"\(\(ARRAY\[(.*?)\]\)::(.*?)\[\]\)", r"ARRAY[\1]::\2[]", definition)

        # Also remove parentheses without cast: (ARRAY[...]) -> ARRAY[...]
        definition = re.sub(r"\(ARRAY\[([^\]]+)\]\)", r"ARRAY[\1]", definition)

        # Normalize array element casts: ('value'::type1)::type2 -> 'value'::type1
        # This handles the case where elements are double-cast
        definition = re.sub(r"\('([^']+)'::([^)]+)\)::(\w+)", r"'\1'::\2", definition)

        # Remove trailing array cast that may be present or absent: ::text[] or ::character varying[]
        # This is safe because the type information is already in each array element
        definition = re.sub(r"::(?:text|character varying)\[\]", "", definition)

        # Remove extra whitespace and normalize spacing
        definition = re.sub(r"\s+", " ", definition).strip()

        return definition

    def __check_equals(self, query, normalize_func=None) -> tuple[bool, list[DifferenceItem]]:
        """Check if the query results on the two databases are equals.

        Args:
            query: The SQL query to execute on both databases.
            normalize_func: Optional function to normalize specific fields in records.
                Should accept (dict, col_names) and return normalized dict.

        Returns:
            tuple: A tuple containing:
                - bool: True if the results are the same, False otherwise.
                - list[DifferenceItem]: A list of DifferenceItem objects with structured data.
        """
        self.cur1.execute(query)
        records1 = self.cur1.fetchall()

        self.cur2.execute(query)
        records2 = self.cur2.fetchall()

        result = True
        differences = []

        # Convert records to dictionaries based on column names
        col_names = [desc[0] for desc in self.cur1.description]

        # Create structured records
        structured1 = [dict(zip(col_names, record)) for record in records1]
        structured2 = [dict(zip(col_names, record)) for record in records2]

        # Apply normalization if provided
        if normalize_func:
            structured1 = [normalize_func(r, col_names) for r in structured1]
            structured2 = [normalize_func(r, col_names) for r in structured2]
            # Recreate records from normalized structured data
            records1 = [tuple(r[col] for col in col_names) for r in structured1]
            records2 = [tuple(r[col] for col in col_names) for r in structured2]

        # Create sets for comparison
        set1 = {str(tuple(r)) for r in records1}
        set2 = {str(tuple(r)) for r in records2}

        # Find differences
        removed = set1 - set2
        added = set2 - set1

        if removed or added:
            result = False

            # Map string representations back to structured data
            str_to_struct1 = {str(tuple(r)): s for r, s in zip(records1, structured1)}
            str_to_struct2 = {str(tuple(r)): s for r, s in zip(records2, structured2)}

            # Add removed items
            for item_str in removed:
                if item_str in str_to_struct1:
                    differences.append(
                        DifferenceItem(
                            type=DifferenceType.REMOVED,
                            content=str_to_struct1[item_str],
                        )
                    )

            # Add added items
            for item_str in added:
                if item_str in str_to_struct2:
                    differences.append(
                        DifferenceItem(
                            type=DifferenceType.ADDED,
                            content=str_to_struct2[item_str],
                        )
                    )

        return result, differences
