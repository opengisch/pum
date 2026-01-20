import difflib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

import psycopg


class DifferenceType(Enum):
    """Type of difference found."""

    ADDED = "added"
    REMOVED = "removed"


@dataclass
class DifferenceItem:
    """Represents a single difference between databases."""

    type: DifferenceType
    content: str

    def __str__(self) -> str:
        """String representation with marker."""
        marker = "+" if self.type == DifferenceType.ADDED else "-"
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

    pg_service1: str
    pg_service2: str
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
        pg_service1,
        pg_service2,
        exclude_schema=None,
        exclude_field_pattern=None,
        ignore_list=None,
    ):
        """Initialize the Checker.

        Args:
            pg_service1: The name of the postgres service (defined in pg_service.conf)
                related to the first db to be compared.
            pg_service2: The name of the postgres service (defined in pg_service.conf)
                related to the second db to be compared.
            exclude_schema: List of schemas to be ignored in check.
            exclude_field_pattern: List of field patterns to be ignored in check.
            ignore_list: List of elements to be ignored in check (ex. tables, columns,
                views, ...).
        """
        self.pg_service1 = pg_service1
        self.pg_service2 = pg_service2

        self.conn1 = psycopg.connect(f"service={pg_service1}")
        self.cur1 = self.conn1.cursor()

        self.conn2 = psycopg.connect(f"service={pg_service2}")
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
            pg_service1=self.pg_service1,
            pg_service2=self.pg_service2,
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
        with_query = None
        if check_views:
            with_query = rf"""WITH table_list AS (
                         SELECT table_schema, table_name
                         FROM information_schema.tables
                         WHERE table_schema NOT IN {self.exclude_schema}
                            AND table_schema NOT LIKE 'pg\_%'
                         ORDER BY table_schema,table_name
                         )"""

        else:
            with_query = rf"""WITH table_list AS (
                         SELECT table_schema, table_name
                         FROM information_schema.tables
                         WHERE table_schema NOT IN {self.exclude_schema}
                            AND table_schema NOT LIKE 'pg\_%'
                            AND table_type NOT LIKE 'VIEW'
                         ORDER BY table_schema,table_name
                         )"""

        query = """{wq}
                SELECT isc.table_schema, isc.table_name, column_name,
                    column_default, is_nullable, data_type,
                    character_maximum_length::text, numeric_precision::text,
                    numeric_precision_radix::text, datetime_precision::text
                FROM information_schema.columns isc,
                table_list tl
                WHERE isc.table_schema = tl.table_schema
                    AND isc.table_name = tl.table_name
                    {efp}
                ORDER BY isc.table_schema, isc.table_name, column_name
                """.format(
            wq=with_query,
            efp="".join(
                [f" AND column_name NOT LIKE '{pattern}'" for pattern in self.exclude_field_pattern]
            ),
        )

        return self.__check_equals(query)

    def check_constraints(self):
        """Check if the constraints are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the constraints are the same, False otherwise.
                - list: A list with the differences.
        """
        query = f""" select
                        tc.constraint_name,
                        tc.constraint_schema || '.' || tc.table_name || '.' ||
                            kcu.column_name as physical_full_name,
                        tc.constraint_schema,
                        tc.table_name,
                        kcu.column_name,
                        ccu.table_name as foreign_table_name,
                        ccu.column_name as foreign_column_name,
                        tc.constraint_type
                    from information_schema.table_constraints as tc
                    join information_schema.key_column_usage as kcu on
                        (tc.constraint_name = kcu.constraint_name and
                        tc.table_name = kcu.table_name)
                    join information_schema.constraint_column_usage as ccu on
                        ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_schema NOT IN {self.exclude_schema}
                    ORDER BY tc.constraint_schema, physical_full_name,
                        tc.constraint_name, foreign_table_name,
                        foreign_column_name  """

        return self.__check_equals(query)

    def check_views(self):
        """Check if the views are equals.

        Returns:
            tuple: A tuple containing:
                - bool: True if the views are the same, False otherwise.
                - list: A list with the differences.
        """
        query = rf"""
        SELECT table_name, REPLACE(view_definition,'"','')
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
        query = rf"""
        select
            t.relname as table_name,
            i.relname as index_name,
            a.attname as column_name,
            ns.nspname as schema_name
        from
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a,
            pg_namespace ns
        where
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and t.relnamespace = ns.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            AND t.relname NOT IN ('information_schema')
            AND t.relname NOT LIKE 'pg\_%'
            AND ns.nspname NOT IN {self.exclude_schema}
        order by
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

    def __check_equals(self, query) -> tuple[bool, list[DifferenceItem]]:
        """Check if the query results on the two databases are equals.

        Args:
            query: The SQL query to execute on both databases.

        Returns:
            tuple: A tuple containing:
                - bool: True if the results are the same, False otherwise.
                - list[DifferenceItem]: A list of DifferenceItem objects.
        """
        self.cur1.execute(query)
        records1 = self.cur1.fetchall()

        self.cur2.execute(query)
        records2 = self.cur2.fetchall()

        result = True
        differences = []

        d = difflib.Differ()
        records1 = [str(x) for x in records1]
        records2 = [str(x) for x in records2]

        for line in d.compare(records1, records2):
            if line[0] in ("-", "+"):
                result = False
                diff_type = DifferenceType.REMOVED if line[0] == "-" else DifferenceType.ADDED
                differences.append(
                    DifferenceItem(
                        type=diff_type,
                        content=line[2:],  # Skip the marker and space
                    )
                )

        return result, differences
