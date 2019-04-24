# -*- coding: utf-8 -*-

from __future__ import print_function
import psycopg2
import psycopg2.extras
import difflib


class Checker:
    """This class is used to compare 2 Postgres databases and show the
    differences."""

    def __init__(
            self, pg_service1, pg_service2,
            exclude_schema=None, ignore_list=None, verbose_level=1):
        """Constructor

        Parameters
        ----------
        pg_service1: str
            The name of the postgres service (defined in pg_service.conf)
            related to the first db to be compared
        pg_service2: str
            The name of the postgres service (defined in pg_service.conf)
            related to the first db to be compared
        ignore_list: list(str)
            List of elements to be ignored in check (ex. tables, columns,
            views, ...)
        exclude_schema: list of strings
            List of schemas to be ignored in check.
        verbose_level: int
            verbose level, 0 -> nothing, 1 -> print first 80 char of each
            difference, 2 -> print all the difference details
        """

        self.conn1 = psycopg2.connect("service={0}".format(pg_service1))
        self.cur1 = self.conn1.cursor()

        self.conn2 = psycopg2.connect("service={0}".format(pg_service2))
        self.cur2 = self.conn2.cursor()

        self.ignore_list = ignore_list
        self.exclude_schema = "('information_schema'"
        if exclude_schema is not None:
            for schema in exclude_schema:
                self.exclude_schema += ", '{}'".format(schema)
        self.exclude_schema += ")"

        self.verbose_level = verbose_level

    def run_checks(self):
        """Run all the checks functions.

            Returns
            -------
            bool
                True if all the checks are true
                False otherwise
            dict
                Dictionary of lists of differences
        """

        result = True
        differences_dict = {}

        if 'tables' not in self.ignore_list:
            tmp_result, differences_dict['tables'] = self.check_tables()
            result = False if not tmp_result else result
        if 'columns' not in self.ignore_list:
            tmp_result, differences_dict['columns'] = self.check_columns(
                'views' not in self.ignore_list)
            result = False if not tmp_result else result
        if 'constraints' not in self.ignore_list:
            tmp_result, differences_dict['constraints'] = \
                self.check_constraints()
            result = False if not tmp_result else result
        if 'views' not in self.ignore_list:
            tmp_result, differences_dict['views'] = self.check_views()
            result = False if not tmp_result else result
        if 'sequences' not in self.ignore_list:
            tmp_result, differences_dict['sequences'] = self.check_sequences()
            result = False if not tmp_result else result
        if 'indexes' not in self.ignore_list:
            tmp_result, differences_dict['indexes'] = self.check_indexes()
            result = False if not tmp_result else result
        if 'triggers' not in self.ignore_list:
            tmp_result, differences_dict['triggers'] = self.check_triggers()
            result = False if not tmp_result else result
        if 'functions' not in self.ignore_list:
            tmp_result, differences_dict['functions'] = self.check_functions()
            result = False if not tmp_result else result
        if 'rules' not in self.ignore_list:
            tmp_result, differences_dict['rules'] = self.check_rules()
            result = False if not tmp_result else result
        if self.verbose_level == 0:
            differences_dict = None
        return result, differences_dict

    def check_tables(self):
        """Check if the tables are equals.

            Returns
            -------
            bool
                True if the tables are the same
                False otherwise
            list
                A list with the differences
        """
        query = """SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN {}
                    AND table_schema NOT LIKE 'pg\_%'
                    AND table_type NOT LIKE 'VIEW'
                ORDER BY table_schema, table_name
                """.format(self.exclude_schema)

        return self.__check_equals(query)

    def check_columns(self, check_views=True):
        """Check if the columns in all tables are equals.

            Parameters
            ----------
            check_views: bool
                if True, check the columns of all the tables and views, if
                False check only the columns of the tables

            Returns
            -------
            bool
                True if the columns are the same
                False otherwise
            list
                A list with the differences
        """
        if check_views:
            query = """WITH table_list AS (
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN {}
                    AND table_schema NOT LIKE 'pg\_%'
                ORDER BY table_schema,table_name
                )
                SELECT isc.table_schema, isc.table_name, column_name,
                    column_default, is_nullable, data_type,
                    character_maximum_length::text, numeric_precision::text,
                    numeric_precision_radix::text, datetime_precision::text
                FROM information_schema.columns isc,
                table_list tl
                WHERE isc.table_schema = tl.table_schema
                    AND isc.table_name = tl.table_name
                ORDER BY isc.table_schema, isc.table_name, column_name
                """.format(self.exclude_schema)

        else:
            query = """WITH table_list AS (
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN {}
                    AND table_schema NOT LIKE 'pg\_%'
                    AND table_type NOT LIKE 'VIEW'
                ORDER BY table_schema,table_name
                )
                SELECT isc.table_schema, isc.table_name, column_name,
                    column_default, is_nullable, data_type,
                    character_maximum_length::text, numeric_precision::text,
                    numeric_precision_radix::text, datetime_precision::text
                FROM information_schema.columns isc,
                table_list tl
                WHERE isc.table_schema = tl.table_schema
                    AND isc.table_name = tl.table_name
                ORDER BY isc.table_schema, isc.table_name, column_name
                """.format(self.exclude_schema)

        return self.__check_equals(query)

    def check_constraints(self):
        """Check if the constraints are equals.

            Returns
            -------
            bool
                True if the constraints are the same
                False otherwise
            list
                A list with the differences
        """
        query = """ select
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
                    ORDER BY tc.constraint_schema, physical_full_name,
                        tc.constraint_name, foreign_table_name,
                        foreign_column_name  """

        return self.__check_equals(query)

    def check_views(self):
        """Check if the views are equals.

            Returns
            -------
            bool
                True if the views are the same
                False otherwise
            list
                A list with the differences
        """
        query = """
        SELECT table_name, REPLACE(view_definition,'"','')
        FROM INFORMATION_SCHEMA.views
        WHERE table_schema NOT IN {}
        AND table_schema NOT LIKE 'pg\_%'
        AND table_name not like 'vw_export_%'
        ORDER BY table_schema, table_name
        """.format(self.exclude_schema)

        return self.__check_equals(query)

    def check_sequences(self):
        """Check if the sequences are equals.

            Returns
            -------
            bool
                True if the sequences are the same
                False otherwise
            list
                A list with the differences
        """
        query = """
        SELECT c.relname
        FROM pg_class c
        WHERE c.relkind = 'S'
        ORDER BY c.relname"""

        return self.__check_equals(query)

    def check_indexes(self):
        """Check if the indexes are equals.

            Returns
            -------
            bool
                True if the indexes are the same
                False otherwise
            list
                A list with the differences
        """

        query = """
        select
            t.relname as table_name,
            i.relname as index_name,
            a.attname as column_name
        from
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        where
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            AND t.relname NOT IN ('information_schema')
            AND t.relname NOT LIKE 'pg\_%'
        order by
            t.relname,
            i.relname,
            a.attname
        """
        return self.__check_equals(query)

    def check_triggers(self) -> dict:
        """Check if the triggers are equals.

            Returns
            -------
            bool
                True if the triggers are the same
                False otherwise
            list
                A list with the differences
        """
        query = """
        WITH trigger_list AS (
            select tgname from pg_trigger
            GROUP BY tgname
        )
        select pp.prosrc, p.relname
        from pg_trigger t, pg_proc pp, trigger_list tl, pg_class p
        where pp.oid = t.tgfoid
            and t.tgname = tl.tgname
            AND t.tgrelid = p.oid
            and  SUBSTR(p.relname, 1, 3) != 'vw_'
            -- We cannot check for vw_ views,
            -- because they are created after that script
        ORDER BY p.relname, /*t.tgname, */pp.prosrc"""

        return self.__check_equals(query)

    def check_functions(self):
        """Check if the functions are equals.

            Returns
            -------
            bool
                True if the functions are the same
                False otherwise
            list
                A list with the differences
        """
        query = """
        SELECT routines.routine_name, parameters.data_type,
            routines.routine_definition
        FROM information_schema.routines
        JOIN information_schema.parameters
        ON routines.specific_name=parameters.specific_name
        WHERE routines.specific_schema NOT IN {}
            AND routines.specific_schema NOT LIKE 'pg\_%'
        ORDER BY routines.routine_name, parameters.data_type,
            routines.routine_definition, parameters.ordinal_position
            """.format(self.exclude_schema)

        return self.__check_equals(query)

    def check_rules(self):
        """Check if the rules are equals.

            Returns
            -------
            bool
                True if the rules are the same
                False otherwise
            list
                A list with the differences
        """
        query = """
        select n.nspname as rule_schema,
        c.relname as rule_table,
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
        WHERE n.nspname NOT IN {excl}
            AND r.rulename != '_RETURN'
            AND n.nspname NOT LIKE 'pg\_%'
        ORDER BY n.nspname, c.relname, rule_event
        """.format(excl=self.exclude_schema)

        return self.__check_equals(query)

    def __check_equals(self, query):
        """Check if the query results on the two databases are equals.

            Returns
            -------
            bool
                True if the results are the same
                False otherwise
            list
                A list with the differences
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
            if line[0] in ('-', '+'):
                result = False
                if self.verbose_level == 1:
                    differences.append(line[0:79])
                elif self.verbose_level == 2:
                    differences.append(line)

        return result, differences
