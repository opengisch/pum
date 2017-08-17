#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import psycopg2
import psycopg2.extras
import difflib

class Checker():
    """This class is used to compare 2 Postgres databases and show the differences."""

    def __init__(self, pg_service1, pg_service2, silent=False):
        """Constructor
        
        Parameters
        ----------
        pg_service1: string
            The name of the postgres service (defined in pg_service.conf) related to the first db to be compared
        pg_service2: sting
            The name of the postgres service (defined in pg_service.conf) related to the first db to be compared
        """

        self.conn1 = psycopg2.connect("service={0}".format(pg_service1))
        self.cur1 = self.conn1.cursor()

        self.conn2 = psycopg2.connect("service={0}".format(pg_service2))
        self.cur2 = self.conn2.cursor()

        self.silent = silent

    def check_tables(self):
        """Check if the tables are equals.

            Returns
            -------
            bool
                True if the tables are the same
                False otherwise            
        """
        query = """SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema') 
                    AND table_schema NOT LIKE 'pg\_%' 
                    AND table_type NOT LIKE 'VIEW' 
                ORDER BY table_schema, table_name"""

        return self.__check_equals(query, 'Tables diff:')

    def check_columns(self, check_views = True):
        """Check if the columns in all tables are equals.
            
            Parameters
            ----------
            check_views: bool
                if True, check the columns of all the tables and views, if False check only the columns of the tables

            Returns
            -------
            bool
                True if the columns are the same
                False otherwise            
        """
        if check_views:
            query = """WITH table_list AS ( 
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema') 
                    AND table_schema NOT LIKE 'pg\_%' 
                ORDER BY table_schema,table_name 
                ) 
                SELECT isc.table_schema, isc.table_name, column_name, column_default, is_nullable, 
                data_type, character_maximum_length::text, numeric_precision::text, 
                numeric_precision_radix::text, datetime_precision::text FROM information_schema.columnS isc,
                table_list tl 
                WHERE isc.table_schema = tl.table_schema 
                    AND isc.table_name = tl.table_name 
                ORDER BY isc.table_schema, isc.table_name, column_name"""

        else:
            query = """WITH table_list AS ( 
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema') 
                    AND table_schema NOT LIKE 'pg\_%'
                    AND table_type NOT LIKE 'VIEW'
                ORDER BY table_schema,table_name 
                ) 
                SELECT isc.table_schema, isc.table_name, column_name, column_default, is_nullable, 
                data_type, character_maximum_length::text, numeric_precision::text, 
                numeric_precision_radix::text, datetime_precision::text FROM information_schema.columnS isc,
                table_list tl 
                WHERE isc.table_schema = tl.table_schema 
                    AND isc.table_name = tl.table_name 
                ORDER BY isc.table_schema, isc.table_name, column_name"""

        return self.__check_equals(query, 'Columns diff:')

    def check_constraints(self):
        """Check if the constraints are equals.

            Returns
            -------
            bool
                True if the constraints are the same
                False otherwise            
        """
        query = """ select
                        tc.constraint_name,
                        tc.constraint_schema || '.' || tc.table_name || '.' || kcu.column_name as physical_full_name,
                        tc.constraint_schema,
                        tc.table_name,
                        kcu.column_name,
                        ccu.table_name as foreign_table_name,
                        ccu.column_name as foreign_column_name,
                        tc.constraint_type
                    from information_schema.table_constraints as tc
                    join information_schema.key_column_usage as kcu on (tc.constraint_name = kcu.constraint_name and tc.table_name = kcu.table_name)
                    join information_schema.constraint_column_usage as ccu on ccu.constraint_name = tc.constraint_name
                    ORDER BY tc.constraint_schema, physical_full_name, tc.constraint_name, foreign_table_name, foreign_column_name  """

        return self.__check_equals(query, 'Constraints diff:')

    def check_views(self):
        """Check if the views are equals.

            Returns
            -------
            bool
                True if the views are the same
                False otherwise            
        """
        query = """
        SELECT table_name, REPLACE(view_definition,'"','')
        FROM INFORMATION_SCHEMA.views
        WHERE table_schema NOT IN ('information_schema') AND table_schema NOT LIKE 'pg\_%' 
        AND table_name not like 'vw_export_%'
        ORDER BY table_schema, table_name"""

        return self.__check_equals(query, 'Views diff:')


    def check_sequences(self):
        """Check if the sequences are equals.

            Returns
            -------
            bool
                True if the sequences are the same
                False otherwise            
        """
        query = """
        SELECT c.relname
        FROM pg_class c
        WHERE c.relkind = 'S'
        ORDER BY c.relname"""

        return self.__check_equals(query, 'Sequences diff:')


    def check_indexes(self):
        """Check if the indexes are equals.

            Returns
            -------
            bool
                True if the indexes are the same
                False otherwise            
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
        return self.__check_equals(query, 'Indexes diff:')
        
    def check_triggers(self):
        """Check if the triggers are equals.

            Returns
            -------
            bool
                True if the triggers are the same
                False otherwise            
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
            and  SUBSTR(p.relname, 1, 3) != 'vw_' -- We cannot check for vw_ views, because  they are created after that script
        ORDER BY p.relname, /*t.tgname, */pp.prosrc"""

        return self.__check_equals(query, 'Triggers diff:')
        
    def check_functions(self):
        """Check if the functions are equals.

            Returns
            -------
            bool
                True if the functions are the same
                False otherwise            
        """
        query = """
        SELECT routines.routine_name, parameters.data_type, routines.routine_definition
        FROM information_schema.routines
        JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
        WHERE routines.specific_schema NOT IN ('information_schema') AND routines.specific_schema NOT LIKE 'pg\_%' 
        ORDER BY routines.routine_name, parameters.data_type, routines.routine_definition, parameters.ordinal_position"""

        return self.__check_equals(query, 'Functions diff:')
        
    def check_rules(self):
        """Check if the rules are equals.

            Returns
            -------
            bool
                True if the rules are the same
                False otherwise            
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
        WHERE n.nspname NOT IN ('information_schema') AND n.nspname NOT LIKE 'pg\_%' 
        ORDER BY n.nspname, c.relname, rule_event"""

        return self.__check_equals(query, 'Rules diff:')
        
    def __check_equals(self, query, context=""):
        self.cur1.execute(query)
        records1 = self.cur1.fetchall()

        self.cur2.execute(query)
        records2 = self.cur2.fetchall()

        result = True

        if not self.silent:
            print(context)

        d = difflib.Differ()
        records1 = [str(x) for x in records1]
        records2 = [str(x) for x in records2]

        for line in d.compare(records1, records2):
            if line[0] in ('-', '+'):
                if not self.silent:
                    print(line)
                result = False

        if not self.silent:
            print('')

        return result

    def check_all(self, ignore=[]):
        """Run all the checks functions.

            Parameters
            ----------
            ignore: list of strings
                List of elements to be ignored in check (ex. tables, columns, views, ...)

            Returns
            -------
            bool
                True if all the checks are true
                False otherwise            
        """

        result = True

        if (not 'tables' in ignore) and (not self.check_tables()):
            result = False
        if not 'columns' in ignore:
            if 'views' in ignore:
                result = self.check_columns(False)
            else:
                result = self.check_columns(True)
        if (not 'constraints' in ignore) and (not self.check_constraints()):
            result = False
        if (not 'views' in ignore) and (not self.check_views()):
            result = False
        if (not 'sequences' in ignore) and (not self.check_sequences()):
            result = False
        if (not 'indexes' in ignore) and (not self.check_indexes()):
            result = False
        if (not 'triggers' in ignore) and (not self.check_triggers()):
            result = False
        if (not 'functions' in ignore) and (not self.check_functions()):
            result = False
        if (not 'rules' in ignore) and (not self.check_rules()):
            result = False

        return result

if __name__ == "__main__":
    """
    Main process
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-p1', '--pg_service1', help='Name of the first postgres service', required=True)
    parser.add_argument('-p2', '--pg_service2', help='Name of the second postgres service', required=True)
    parser.add_argument('-s', '--silent', help='Don\'t print lines with differences')
    parser.add_argument('-i', '--ignore', help='Elements to be ignored', nargs='+', choices=['tables',
                                                                                       'columns',
                                                                                       'constraints',
                                                                                       'views',
                                                                                       'sequences',
                                                                                       'indexes',
                                                                                       'triggers',
                                                                                       'functions',
                                                                                       'rules'])
    args = parser.parse_args()

    db_checker = Checker(args.pg_service1, args.pg_service2, args.silent)
    print('Running checker')
    if db_checker.check_all(args.ignore):
        print('The checked elements are equals')
    else:
        print('The checked elements are not equals')