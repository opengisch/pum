from unittest import TestCase

import psycopg2
import psycopg2.extras
from pum.core.checker import Checker


class TestChecker(TestCase):
    """Test the class Checker.
    
    2 pg_services related to 2 empty db, needed for test:
        qwat_test_1 
        qwat_test_2
    """

    def setUp(self):
        pg_service1 = 'qwat_test_1'
        pg_service2 = 'qwat_test_2'

        self.conn1 = psycopg2.connect("service={0}".format(pg_service1))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute('DROP SCHEMA IF EXISTS schema_foo CASCADE;'
                          'CREATE SCHEMA schema_foo;')
        self.conn1.commit()

        self.conn2 = psycopg2.connect("service={0}".format(pg_service2))
        self.cur2 = self.conn2.cursor()

        self.checker = Checker(pg_service1, pg_service2)

        self.cur2.execute('DROP SCHEMA IF EXISTS schema_foo CASCADE;'
                          'CREATE SCHEMA schema_foo;')
        self.conn2.commit()

    def test_check_tables(self):
        self.cur1.execute('CREATE TABLE schema_foo.bar (id integer);')
        self.conn1.commit()

        result, differences = self.checker.check_tables()
        self.assertFalse(result)

        self.cur2.execute('CREATE TABLE schema_foo.bar (id integer);')
        self.conn2.commit()

        result, differences = self.checker.check_tables()
        self.assertTrue(result)

    def test_check_columns(self):
        self.cur1.execute(
            'CREATE TABLE schema_foo.bar '
            '(id smallint, value integer, name varchar(100));')
        self.conn1.commit()

        result, differences = self.checker.check_columns()
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE TABLE schema_foo.bar '
            '(id integer, value integer, name varchar(100));')
        self.conn2.commit()

        result, differences = self.checker.check_columns()
        self.assertFalse(result)

        self.cur2.execute(
            'ALTER TABLE schema_foo.bar '
            'ALTER COLUMN id SET DATA TYPE smallint;')
        self.conn2.commit()

        result, differences = self.checker.check_columns()
        self.assertTrue(result)

    def test_check_constraints(self):

        self.cur1.execute(
            'CREATE TABLE schema_foo.bar '
            '(id smallint, value integer, name varchar(100), PRIMARY KEY(id));')
        self.conn1.commit()

        result, differences = self.checker.check_constraints()
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE TABLE schema_foo.bar '
            '(id smallint, value integer, name varchar(100));')
        self.conn2.commit()

        result, differences = self.checker.check_constraints()
        self.assertFalse(result)

        self.cur2.execute(
            'ALTER TABLE schema_foo.bar DROP COLUMN id;'
            'ALTER TABLE schema_foo.bar ADD COLUMN id smallint PRIMARY KEY')
        self.conn2.commit()

        result, differences = self.checker.check_constraints()
        self.assertTrue(result)

    def test_check_views(self):

        self.cur1.execute("CREATE VIEW schema_foo.bar AS SELECT 'foobar';")
        self.conn1.commit()

        result, differences = self.checker.check_views()
        self.assertFalse(result)

        self.cur2.execute("CREATE VIEW schema_foo.bar AS SELECT 'foobar';")
        self.conn2.commit()

        result, differences = self.checker.check_views()
        self.assertTrue(result)

    def test_check_sequences(self):

        self.cur1.execute('DROP SEQUENCE IF EXISTS serial CASCADE;')
        self.conn1.commit()
        self.cur2.execute('DROP SEQUENCE IF EXISTS serial CASCADE;')
        self.conn2.commit()

        result, differences = self.checker.check_sequences()
        self.assertTrue(result)

        self.cur1.execute('CREATE SEQUENCE serial START 101;')
        self.conn1.commit()

        result, differences = self.checker.check_sequences()
        self.assertFalse(result)

        self.cur2.execute('CREATE SEQUENCE serial START 101;')
        self.conn2.commit()

        result, differences = self.checker.check_sequences()
        self.assertTrue(result)

    def test_check_indexes(self):

        self.cur1.execute(
            'CREATE TABLE schema_foo.bar '
            '(id smallint, value integer, name varchar(100));'
            'CREATE UNIQUE INDEX name_idx ON schema_foo.bar (name);')
        self.conn1.commit()

        result, differences = self.checker.check_indexes()
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE TABLE schema_foo.bar '
            '(id smallint, value integer, name varchar(100));')
        self.conn2.commit()

        result, differences = self.checker.check_indexes()
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE UNIQUE INDEX name_idx ON schema_foo.bar (name);')
        self.conn2.commit()

        result, differences = self.checker.check_indexes()
        self.assertTrue(result)

    def test_check_triggers(self):
        self.cur1.execute('DROP FUNCTION IF EXISTS trigger_function();')
        self.conn1.commit()
        self.cur2.execute('DROP FUNCTION IF EXISTS trigger_function();')
        self.conn2.commit()

        result, differences = self.checker.check_triggers()
        self.assertTrue(result)

        self.cur1.execute(
            """CREATE FUNCTION trigger_function() RETURNS trigger AS 
            $$
            BEGIN
            select ("a"); 
            END; 
            $$
            LANGUAGE 'plpgsql';
            CREATE TABLE schema_foo.bar (id smallint, 
            value integer, name varchar(100));
            CREATE TRIGGER trig       
            BEFORE UPDATE ON schema_foo.bar
            FOR EACH ROW
            EXECUTE PROCEDURE trigger_function();""")
        self.conn1.commit()

        result, differences = self.checker.check_triggers()
        self.assertFalse(result)

        self.cur2.execute(
            """CREATE FUNCTION trigger_function() RETURNS trigger AS 
            $$
            BEGIN
            select ("a"); 
            END; 
            $$
            LANGUAGE 'plpgsql';
            CREATE TABLE schema_foo.bar 
            (id smallint, value integer, name varchar(100));
            CREATE TRIGGER trig       
            BEFORE UPDATE ON schema_foo.bar
            FOR EACH ROW
            EXECUTE PROCEDURE trigger_function();""")
        self.conn2.commit()

        result, differences = self.checker.check_triggers()
        self.assertTrue(result)

    def test_check_functions(self):

        self.cur1.execute('DROP FUNCTION IF EXISTS add(integer, integer);')
        self.conn1.commit()
        self.cur2.execute('DROP FUNCTION IF EXISTS add(integer, integer);')
        self.conn2.commit()

        result, differences = self.checker.check_functions()
        self.assertTrue(result)

        self.cur1.execute(
            """CREATE FUNCTION add(integer, integer) RETURNS integer
            AS 'select $1 + $2;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT;""")

        self.conn1.commit()

        result, differences = self.checker.check_functions()
        self.assertFalse(result)

        self.cur2.execute(
            """CREATE FUNCTION add(integer, integer) RETURNS integer
            AS 'select $1 + $2;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT;""")
        self.conn2.commit()

        result, differences = self.checker.check_functions()
        self.assertTrue(result)

    def test_check_rules(self):
        self.cur1.execute('DROP RULE IF EXISTS foorule ON schema_foo.bar;')
        self.conn1.commit()
        self.cur2.execute('DROP RULE IF EXISTS foorule ON schema_foo.bar;')
        self.conn2.commit()

        result, differences = self.checker.check_rules()
        self.assertTrue(result)

        self.cur1.execute(
            """CREATE TABLE schema_foo.bar 
            (id smallint, value integer, name varchar(100));
            CREATE RULE foorule AS ON UPDATE TO 
            schema_foo.bar DO ALSO NOTIFY bar;""")
        self.conn1.commit()

        result, differences = self.checker.check_rules()
        self.assertFalse(result)

        self.cur2.execute(
            """CREATE TABLE schema_foo.bar 
            (id smallint, value integer, name varchar(100));
            CREATE RULE foorule AS ON UPDATE TO 
            schema_foo.bar DO ALSO NOTIFY bar;""")
        self.conn2.commit()

        result, differences = self.checker.check_rules()
        self.assertTrue(result)
