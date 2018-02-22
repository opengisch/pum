import psycopg2
from unittest import TestCase

from pum.commands.check import Check


class TestCheck(TestCase):

    PG_SERVICE_1 = "pum_test_1"
    PG_SERVICE_2 = "pum_test_2"

    def setUp(self):
        self.conn1 = psycopg2.connect("service={0}".format(
            TestCheck.PG_SERVICE_1))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""CREATE SCHEMA test_check;""")
        self.conn1.commit()

        self.conn2 = psycopg2.connect("service={0}".format(
            TestCheck.PG_SERVICE_2))
        self.cur2 = self.conn2.cursor()

        self.cur2.execute("""CREATE SCHEMA test_check;""")
        self.conn2.commit()

    def tearDown(self):
        self.conn1 = psycopg2.connect("service={0}".format(
            TestCheck.PG_SERVICE_1))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""DROP SCHEMA test_check CASCADE;""")
        self.conn1.commit()

        self.conn2 = psycopg2.connect("service={0}".format(
            TestCheck.PG_SERVICE_2))
        self.cur2 = self.conn2.cursor()

        self.cur2.execute("""DROP SCHEMA test_check CASCADE;""")
        self.conn2.commit()

    def test_check_tables(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['columns', 'constraints', 'views', 'sequences',
                       'indexes', 'triggers', 'functions', 'rules']
        args.verbose_level = 0

        self.cur1.execute('CREATE TABLE test_check.bar (id integer);')
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute('CREATE TABLE test_check.bar (id integer);')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_columns(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'constraints', 'views', 'sequences',
                       'indexes', 'triggers', 'functions', 'rules']
        args.verbose_level = 0

        self.cur1.execute(
            'CREATE TABLE test_check.bar '
            '(id smallint, value integer, name varchar(100));')
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE TABLE test_check.bar '
            '(id integer, value integer, name varchar(100));')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            'ALTER TABLE test_check.bar '
            'ALTER COLUMN id SET DATA TYPE smallint;')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_constraints(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'columns', 'views', 'sequences',
                       'indexes', 'triggers', 'functions', 'rules']
        args.verbose_level = 0

        self.cur1.execute(
            'CREATE TABLE test_check.bar '
            '(id smallint, value integer, name varchar(100),'
            'PRIMARY KEY(id));')
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE TABLE test_check.bar '
            '(id smallint, value integer, name varchar(100));')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            'ALTER TABLE test_check.bar DROP COLUMN id;'
            'ALTER TABLE test_check.bar ADD COLUMN id smallint PRIMARY KEY')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_views(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'columns', 'constraints', 'sequences',
                       'indexes', 'triggers', 'functions', 'rules']
        args.verbose_level = 0

        self.cur1.execute("CREATE VIEW test_check.bar AS SELECT 'foobar';")
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute("CREATE VIEW test_check.bar AS SELECT 'foobar';")
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_sequences(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'columns', 'constraints', 'views',
                       'indexes', 'triggers', 'functions', 'rules']
        args.verbose_level = 0

        self.cur1.execute('DROP SEQUENCE IF EXISTS serial CASCADE;')
        self.conn1.commit()
        self.cur2.execute('DROP SEQUENCE IF EXISTS serial CASCADE;')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

        self.cur1.execute('CREATE SEQUENCE serial START 101;')
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute('CREATE SEQUENCE serial START 101;')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_indexes(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'columns', 'constraints', 'views',
                       'sequences', 'triggers', 'functions', 'rules']
        args.verbose_level = 0

        self.cur1.execute(
            'CREATE TABLE test_check.bar '
            '(id smallint, value integer, name varchar(100));'
            'CREATE UNIQUE INDEX name_idx ON test_check.bar (name);')
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE TABLE test_check.bar '
            '(id smallint, value integer, name varchar(100));')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            'CREATE UNIQUE INDEX name_idx ON test_check.bar (name);')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_triggers(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'columns', 'constraints', 'views',
                       'sequences', 'indexes', 'functions', 'rules']
        args.verbose_level = 0

        self.cur1.execute('DROP FUNCTION IF EXISTS trigger_function();')
        self.conn1.commit()
        self.cur2.execute('DROP FUNCTION IF EXISTS trigger_function();')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

        self.cur1.execute(
            """CREATE FUNCTION trigger_function() RETURNS trigger AS
            $$
            BEGIN
            select ("a");
            END;
            $$
            LANGUAGE 'plpgsql';
            CREATE TABLE test_check.bar (id smallint,
            value integer, name varchar(100));
            CREATE TRIGGER trig
            BEFORE UPDATE ON test_check.bar
            FOR EACH ROW
            EXECUTE PROCEDURE trigger_function();""")
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            """CREATE FUNCTION trigger_function() RETURNS trigger AS
            $$
            BEGIN
            select ("a");
            END;
            $$
            LANGUAGE 'plpgsql';
            CREATE TABLE test_check.bar
            (id smallint, value integer, name varchar(100));
            CREATE TRIGGER trig
            BEFORE UPDATE ON test_check.bar
            FOR EACH ROW
            EXECUTE PROCEDURE trigger_function();""")
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_functions(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'columns', 'constraints', 'views',
                       'sequences', 'indexes', 'triggers', 'rules']
        args.verbose_level = 0

        self.cur1.execute('DROP FUNCTION IF EXISTS add(integer, integer);')
        self.conn1.commit()
        self.cur2.execute('DROP FUNCTION IF EXISTS add(integer, integer);')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

        self.cur1.execute(
            """CREATE FUNCTION add(integer, integer) RETURNS integer
            AS 'select $1 + $2;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT;""")

        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            """CREATE FUNCTION add(integer, integer) RETURNS integer
            AS 'select $1 + $2;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT;""")
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

    def test_check_rules(self):

        args = Args()
        args.pg_service1 = TestCheck.PG_SERVICE_1
        args.pg_service2 = TestCheck.PG_SERVICE_2
        args.ignore = ['tables', 'columns', 'constraints', 'views',
                       'sequences', 'indexes', 'triggers', 'functions']
        args.verbose_level = 0

        self.cur1.execute('DROP RULE IF EXISTS foorule ON test_check.bar;')
        self.conn1.commit()
        self.cur2.execute('DROP RULE IF EXISTS foorule ON test_check.bar;')
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)

        self.cur1.execute(
            """CREATE TABLE test_check.bar
            (id smallint, value integer, name varchar(100));
            CREATE RULE foorule AS ON UPDATE TO
            test_check.bar DO ALSO NOTIFY bar;""")
        self.conn1.commit()

        result, differences = Check.run(args)
        self.assertFalse(result)

        self.cur2.execute(
            """CREATE TABLE test_check.bar
            (id smallint, value integer, name varchar(100));
            CREATE RULE foorule AS ON UPDATE TO
            test_check.bar DO ALSO NOTIFY bar;""")
        self.conn2.commit()

        result, differences = Check.run(args)
        self.assertTrue(result)


class Args(object):
    pass
