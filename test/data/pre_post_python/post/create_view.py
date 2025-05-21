from pirogue.utils import select_columns
from psycopg import Connection
from pum.sql_content import SqlContent


def run_hook(connection: Connection):
    columns = select_columns(
        pg_cur=connection.cursor(), table_schema="pum_test_data", table_name="some_table"
    )
    sql_code = f"""
        CREATE OR REPLACE VIEW pum_test_app.some_view AS
        SELECT {columns}
        FROM pum_test_data.some_table
        WHERE is_active = TRUE;
        """
    SqlContent(sql_code).execute(connection=connection, commit=False)
