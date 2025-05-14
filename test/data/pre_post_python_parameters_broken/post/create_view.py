from pirogue.utils import select_columns
from psycopg import Connection
from pum.utils.execute_sql import execute_sql


def run_hook(connection: Connection, my_comment: str = None):
    columns = select_columns(
        pg_cur=connection.cursor(), table_schema="pum_test_data", table_name="some_table"
    )
    sql_code = f"""
    CREATE OR REPLACE VIEW pum_test_app.some_view AS
    SELECT {columns}
    FROM pum_test_data.some_table
    WHERE is_active = TRUE;

    COMMENT ON VIEW pum_test_app.some_view IS '{my_comment}';
    """
    execute_sql(connection=connection, sql=sql_code, commit=False)
