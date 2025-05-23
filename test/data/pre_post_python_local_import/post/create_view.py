from pirogue.utils import select_columns
from psycopg import Connection

from pum.sql_content import SqlContent
from folder.my_module import produce_sql_code


def run_hook(connection: Connection) -> None:
    """Run the migration hook to create a view."""
    columns = select_columns(
        pg_cur=connection.cursor(),
        table_schema="pum_test_data",
        table_name="some_table",
    )
    sql_code = produce_sql_code(columns)
    SqlContent(sql_code).execute(connection=connection, commit=False)
