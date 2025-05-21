import psycopg
from pirogue.utils import select_columns

from pum.sql_content import SqlContent


def run_hook(connection: psycopg.Connection, my_comment: str) -> None:
    """Run the migration hook to create a view."""
    columns = select_columns(
        pg_cur=connection.cursor(),
        table_schema="pum_test_data",
        table_name="some_table",
    )
    sql_code = f"""
    CREATE OR REPLACE VIEW pum_test_app.some_view AS
    SELECT {columns}
    FROM pum_test_data.some_table
    WHERE is_active = TRUE;

    COMMENT ON VIEW pum_test_app.some_view IS '{my_comment}';
    """  # noqa: S608
    SqlContent(sql_code).execute(connection=connection, commit=False)
