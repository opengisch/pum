from pirogue.utils import select_columns
from psycopg import Connection

from pum import SqlContent


def run_hook(connection: Connection, my_comment: str) -> None:
    """Run the migration hook to create a view.

    Args:
        connection (Connection): The database connection.
        my_comment (str): The comment to be added to the view.

    """
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
