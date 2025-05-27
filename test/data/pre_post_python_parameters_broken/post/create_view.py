from pirogue.utils import select_columns
import psycopg

from pum import HookBase


class Hook(HookBase):
    def run_hook(self, connection: psycopg.Connection, my_comment: str) -> None:
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
        sql_code = psycopg.sql.SQL(f"""
        CREATE OR REPLACE VIEW pum_test_app.some_view AS
        SELECT {columns}
        FROM pum_test_data.some_table
        WHERE is_active = TRUE;

        COMMENT ON VIEW pum_test_app.some_view IS {{my_comment}};
        """)
        self.execute(sql_code=sql_code)
