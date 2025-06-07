from pirogue.utils import select_columns
import psycopg

from pum import HookBase


class Hook(HookBase):
    def run_hook(self, connection: psycopg.Connection) -> None:
        """Run the migration hook to create a view."""
        columns = select_columns(
            connection=connection,
            table_schema="pum_test_data",
            table_name="some_table",
        )
        sql_code = f"""
        CREATE OR REPLACE VIEW pum_test_app.some_view AS
        SELECT {columns}
        FROM pum_test_data.some_table
        WHERE is_active = TRUE;
        """

        self.execute(sql=sql_code)
