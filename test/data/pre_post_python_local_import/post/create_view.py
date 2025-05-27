from pirogue.utils import select_columns
import psycopg

from folder.my_module import produce_sql_code

from pum import HookBase


class Hook(HookBase):
    def run_hook(self, connection: psycopg.Connection) -> None:
        """Run the migration hook to create a view."""
        columns = select_columns(
            pg_cur=connection.cursor(),
            table_schema="pum_test_data",
            table_name="some_table",
        )
        sql_code = produce_sql_code(columns)
        self.execute(sql=sql_code)
