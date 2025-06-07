import psycopg

from folder.my_module import produce_sql_code

from pum import HookBase


class Hook(HookBase):
    def run_hook(self, connection: psycopg.Connection) -> None:
        """Run the migration hook to create a view."""
        sql_code = produce_sql_code(["id", "name", "created_date"])
        self.execute(sql=sql_code)
