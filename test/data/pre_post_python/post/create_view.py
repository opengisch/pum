import psycopg

from pum import HookBase


class Hook(HookBase):
    def run_hook(self, connection: psycopg.Connection) -> None:
        """Run the migration hook to create a view."""
        sql_code = """
            CREATE OR REPLACE VIEW pum_test_app.some_view AS
            SELECT id, name, created
            FROM pum_test_data.some_table
            WHERE is_active = TRUE;
            """
        self.execute(sql=sql_code)
