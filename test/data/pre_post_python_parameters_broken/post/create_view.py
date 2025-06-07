import psycopg

from pum import HookBase


class Hook(HookBase):
    def run_hook(self, connection: psycopg.Connection, my_comment: str) -> None:
        """Run the migration hook to create a view.

        Args:
            connection (Connection): The database connection.
            my_comment (str): The comment to be added to the view.

        """

        sql_code = """
        CREATE OR REPLACE VIEW pum_test_app.some_view AS
        SELECT id, name, created
        FROM pum_test_data.some_table
        WHERE is_active = TRUE;

        COMMENT ON VIEW pum_test_app.some_view IS {my_comment};
        """
        self.execute(sql=sql_code)
