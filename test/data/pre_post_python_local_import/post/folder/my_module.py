def produce_sql_code(columns: str) -> str:
    """Produce SQL code."""
    return f"""
        CREATE OR REPLACE VIEW pum_test_app.some_view AS
        SELECT {columns}
        FROM pum_test_data.some_table
        WHERE is_active = TRUE;
        """  # noqa: S608
