
CREATE SCHEMA IF NOT EXISTS pum_test_app;

CREATE VIEW pum_test_app.some_view AS
SELECT id, name, created_date, is_active, amount
FROM pum_test_data.some_table
WHERE is_active = TRUE;
