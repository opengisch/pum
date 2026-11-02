CREATE SCHEMA pum_test_app_schema;

CREATE VIEW pum_test_app_schema.some_view AS
SELECT id, name FROM pum_test_data_schema_1.some_table_1;
