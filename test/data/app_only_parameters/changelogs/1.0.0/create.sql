CREATE SCHEMA IF NOT EXISTS pum_test_data;

CREATE TABLE pum_test_data.some_table (
    id INT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT {schema_name}
);
