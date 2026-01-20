CREATE SCHEMA IF NOT EXISTS pum_test_data_schema_1;

CREATE TABLE pum_test_data_schema_1.some_table_1 (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT TRUE,
    amount NUMERIC(10,2)
);

CREATE SEQUENCE pum_test_data_schema_1.some_sequence_1 START 1;

CREATE OR REPLACE FUNCTION pum_test_data_schema_1.some_function_1()
RETURNS INT AS $$
BEGIN
    RETURN 42;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW pum_test_data_schema_1.some_view_1 AS
SELECT id, name FROM pum_test_data_schema_1.some_table_1;

CREATE SCHEMA IF NOT EXISTS pum_test_data_schema_2;

CREATE TABLE pum_test_data_schema_2.some_table_2 (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT TRUE,
    amount NUMERIC(10,2)
);

CREATE SEQUENCE pum_test_data_schema_2.some_sequence_2 START 1;

CREATE OR REPLACE FUNCTION pum_test_data_schema_2.some_function_2()
RETURNS INT AS $$
BEGIN
    RETURN 84;
END;
$$ LANGUAGE plpgsql;
