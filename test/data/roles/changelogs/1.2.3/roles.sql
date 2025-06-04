CREATE SCHEMA IF NOT EXISTS pum_test_data_schema_1;

CREATE TABLE pum_test_data_schema_1.some_table_1 (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT TRUE,
    amount NUMERIC(10,2)
);

CREATE SCHEMA IF NOT EXISTS pum_test_data_schema_2;

CREATE TABLE pum_test_data_schema_2.some_table_2 (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT TRUE,
    amount NUMERIC(10,2)
);
