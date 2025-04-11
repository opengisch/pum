-- Create a schema named "northwind"
CREATE SCHEMA IF NOT EXISTS test;

CREATE TABLE test.some_table (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT TRUE,
    amount NUMERIC(10,2)
);
