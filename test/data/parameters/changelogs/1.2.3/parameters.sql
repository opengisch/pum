CREATE SCHEMA IF NOT EXISTS pum_test_data;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE pum_test_data.some_table (
    id INT PRIMARY KEY,
    geom geometry(LineString, {SRID})
);


CREATE TABLE pum_test_data.some_table2 (
    id INT PRIMARY KEY,
    some_text TEXT NOT NULL DEFAULT {default_text_value},
    some_number INT NOT NULL DEFAULT {default_integer_value}
);
