CREATE SCHEMA IF NOT EXISTS pum_test_data;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE pum_test_data.some_table (
    id INT PRIMARY KEY,
    geom geometry(LineString, %(SRID)s)
);
