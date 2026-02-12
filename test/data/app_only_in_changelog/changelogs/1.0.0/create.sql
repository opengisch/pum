CREATE TABLE test_table (
    id INT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT {schema_name},
    comment TEXT DEFAULT {view_comment}
);
