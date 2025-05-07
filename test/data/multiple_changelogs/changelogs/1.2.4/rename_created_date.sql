
ALTER TABLE pum_test_data.some_table RENAME COLUMN created_date TO created;
COMMENT ON COLUMN pum_test_data.some_table.created IS 'A comment with quotes '' and a backslash \\';
