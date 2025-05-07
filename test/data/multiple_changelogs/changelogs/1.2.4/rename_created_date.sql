
ALTER TABLE pum_test_data.some_table RENAME COLUMN created_date TO created;
COMMENT ON COLUMN pum_test_data.some_table.created IS 'A comment with semi column; , quotes '' and a backslash \\';

-- commented code that has ; in it
/* ALTER TABLE mytable ADD COLUMN newcol varchar (16);
ALTER TABLE mytable ADD COLUMN newcol2 varchar (16); */
