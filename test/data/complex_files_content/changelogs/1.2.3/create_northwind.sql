CREATE SCHEMA IF NOT EXISTS pum_test_data;

CREATE TABLE pum_test_data.some_table (
    id INT PRIMARY KEY, -- comment in code
    name VARCHAR(100) NOT NULL,
    created_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT TRUE,
    amount NUMERIC(10,2)
);

COMMENT ON COLUMN pum_test_data.some_table.created_date IS 'A comment with semi column; , quotes '' and a backslash \\ and -- dashes';

-- commented code that has ; in it

/* ALTER TABLE mytable ADD COLUMN newcolvarchar (16);
ALTER TABLE mytable ADD COLUMN newcol2 varchar (16); */

/* -- ; ' */

-- This is a comment with a semicolon; and a quote ' and a backslash \

COMMENT ON TABLE pum_test_data.some_table IS 'Une table écrite en français avec un commentaire contenant un point-virgule ; et une apostrophe '' et un antislash \\';


CREATE OR REPLACE FUNCTION pum_test_data.random_between_body(min_val INT, max_val INT)
RETURNS INT AS $BODY$
BEGIN
  RETURN floor(random() * (max_val - min_val + 1) + min_val)::INT;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pum_test_data.random_between_do(min_val INT, max_val INT)
RETURNS INT AS $DO$
BEGIN
  RETURN floor(random() * (max_val - min_val + 1) + min_val)::INT;
END;
$DO$ LANGUAGE plpgsql;
