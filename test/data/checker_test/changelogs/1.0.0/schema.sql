CREATE SCHEMA IF NOT EXISTS pum_test_checker;

-- Tables
CREATE TABLE pum_test_checker.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pum_test_checker.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price NUMERIC(10,2),
    in_stock BOOLEAN DEFAULT TRUE
);

-- Sequences
CREATE SEQUENCE pum_test_checker.order_sequence START 1000;

-- Functions
CREATE OR REPLACE FUNCTION pum_test_checker.get_user_count()
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM pum_test_checker.users);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pum_test_checker.calculate_total(p_price NUMERIC, p_quantity INTEGER)
RETURNS NUMERIC AS $$
BEGIN
    RETURN p_price * p_quantity;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Views
CREATE VIEW pum_test_checker.active_products AS
SELECT id, name, price
FROM pum_test_checker.products
WHERE in_stock = TRUE;

-- Indexes
CREATE INDEX idx_users_username ON pum_test_checker.users(username);
CREATE INDEX idx_products_name ON pum_test_checker.products(name);

-- Constraints
ALTER TABLE pum_test_checker.users ADD CONSTRAINT check_email CHECK (email LIKE '%@%');

-- Triggers
CREATE OR REPLACE FUNCTION pum_test_checker.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_update_trigger
BEFORE UPDATE ON pum_test_checker.users
FOR EACH ROW
EXECUTE FUNCTION pum_test_checker.update_timestamp();
