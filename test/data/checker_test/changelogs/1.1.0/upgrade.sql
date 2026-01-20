-- Add a new table
CREATE TABLE pum_test_checker.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES pum_test_checker.users(id),
    total NUMERIC(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Modify existing table - add column
ALTER TABLE pum_test_checker.products ADD COLUMN description TEXT;

-- Add a new index
CREATE INDEX idx_orders_user_id ON pum_test_checker.orders(user_id);

-- Add a new sequence
CREATE SEQUENCE pum_test_checker.invoice_sequence START 5000;

-- Add a new function
CREATE OR REPLACE FUNCTION pum_test_checker.get_order_count(p_user_id INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM pum_test_checker.orders WHERE user_id = p_user_id);
END;
$$ LANGUAGE plpgsql;

-- Add a new view
CREATE VIEW pum_test_checker.user_orders AS
SELECT u.id, u.username, COUNT(o.id) as order_count
FROM pum_test_checker.users u
LEFT JOIN pum_test_checker.orders o ON u.id = o.user_id
GROUP BY u.id, u.username;

-- Add a new trigger
CREATE OR REPLACE FUNCTION pum_test_checker.update_order_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.order_date = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_update_trigger
BEFORE UPDATE ON pum_test_checker.orders
FOR EACH ROW
EXECUTE FUNCTION pum_test_checker.update_order_timestamp();
