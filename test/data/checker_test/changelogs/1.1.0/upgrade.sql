-- Drop the inventory table (will only remain in DB2)
DROP TABLE IF EXISTS pum_test_checker.inventory;

-- Add a new table (only in DB1)
CREATE TABLE pum_test_checker.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES pum_test_checker.users(id),
    total NUMERIC(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add columns to products
ALTER TABLE pum_test_checker.products ADD COLUMN description TEXT;
ALTER TABLE pum_test_checker.products ADD COLUMN weight NUMERIC(10,3);

-- Modify users table - remove email, add new columns
ALTER TABLE pum_test_checker.users DROP COLUMN email;
ALTER TABLE pum_test_checker.users ADD COLUMN phone VARCHAR(20);
ALTER TABLE pum_test_checker.users ADD COLUMN status VARCHAR(20) DEFAULT 'active';

-- Add indexes
CREATE INDEX idx_orders_user_id ON pum_test_checker.orders(user_id);
CREATE INDEX idx_products_name_desc ON pum_test_checker.products(name, description);

-- Modify constraints
-- Drop the old products constraint (exists in DB2 only after this)
ALTER TABLE pum_test_checker.products DROP CONSTRAINT IF EXISTS products_in_stock_check;
-- Add new constraints on common tables (will only exist in DB1)
ALTER TABLE pum_test_checker.products ADD CONSTRAINT products_weight_positive CHECK (weight IS NULL OR weight > 0);
ALTER TABLE pum_test_checker.users ADD CONSTRAINT users_status_check CHECK (status IN ('active', 'inactive', 'pending'));
-- Add constraint on orders table
ALTER TABLE pum_test_checker.orders ADD CONSTRAINT orders_total_positive CHECK (total > 0);

-- Add sequence
CREATE SEQUENCE pum_test_checker.invoice_sequence START 5000;

-- Add function
CREATE OR REPLACE FUNCTION pum_test_checker.get_order_count(p_user_id INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM pum_test_checker.orders WHERE user_id = p_user_id);
END;
$$ LANGUAGE plpgsql;

-- Add view
CREATE VIEW pum_test_checker.user_orders AS
SELECT u.id, u.username, COUNT(o.id) as order_count
FROM pum_test_checker.users u
LEFT JOIN pum_test_checker.orders o ON u.id = o.user_id
GROUP BY u.id, u.username;

-- Add trigger
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
