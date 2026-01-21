-- Schema for testing constraint definition changes
-- This mimics the qwat issue where a CHECK constraint definition was changed

CREATE SCHEMA IF NOT EXISTS pum_test_constraint_def;

-- Table with a pipe/year scenario like the qwat issue
CREATE TABLE pum_test_constraint_def.pipes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    year INTEGER,
    diameter NUMERIC(10,2)
);

-- Add CHECK constraint with > operator (old definition)
-- This constraint will be modified in the upgrade
ALTER TABLE pum_test_constraint_def.pipes
    ADD CONSTRAINT pipe_year_check
    CHECK (year IS NULL OR year > 1800 AND year <= EXTRACT(YEAR FROM NOW()));

-- Another table to test multiple constraint types
CREATE TABLE pum_test_constraint_def.measurements (
    id SERIAL PRIMARY KEY,
    pipe_id INTEGER REFERENCES pum_test_constraint_def.pipes(id),
    value NUMERIC(10,3),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add another CHECK constraint
ALTER TABLE pum_test_constraint_def.measurements
    ADD CONSTRAINT measurement_value_positive
    CHECK (value > 0);
