-- Upgrade that changes constraint definition
-- This simulates the qwat issue where the constraint definition was modified
-- from year > 1800 to year >= 1800

-- Drop the old constraint
ALTER TABLE pum_test_constraint_def.pipes
    DROP CONSTRAINT pipe_year_check;

-- Add the new constraint with >= operator (new definition)
-- This is the fix from the qwat PR - allowing year = 1800
ALTER TABLE pum_test_constraint_def.pipes
    ADD CONSTRAINT pipe_year_check
    CHECK (year IS NULL OR year >= 1800 AND year <= EXTRACT(YEAR FROM NOW()));

-- Also modify the other constraint to test multiple changes
ALTER TABLE pum_test_constraint_def.measurements
    DROP CONSTRAINT measurement_value_positive;

ALTER TABLE pum_test_constraint_def.measurements
    ADD CONSTRAINT measurement_value_positive
    CHECK (value >= 0);  -- Changed from > 0 to >= 0
