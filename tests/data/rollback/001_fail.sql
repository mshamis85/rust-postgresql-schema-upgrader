--- 0: fail_step
CREATE TABLE side_effect_table (id INT);
-- This will fail execution
SELECT * FROM non_existent_table_intentional_fail;