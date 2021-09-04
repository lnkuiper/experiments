PRAGMA memory_limit='10GB';
PRAGMA threads=4;
CREATE TEMPORARY TABLE output AS SELECT * FROM customer ORDER BY c_first_name, c_last_name;

