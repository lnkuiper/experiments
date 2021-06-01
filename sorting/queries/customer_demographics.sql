-- all constant size columns
SELECT cd_demo_sk, cd_purchase_estimate, cd_dep_count, cd_dep_employed_count, cd_dep_college_count FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- all variable size columns
SELECT cd_gender, cd_marital_status, cd_education_status, cd_credit_rating FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- all columns
SELECT cd_demo_sk, cd_purchase_estimate, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cd_gender, cd_marital_status, cd_education_status, cd_credit_rating FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 1 sorting column
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 2 sorting columns
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 3 sorting columns
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 4 sorting columns
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 5 sorting columns
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 6 sorting columns
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 7 sorting columns
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
-- 8 sorting columns
SELECT * FROM customer_demographics ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;