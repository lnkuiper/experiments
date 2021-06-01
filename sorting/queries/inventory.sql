-- 1 payload column
SELECT inv_warehouse_sk FROM inventory ORDER BY inv_warehouse_sk, inv_item_sk;
-- 2 payload columns
SELECT inv_warehouse_sk, inv_item_sk FROM inventory ORDER BY inv_warehouse_sk, inv_item_sk;
-- 3 payload column
SELECT inv_warehouse_sk, inv_item_sk, inv_date_sk FROM inventory ORDER BY inv_warehouse_sk, inv_item_sk;
-- 4 payload columns
SELECT inv_warehouse_sk, inv_item_sk, inv_date_sk, inv_quantity_on_hand FROM inventory ORDER BY inv_warehouse_sk, inv_item_sk;
-- 1 sorting column
SELECT * FROM inventory ORDER BY inv_warehouse_sk;
-- 2 sorting columns
SELECT * FROM inventory ORDER BY inv_warehouse_sk, inv_item_sk;
-- 3 sorting columns
SELECT * FROM inventory ORDER BY inv_warehouse_sk, inv_item_sk, inv_date_sk;
-- 4 sorting columns
SELECT * FROM inventory ORDER BY inv_warehouse_sk, inv_item_sk, inv_date_sk, inv_quantity_on_hand;