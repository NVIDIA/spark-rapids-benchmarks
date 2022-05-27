CREATE view iv AS
SELECT d_date_sk inv_date_sk,
 i_item_sk inv_item_sk,
 w_warehouse_sk inv_warehouse_sk,
 invn_qty_on_hand inv_quantity_on_hand
FROM s_inventory
LEFT OUTER JOIN warehouse ON (invn_warehouse_id=w_warehouse_id)
LEFT OUTER JOIN item ON (invn_item_id=i_item_id AND i_rec_end_date IS NULL)
LEFT OUTER JOIN date_dim ON (d_date=invn_date);
------------------------------------------------
insert into inventory (select * from iv);