delete from catalog_returns where cr_order_number in  (select cs_order_number from catalog_sales, date_dim  where cs_sold_date_sk=d_date_sk and d_date between 'DATE1' and 'DATE2');
delete from catalog_sales where cs_sold_date_sk >= (select min(d_date_sk) from date_dim  where d_date between 'DATE1' and 'DATE2') and 
                cs_sold_date_sk <= (select max(d_date_sk) from date_dim  where d_date between 'DATE1' and 'DATE2');
