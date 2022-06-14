delete from store_returns where sr_ticket_number in (select ss_ticket_number from store_sales, date_dim   where ss_sold_date_sk=d_date_sk and d_date between 'DATE1' and 'DATE2');
delete from store_sales where ss_sold_date_sk >= (select min(d_date_sk) from date_dim  where d_date between 'DATE1' and 'DATE2') and
                ss_sold_date_sk <= (select max(d_date_sk) from date_dim  where d_date between 'DATE1' and 'DATE2');
