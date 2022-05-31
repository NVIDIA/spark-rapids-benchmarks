delete from inventory where inv_date_sk >= ( select min(d_date_sk) from date_dim  where  d_date between 'DATE1' and 'DATE2') and 
                inv_date_sk <= ( select max(d_date_sk) from date_dim  where  d_date between 'DATE1' and 'DATE2');
