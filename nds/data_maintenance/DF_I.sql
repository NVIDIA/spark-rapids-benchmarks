select min(d_date_sk) from date_dim  where  d_date between 'DATE1' and 'DATE2';
select max(d_date_sk) from date_dim  where  d_date between 'DATE1' and 'DATE2';

delete from inventory where inv_date_sk >= (SQL1) and inv_date_sk <= (SQL2);
