--
-- SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- -----
--
-- Certain portions of the contents of this file are derived from TPC-DS version 3.2.0
-- (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
-- Such portions are subject to copyrights held by Transaction Processing Performance Council (“TPC”)
-- and licensed under the TPC EULA (a copy of which accompanies this file as “TPC EULA” and is also
-- available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the “TPC EULA”).
--
-- You may not use this file except in compliance with the TPC EULA.
-- DISCLAIMER: Portions of this file is derived from the TPC-DS Benchmark and as such any results
-- obtained using this file are not comparable to published TPC-DS Benchmark results, as the results
-- obtained from using this file do not comply with the TPC-DS Benchmark.
--

DROP VIEW IF EXISTS ssv;
CREATE TEMP view ssv as
SELECT d_date_sk ss_sold_date_sk, 
 t_time_sk ss_sold_time_sk, 
 i_item_sk ss_item_sk, 
 c_customer_sk ss_customer_sk, 
 c_current_cdemo_sk ss_cdemo_sk, 
 c_current_hdemo_sk ss_hdemo_sk,
 c_current_addr_sk ss_addr_sk,
 s_store_sk ss_store_sk,
 p_promo_sk ss_promo_sk,
 purc_purchase_id ss_ticket_number, 
 plin_quantity ss_quantity, 
 i_wholesale_cost ss_wholesale_cost, 
 i_current_price ss_list_price,
 plin_sale_price ss_sales_price,
 (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
 plin_sale_price * plin_quantity ss_ext_sales_price,
 i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
 i_current_price * plin_quantity ss_ext_list_price, 
 i_current_price * s_tax_precentage ss_ext_tax, 
 plin_coupon_amt ss_coupon_amt,
 (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
 ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
 ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) 
ss_net_profit
FROM s_purchase 
LEFT OUTER JOIN customer ON (purc_customer_id = c_customer_id) 
LEFT OUTER JOIN store ON (purc_store_id = s_store_id)
LEFT OUTER JOIN date_dim ON (cast(purc_purchase_date as date) = d_date)
LEFT OUTER JOIN time_dim ON (PURC_PURCHASE_TIME = t_time)
JOIN s_purchase_lineitem ON (purc_purchase_id = plin_purchase_id)
LEFT OUTER JOIN promotion ON plin_promotion_id = p_promo_id
LEFT OUTER JOIN item ON plin_item_id = i_item_id
WHERE purc_purchase_id = plin_purchase_id
 AND i_rec_end_date is NULL
 AND s_rec_end_date is NULL;
------------------------------------------------
insert into store_sales (select * from ssv order by ss_sold_date_sk);