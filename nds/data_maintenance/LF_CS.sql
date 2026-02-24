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

DROP VIEW IF EXISTS csv;
CREATE TEMP view csv as
SELECT d1.d_date_sk cs_sold_date_sk 
 ,t_time_sk cs_sold_time_sk 
 ,d2.d_date_sk cs_ship_date_sk
 ,c1.c_customer_sk cs_bill_customer_sk
 ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
 ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
 ,c1.c_current_addr_sk cs_bill_addr_sk
 ,c2.c_customer_sk cs_ship_customer_sk
 ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
 ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
 ,c2.c_current_addr_sk cs_ship_addr_sk
 ,cc_call_center_sk cs_call_center_sk
 ,cp_catalog_page_sk cs_catalog_page_sk
 ,sm_ship_mode_sk cs_ship_mode_sk
 ,w_warehouse_sk cs_warehouse_sk
 ,i_item_sk cs_item_sk
 ,p_promo_sk cs_promo_sk
 ,cord_order_id cs_order_number
 ,clin_quantity cs_quantity
 ,i_wholesale_cost cs_wholesale_cost
 ,i_current_price cs_list_price
 ,clin_sales_price cs_sales_price
 ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
 ,clin_sales_price * clin_quantity cs_ext_sales_price
 ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
 ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
 ,i_current_price * cc_tax_percentage CS_EXT_TAX
 ,clin_coupon_amt cs_coupon_amt
 ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
 ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
 ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
 ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
 ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
 + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
 ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
FROM s_catalog_order 
LEFT OUTER JOIN date_dim d1 ON
 (cast(cord_order_date as date) = d1.d_date)
LEFT OUTER JOIN time_dim ON (cord_order_time = t_time)
LEFT OUTER JOIN customer c1 ON (cord_bill_customer_id = c1.c_customer_id)
LEFT OUTER JOIN customer c2 ON (cord_ship_customer_id = c2.c_customer_id)
LEFT OUTER JOIN call_center ON (cord_call_center_id = cc_call_center_id AND cc_rec_end_date IS NULL)
LEFT OUTER JOIN ship_mode ON (cord_ship_mode_id = sm_ship_mode_id)
JOIN s_catalog_order_lineitem ON (cord_order_id = clin_order_id)
LEFT OUTER JOIN date_dim d2 ON
 (cast(clin_ship_date as date) = d2.d_date)
LEFT OUTER JOIN catalog_page ON
 (clin_catalog_page_number = cp_catalog_page_number and clin_catalog_number = cp_catalog_number)
LEFT OUTER JOIN warehouse ON (clin_warehouse_id = w_warehouse_id)
LEFT OUTER JOIN item ON (clin_item_id = i_item_id AND i_rec_end_date IS NULL)
LEFT OUTER JOIN promotion ON (clin_promotion_id = p_promo_id);
------------------------------------------------
insert into catalog_sales (select * from csv order by cs_sold_date_sk);