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

DROP VIEW IF EXISTS wsv;
CREATE VIEW wsv AS
SELECT d1.d_date_sk ws_sold_date_sk, 
 t_time_sk ws_sold_time_sk, 
 d2.d_date_sk ws_ship_date_sk,
 i_item_sk ws_item_sk, 
 c1.c_customer_sk ws_bill_customer_sk, 
 c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
 c1.c_current_hdemo_sk ws_bill_hdemo_sk,
 c1.c_current_addr_sk ws_bill_addr_sk,
 c2.c_customer_sk ws_ship_customer_sk,
 c2.c_current_cdemo_sk ws_ship_cdemo_sk,
 c2.c_current_hdemo_sk ws_ship_hdemo_sk,
 c2.c_current_addr_sk ws_ship_addr_sk,
 wp_web_page_sk ws_web_page_sk,
 web_site_sk ws_web_site_sk,
 sm_ship_mode_sk ws_ship_mode_sk,
 w_warehouse_sk ws_warehouse_sk,
 p_promo_sk ws_promo_sk,
 word_order_id ws_order_number, 
 wlin_quantity ws_quantity, 
 i_wholesale_cost ws_wholesale_cost, 
 i_current_price ws_list_price,
 wlin_sales_price ws_sales_price,
 (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
 wlin_sales_price * wlin_quantity ws_ext_sales_price,
 i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
 i_current_price * wlin_quantity ws_ext_list_price, 
 i_current_price * web_tax_percentage ws_ext_tax, 
 wlin_coupon_amt ws_coupon_amt,
 wlin_ship_cost * wlin_quantity WS_EXT_SHIP_COST,
 (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
 ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
 ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) 
WS_NET_PAID_INC_SHIP,
 (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
 + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
 ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) 
WS_NET_PROFIT
FROM s_web_order 
LEFT OUTER JOIN date_dim d1 ON (cast(word_order_date as date) = d1.d_date)
LEFT OUTER JOIN time_dim ON (word_order_time = t_time)
LEFT OUTER JOIN customer c1 ON (word_bill_customer_id = c1.c_customer_id)
LEFT OUTER JOIN customer c2 ON (word_ship_customer_id = c2.c_customer_id)
LEFT OUTER JOIN web_site ON (word_web_site_id = web_site_id AND web_rec_end_date IS NULL)
LEFT OUTER JOIN ship_mode ON (word_ship_mode_id = sm_ship_mode_id)
JOIN s_web_order_lineitem ON (word_order_id = wlin_order_id)
LEFT OUTER JOIN date_dim d2 ON (cast(wlin_ship_date as date) = d2.d_date)
LEFT OUTER JOIN item ON (wlin_item_id = i_item_id AND i_rec_end_date IS NULL)
LEFT OUTER JOIN web_page ON (wlin_web_page_id = wp_web_page_id AND wp_rec_end_date IS NULL)
LEFT OUTER JOIN warehouse ON (wlin_warehouse_id = w_warehouse_id)
LEFT OUTER JOIN promotion ON (wlin_promotion_id = p_promo_id);
------------------------------------------------
insert into web_sales (select * from wsv);