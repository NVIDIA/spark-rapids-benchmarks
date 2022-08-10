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

DROP VIEW IF EXISTS crv;
CREATE TEMP VIEW crv as
SELECT d_date_sk cr_returned_date_sk
 ,t_time_sk cr_returned_time_sk
 ,i_item_sk cr_item_sk
 ,c1.c_customer_sk cr_refunded_customer_sk
 ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
 ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
 ,c1.c_current_addr_sk cr_refunded_addr_sk
 ,c2.c_customer_sk cr_returning_customer_sk
 ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
 ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
 ,c2.c_current_addr_sk cr_returing_addr_sk
 ,cc_call_center_sk cr_call_center_sk
 ,cp_catalog_page_sk CR_CATALOG_PAGE_SK
 ,sm_ship_mode_sk CR_SHIP_MODE_SK
 ,w_warehouse_sk CR_WAREHOUSE_SK
 ,r_reason_sk cr_reason_sk
 ,cret_order_id cr_order_number
 ,cret_return_qty cr_return_quantity
 ,cret_return_amt cr_return_amt
 ,cret_return_tax cr_return_tax
 ,cret_return_amt + cret_return_tax AS cr_return_amt_inc_tax
 ,cret_return_fee cr_fee
 ,cret_return_ship_cost cr_return_ship_cost
 ,cret_refunded_cash cr_refunded_cash
 ,cret_reversed_charge cr_reversed_charge
 ,cret_merchant_credit cr_merchant_credit
 ,cret_return_amt+cret_return_tax+cret_return_fee
 -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
FROM s_catalog_returns 
LEFT OUTER JOIN date_dim 
 ON (cast(cret_return_date as date) = d_date)
LEFT OUTER JOIN time_dim ON 
 ((CAST(substr(cret_return_time,1,2) AS integer)*3600
 +CAST(substr(cret_return_time,4,2) AS integer)*60
 +CAST(substr(cret_return_time,7,2) AS integer)) = t_time)
LEFT OUTER JOIN item ON (cret_item_id = i_item_id)
LEFT OUTER JOIN customer c1 ON (cret_return_customer_id = c1.c_customer_id)
LEFT OUTER JOIN customer c2 ON (cret_refund_customer_id = c2.c_customer_id)
LEFT OUTER JOIN reason ON (cret_reason_id = r_reason_id)
LEFT OUTER JOIN call_center ON (cret_call_center_id = cc_call_center_id)
LEFT OUTER JOIN catalog_page ON (cret_catalog_page_id = cp_catalog_page_id)
LEFT OUTER JOIN ship_mode ON (cret_shipmode_id = sm_ship_mode_id)
LEFT OUTER JOIN warehouse ON (cret_warehouse_id = w_warehouse_id)
WHERE i_rec_end_date IS NULL AND cc_rec_end_date IS NULL;
------------------------------------------------
insert into catalog_returns (select * from crv);