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

DROP VIEW IF EXISTS srv;
CREATE TEMP view srv as
SELECT d_date_sk sr_returned_date_sk
 ,t_time_sk sr_return_time_sk
 ,i_item_sk sr_item_sk
 ,c_customer_sk sr_customer_sk
 ,c_current_cdemo_sk sr_cdemo_sk
 ,c_current_hdemo_sk sr_hdemo_sk
 ,c_current_addr_sk sr_addr_sk
 ,s_store_sk sr_store_sk
 ,r_reason_sk sr_reason_sk
 ,sret_ticket_number sr_ticket_number
 ,sret_return_qty sr_return_quantity
 ,sret_return_amt sr_return_amt
 ,sret_return_tax sr_return_tax
 ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
 ,sret_return_fee sr_fee
 ,sret_return_ship_cost sr_return_ship_cost
 ,sret_refunded_cash sr_refunded_cash
 ,sret_reversed_charge sr_reversed_charge
 ,sret_store_credit sr_store_credit
 ,sret_return_amt+sret_return_tax+sret_return_fee
 -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
FROM s_store_returns 
LEFT OUTER JOIN date_dim 
 ON (cast(sret_return_date as date) = d_date)
LEFT OUTER JOIN time_dim 
 ON (( cast(substr(sret_return_time,1,2) AS integer)*3600
 +cast(substr(sret_return_time,4,2) AS integer)*60
 +cast(substr(sret_return_time,7,2) AS integer)) = t_time)
LEFT OUTER JOIN item ON (sret_item_id = i_item_id)
LEFT OUTER JOIN customer ON (sret_customer_id = c_customer_id)
LEFT OUTER JOIN store ON (sret_store_id = s_store_id)
LEFT OUTER JOIN reason ON (sret_reason_id = r_reason_id)
WHERE i_rec_end_date IS NULL
 AND s_rec_end_date IS NULL;
------------------------------------------------
insert into store_returns (select * from srv);