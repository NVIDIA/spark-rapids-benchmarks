#
# SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----
#
# Certain portions of the contents of this file are derived from TPC-DS version 3.2.0
# (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
# Such portions are subject to copyrights held by Transaction Processing Performance Council (“TPC”)
# and licensed under the TPC EULA (a copy of which accompanies this file as “TPC EULA” and is also
# available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the “TPC EULA”).
#
# You may not use this file except in compliance with the TPC EULA.
# DISCLAIMER: Portions of this file is derived from the TPC-DS Benchmark and as such any results
# obtained using this file are not comparable to published TPC-DS Benchmark results, as the results
# obtained from using this file do not comply with the TPC-DS Benchmark.
#

import argparse
import timeit
import pyspark
import os

from pyspark.sql.types import *
from pyspark.sql.functions import col

if not hasattr(pyspark.sql.types, "VarcharType"):
    # this is a version of Spark that doesn't have fixed- and max-length string types
    setattr(pyspark.sql.types, "VarcharType", lambda x: StringType())
    setattr(pyspark.sql.types, "CharType", lambda x: StringType())

from pyspark.sql.types import VarcharType, CharType

SCHEMAS = {}

SCHEMAS["dbgen_version"] = StructType([
    StructField("dv_version", VarcharType(16)),
    StructField("dv_create_date", DateType()),
    StructField("dv_create_time", TimestampType()),
    StructField("dv_cmdline_args", VarcharType(200))
])

SCHEMAS["customer_address"] = StructType([
    StructField("ca_address_sk", IntegerType(), nullable=False),
    StructField("ca_address_id", CharType(16), nullable=False),
    StructField("ca_street_number", CharType(10)),
    StructField("ca_street_name", VarcharType(60)),
    StructField("ca_street_type", CharType(15)),
    StructField("ca_suite_number", CharType(10)),
    StructField("ca_city", VarcharType(60)),
    StructField("ca_county", VarcharType(30)),
    StructField("ca_state", CharType(2)),
    StructField("ca_zip", CharType(10)),
    StructField("ca_country", VarcharType(20)),
    StructField("ca_gmt_offset", DecimalType(5,2)),
    StructField("ca_location_type", CharType(20))
])

SCHEMAS["customer_demographics"] = StructType([
    StructField("cd_demo_sk", IntegerType(), nullable=False),
    StructField("cd_gender", CharType(1)),
    StructField("cd_marital_status", CharType(1)),
    StructField("cd_education_status", CharType(20)),
    StructField("cd_purchase_estimate", IntegerType()),
    StructField("cd_credit_rating", CharType(10)),
    StructField("cd_dep_count", IntegerType()),
    StructField("cd_dep_employed_count", IntegerType()),
    StructField("cd_dep_college_count", IntegerType())
])

SCHEMAS["date_dim"] = StructType([
    StructField("d_date_sk", IntegerType(), nullable=False),
    StructField("d_date_id", CharType(16), nullable=False),
    StructField("d_date", DateType()),
    StructField("d_month_seq", IntegerType()),
    StructField("d_week_seq", IntegerType()),
    StructField("d_quarter_seq", IntegerType()),
    StructField("d_year", IntegerType()),
    StructField("d_dow", IntegerType()),
    StructField("d_moy", IntegerType()),
    StructField("d_dom", IntegerType()),
    StructField("d_qoy", IntegerType()),
    StructField("d_fy_year", IntegerType()),
    StructField("d_fy_quarter_seq", IntegerType()),
    StructField("d_fy_week_seq", IntegerType()),
    StructField("d_day_name", CharType(9)),
    StructField("d_quarter_name", CharType(6)),
    StructField("d_holiday", CharType(1)),
    StructField("d_weekend", CharType(1)),
    StructField("d_following_holiday", CharType(1)),
    StructField("d_first_dom", IntegerType()),
    StructField("d_last_dom", IntegerType()),
    StructField("d_same_day_ly", IntegerType()),
    StructField("d_same_day_lq", IntegerType()),
    StructField("d_current_day", CharType(1)),
    StructField("d_current_week", CharType(1)),
    StructField("d_current_month", CharType(1)),
    StructField("d_current_quarter", CharType(1)),
    StructField("d_current_year", CharType(1))
])

SCHEMAS["warehouse"] = StructType([
    StructField("w_warehouse_sk", IntegerType(), nullable=False),
    StructField("w_warehouse_id", CharType(16), nullable=False),
    StructField("w_warehouse_name", VarcharType(20)),
    StructField("w_warehouse_sq_ft", IntegerType()),
    StructField("w_street_number", CharType(10)),
    StructField("w_street_name", VarcharType(60)),
    StructField("w_street_type", CharType(15)),
    StructField("w_suite_number", CharType(10)),
    StructField("w_city", VarcharType(60)),
    StructField("w_county", VarcharType(30)),
    StructField("w_state", CharType(2)),
    StructField("w_zip", CharType(10)),
    StructField("w_country", VarcharType(20)),
    StructField("w_gmt_offset", DecimalType(5,2))
])

SCHEMAS["ship_mode"] = StructType([
    StructField("sm_ship_mode_sk", IntegerType(), nullable=False),
    StructField("sm_ship_mode_id", CharType(16), nullable=False),
    StructField("sm_type", CharType(30)),
    StructField("sm_code", CharType(10)),
    StructField("sm_carrier", CharType(20)),
    StructField("sm_contract", CharType(20))
])

SCHEMAS["time_dim"] = StructType([
    StructField("t_time_sk", IntegerType(), nullable=False),
    StructField("t_time_id", CharType(16), nullable=False),
    StructField("t_time", IntegerType()),
    StructField("t_hour", IntegerType()),
    StructField("t_minute", IntegerType()),
    StructField("t_second", IntegerType()),
    StructField("t_am_pm", CharType(2)),
    StructField("t_shift", CharType(20)),
    StructField("t_sub_shift", CharType(20)),
    StructField("t_meal_time", CharType(20))
])

SCHEMAS["reason"] = StructType([
    StructField("r_reason_sk", IntegerType(), nullable=False),
    StructField("r_reason_id", CharType(16), nullable=False),
    StructField("r_reason_desc", CharType(100))
])

SCHEMAS["income_band"] = StructType([
    StructField("ib_income_band_sk", IntegerType(), nullable=False),
    StructField("ib_lower_bound", IntegerType()),
    StructField("ib_upper_bound", IntegerType())
])

SCHEMAS["item"] = StructType([
    StructField("i_item_sk", IntegerType(), nullable=False),
    StructField("i_item_id", CharType(16), nullable=False),
    StructField("i_rec_start_date", DateType()),
    StructField("i_rec_end_date", DateType()),
    StructField("i_item_desc", VarcharType(200)),
    StructField("i_current_price", DecimalType(7,2)),
    StructField("i_wholesale_cost", DecimalType(7,2)),
    StructField("i_brand_id", IntegerType()),
    StructField("i_brand", CharType(50)),
    StructField("i_class_id", IntegerType()),
    StructField("i_class", CharType(50)),
    StructField("i_category_id", IntegerType()),
    StructField("i_category", CharType(50)),
    StructField("i_manufact_id", IntegerType()),
    StructField("i_manufact", CharType(50)),
    StructField("i_size", CharType(20)),
    StructField("i_formulation", CharType(20)),
    StructField("i_color", CharType(20)),
    StructField("i_units", CharType(10)),
    StructField("i_container", CharType(10)),
    StructField("i_manager_id", IntegerType()),
    StructField("i_product_name", CharType(50))
])

SCHEMAS["store"] = StructType([
    StructField("s_store_sk", IntegerType(), nullable=False),
    StructField("s_store_id", CharType(16), nullable=False),
    StructField("s_rec_start_date", DateType()),
    StructField("s_rec_end_date", DateType()),
    StructField("s_closed_date_sk", IntegerType()),
    StructField("s_store_name", VarcharType(50)),
    StructField("s_number_employees", IntegerType()),
    StructField("s_floor_space", IntegerType()),
    StructField("s_hours", CharType(20)),
    StructField("s_manager", VarcharType(40)),
    StructField("s_market_id", IntegerType()),
    StructField("s_geography_class", VarcharType(100)),
    StructField("s_market_desc", VarcharType(100)),
    StructField("s_market_manager", VarcharType(40)),
    StructField("s_division_id", IntegerType()),
    StructField("s_division_name", VarcharType(50)),
    StructField("s_company_id", IntegerType()),
    StructField("s_company_name", VarcharType(50)),
    StructField("s_street_number", VarcharType(10)),
    StructField("s_street_name", VarcharType(60)),
    StructField("s_street_type", CharType(15)),
    StructField("s_suite_number", CharType(10)),
    StructField("s_city", VarcharType(60)),
    StructField("s_county", VarcharType(30)),
    StructField("s_state", CharType(2)),
    StructField("s_zip", CharType(10)),
    StructField("s_country", VarcharType(20)),
    StructField("s_gmt_offset", DecimalType(5,2)),
    StructField("s_tax_precentage", DecimalType(5,2))
])

SCHEMAS["call_center"] = StructType([
    StructField("cc_call_center_sk", IntegerType(), nullable=False),
    StructField("cc_call_center_id", CharType(16), nullable=False),
    StructField("cc_rec_start_date", DateType()),
    StructField("cc_rec_end_date", DateType()),
    StructField("cc_closed_date_sk", IntegerType()),
    StructField("cc_open_date_sk", IntegerType()),
    StructField("cc_name", VarcharType(50)),
    StructField("cc_class", VarcharType(50)),
    StructField("cc_employees", IntegerType()),
    StructField("cc_sq_ft", IntegerType()),
    StructField("cc_hours", CharType(20)),
    StructField("cc_manager", VarcharType(40)),
    StructField("cc_mkt_id", IntegerType()),
    StructField("cc_mkt_class", CharType(50)),
    StructField("cc_mkt_desc", VarcharType(100)),
    StructField("cc_market_manager", VarcharType(40)),
    StructField("cc_division", IntegerType()),
    StructField("cc_division_name", VarcharType(50)),
    StructField("cc_company", IntegerType()),
    StructField("cc_company_name", CharType(50)),
    StructField("cc_street_number", CharType(10)),
    StructField("cc_street_name", VarcharType(60)),
    StructField("cc_street_type", CharType(15)),
    StructField("cc_suite_number", CharType(10)),
    StructField("cc_city", VarcharType(60)),
    StructField("cc_county", VarcharType(30)),
    StructField("cc_state", CharType(2)),
    StructField("cc_zip", CharType(10)),
    StructField("cc_country", VarcharType(20)),
    StructField("cc_gmt_offset", DecimalType(5,2)),
    StructField("cc_tax_percentage", DecimalType(5,2))
])

SCHEMAS["customer"] = StructType([
    StructField("c_customer_sk", IntegerType(), nullable=False),
    StructField("c_customer_id", CharType(16), nullable=False),
    StructField("c_current_cdemo_sk", IntegerType()),
    StructField("c_current_hdemo_sk", IntegerType()),
    StructField("c_current_addr_sk", IntegerType()),
    StructField("c_first_shipto_date_sk", IntegerType()),
    StructField("c_first_sales_date_sk", IntegerType()),
    StructField("c_salutation", CharType(10)),
    StructField("c_first_name", CharType(20)),
    StructField("c_last_name", CharType(30)),
    StructField("c_preferred_cust_flag", CharType(1)),
    StructField("c_birth_day", IntegerType()),
    StructField("c_birth_month", IntegerType()),
    StructField("c_birth_year", IntegerType()),
    StructField("c_birth_country", VarcharType(20)),
    StructField("c_login", CharType(13)),
    StructField("c_email_address", CharType(50)),
    StructField("c_last_review_date_sk", CharType(10))
])

SCHEMAS["web_site"] = StructType([
    StructField("web_site_sk", IntegerType(), nullable=False),
    StructField("web_site_id", CharType(16), nullable=False),
    StructField("web_rec_start_date", DateType()),
    StructField("web_rec_end_date", DateType()),
    StructField("web_name", VarcharType(50)),
    StructField("web_open_date_sk", IntegerType()),
    StructField("web_close_date_sk", IntegerType()),
    StructField("web_class", VarcharType(50)),
    StructField("web_manager", VarcharType(40)),
    StructField("web_mkt_id", IntegerType()),
    StructField("web_mkt_class", VarcharType(50)),
    StructField("web_mkt_desc", VarcharType(100)),
    StructField("web_market_manager", VarcharType(40)),
    StructField("web_company_id", IntegerType()),
    StructField("web_company_name", CharType(50)),
    StructField("web_street_number", CharType(10)),
    StructField("web_street_name", VarcharType(60)),
    StructField("web_street_type", CharType(15)),
    StructField("web_suite_number", CharType(10)),
    StructField("web_city", VarcharType(60)),
    StructField("web_county", VarcharType(30)),
    StructField("web_state", CharType(2)),
    StructField("web_zip", CharType(10)),
    StructField("web_country", VarcharType(20)),
    StructField("web_gmt_offset", DecimalType(5,2)),
    StructField("web_tax_percentage", DecimalType(5,2))
])

SCHEMAS["store_returns"] = StructType([
    StructField("sr_returned_date_sk", IntegerType()),
    StructField("sr_return_time_sk", IntegerType()),
    StructField("sr_item_sk", IntegerType(), nullable=False),
    StructField("sr_customer_sk", IntegerType()),
    StructField("sr_cdemo_sk", IntegerType()),
    StructField("sr_hdemo_sk", IntegerType()),
    StructField("sr_addr_sk", IntegerType()),
    StructField("sr_store_sk", IntegerType()),
    StructField("sr_reason_sk", IntegerType()),
    StructField("sr_ticket_number", IntegerType(), nullable=False),
    StructField("sr_return_quantity", IntegerType()),
    StructField("sr_return_amt", DecimalType(7,2)),
    StructField("sr_return_tax", DecimalType(7,2)),
    StructField("sr_return_amt_inc_tax", DecimalType(7,2)),
    StructField("sr_fee", DecimalType(7,2)),
    StructField("sr_return_ship_cost", DecimalType(7,2)),
    StructField("sr_refunded_cash", DecimalType(7,2)),
    StructField("sr_reversed_charge", DecimalType(7,2)),
    StructField("sr_store_credit", DecimalType(7,2)),
    StructField("sr_net_loss", DecimalType(7,2))
])

SCHEMAS["household_demographics"] = StructType([
    StructField("hd_demo_sk", IntegerType(), nullable=False),
    StructField("hd_income_band_sk", IntegerType()),
    StructField("hd_buy_potential", CharType(15)),
    StructField("hd_dep_count", IntegerType()),
    StructField("hd_vehicle_count", IntegerType())
])

SCHEMAS["web_page"] = StructType([
    StructField("wp_web_page_sk", IntegerType(), nullable=False),
    StructField("wp_web_page_id", CharType(16), nullable=False),
    StructField("wp_rec_start_date", DateType()),
    StructField("wp_rec_end_date", DateType()),
    StructField("wp_creation_date_sk", IntegerType()),
    StructField("wp_access_date_sk", IntegerType()),
    StructField("wp_autogen_flag", CharType(1)),
    StructField("wp_customer_sk", IntegerType()),
    StructField("wp_url", VarcharType(100)),
    StructField("wp_type", CharType(50)),
    StructField("wp_char_count", IntegerType()),
    StructField("wp_link_count", IntegerType()),
    StructField("wp_image_count", IntegerType()),
    StructField("wp_max_ad_count", IntegerType())
])

SCHEMAS["promotion"] = StructType([
    StructField("p_promo_sk", IntegerType(), nullable=False),
    StructField("p_promo_id", CharType(16), nullable=False),
    StructField("p_start_date_sk", IntegerType()),
    StructField("p_end_date_sk", IntegerType()),
    StructField("p_item_sk", IntegerType()),
    StructField("p_cost", DecimalType(15,2)),
    StructField("p_response_target", IntegerType()),
    StructField("p_promo_name", CharType(50)),
    StructField("p_channel_dmail", CharType(1)),
    StructField("p_channel_email", CharType(1)),
    StructField("p_channel_catalog", CharType(1)),
    StructField("p_channel_tv", CharType(1)),
    StructField("p_channel_radio", CharType(1)),
    StructField("p_channel_press", CharType(1)),
    StructField("p_channel_event", CharType(1)),
    StructField("p_channel_demo", CharType(1)),
    StructField("p_channel_details", VarcharType(100)),
    StructField("p_purpose", CharType(15)),
    StructField("p_discount_active", CharType(1))
])

SCHEMAS["catalog_page"] = StructType([
    StructField("cp_catalog_page_sk", IntegerType(), nullable=False),
    StructField("cp_catalog_page_id", CharType(16), nullable=False),
    StructField("cp_start_date_sk", IntegerType()),
    StructField("cp_end_date_sk", IntegerType()),
    StructField("cp_department", VarcharType(50)),
    StructField("cp_catalog_number", IntegerType()),
    StructField("cp_catalog_page_number", IntegerType()),
    StructField("cp_description", VarcharType(100)),
    StructField("cp_type", VarcharType(100))
])

SCHEMAS["inventory"] = StructType([
    StructField("inv_date_sk", IntegerType(), nullable=False),
    StructField("inv_item_sk", IntegerType(), nullable=False),
    StructField("inv_warehouse_sk", IntegerType(), nullable=False),
    StructField("inv_quantity_on_hand", IntegerType())
])

SCHEMAS["catalog_returns"] = StructType([
    StructField("cr_returned_date_sk", IntegerType()),
    StructField("cr_returned_time_sk", IntegerType()),
    StructField("cr_item_sk", IntegerType(), nullable=False),
    StructField("cr_refunded_customer_sk", IntegerType()),
    StructField("cr_refunded_cdemo_sk", IntegerType()),
    StructField("cr_refunded_hdemo_sk", IntegerType()),
    StructField("cr_refunded_addr_sk", IntegerType()),
    StructField("cr_returning_customer_sk", IntegerType()),
    StructField("cr_returning_cdemo_sk", IntegerType()),
    StructField("cr_returning_hdemo_sk", IntegerType()),
    StructField("cr_returning_addr_sk", IntegerType()),
    StructField("cr_call_center_sk", IntegerType()),
    StructField("cr_catalog_page_sk", IntegerType()),
    StructField("cr_ship_mode_sk", IntegerType()),
    StructField("cr_warehouse_sk", IntegerType()),
    StructField("cr_reason_sk", IntegerType()),
    StructField("cr_order_number", IntegerType(), nullable=False),
    StructField("cr_return_quantity", IntegerType()),
    StructField("cr_return_amount", DecimalType(7,2)),
    StructField("cr_return_tax", DecimalType(7,2)),
    StructField("cr_return_amt_inc_tax", DecimalType(7,2)),
    StructField("cr_fee", DecimalType(7,2)),
    StructField("cr_return_ship_cost", DecimalType(7,2)),
    StructField("cr_refunded_cash", DecimalType(7,2)),
    StructField("cr_reversed_charge", DecimalType(7,2)),
    StructField("cr_store_credit", DecimalType(7,2)),
    StructField("cr_net_loss", DecimalType(7,2))
])

SCHEMAS["web_returns"] = StructType([
    StructField("wr_returned_date_sk", IntegerType()),
    StructField("wr_returned_time_sk", IntegerType()),
    StructField("wr_item_sk", IntegerType(), nullable=False),
    StructField("wr_refunded_customer_sk", IntegerType()),
    StructField("wr_refunded_cdemo_sk", IntegerType()),
    StructField("wr_refunded_hdemo_sk", IntegerType()),
    StructField("wr_refunded_addr_sk", IntegerType()),
    StructField("wr_returning_customer_sk", IntegerType()),
    StructField("wr_returning_cdemo_sk", IntegerType()),
    StructField("wr_returning_hdemo_sk", IntegerType()),
    StructField("wr_returning_addr_sk", IntegerType()),
    StructField("wr_web_page_sk", IntegerType()),
    StructField("wr_reason_sk", IntegerType()),
    StructField("wr_order_number", IntegerType(), nullable=False),
    StructField("wr_return_quantity", IntegerType()),
    StructField("wr_return_amt", DecimalType(7,2)),
    StructField("wr_return_tax", DecimalType(7,2)),
    StructField("wr_return_amt_inc_tax", DecimalType(7,2)),
    StructField("wr_fee", DecimalType(7,2)),
    StructField("wr_return_ship_cost", DecimalType(7,2)),
    StructField("wr_refunded_cash", DecimalType(7,2)),
    StructField("wr_reversed_charge", DecimalType(7,2)),
    StructField("wr_account_credit", DecimalType(7,2)),
    StructField("wr_net_loss", DecimalType(7,2))
])

SCHEMAS["web_sales"] = StructType([
    StructField("ws_sold_date_sk", IntegerType()),
    StructField("ws_sold_time_sk", IntegerType()),
    StructField("ws_ship_date_sk", IntegerType()),
    StructField("ws_item_sk", IntegerType(), nullable=False),
    StructField("ws_bill_customer_sk", IntegerType()),
    StructField("ws_bill_cdemo_sk", IntegerType()),
    StructField("ws_bill_hdemo_sk", IntegerType()),
    StructField("ws_bill_addr_sk", IntegerType()),
    StructField("ws_ship_customer_sk", IntegerType()),
    StructField("ws_ship_cdemo_sk", IntegerType()),
    StructField("ws_ship_hdemo_sk", IntegerType()),
    StructField("ws_ship_addr_sk", IntegerType()),
    StructField("ws_web_page_sk", IntegerType()),
    StructField("ws_web_site_sk", IntegerType()),
    StructField("ws_ship_mode_sk", IntegerType()),
    StructField("ws_warehouse_sk", IntegerType()),
    StructField("ws_promo_sk", IntegerType()),
    StructField("ws_order_number", IntegerType(), nullable=False),
    StructField("ws_quantity", IntegerType()),
    StructField("ws_wholesale_cost", DecimalType(7,2)),
    StructField("ws_list_price", DecimalType(7,2)),
    StructField("ws_sales_price", DecimalType(7,2)),
    StructField("ws_ext_discount_amt", DecimalType(7,2)),
    StructField("ws_ext_sales_price", DecimalType(7,2)),
    StructField("ws_ext_wholesale_cost", DecimalType(7,2)),
    StructField("ws_ext_list_price", DecimalType(7,2)),
    StructField("ws_ext_tax", DecimalType(7,2)),
    StructField("ws_coupon_amt", DecimalType(7,2)),
    StructField("ws_ext_ship_cost", DecimalType(7,2)),
    StructField("ws_net_paid", DecimalType(7,2)),
    StructField("ws_net_paid_inc_tax", DecimalType(7,2)),
    StructField("ws_net_paid_inc_ship", DecimalType(7,2)),
    StructField("ws_net_paid_inc_ship_tax", DecimalType(7,2)),
    StructField("ws_net_profit", DecimalType(7,2))
])

SCHEMAS["catalog_sales"] = StructType([
    StructField("cs_sold_date_sk", IntegerType()),
    StructField("cs_sold_time_sk", IntegerType()),
    StructField("cs_ship_date_sk", IntegerType()),
    StructField("cs_bill_customer_sk", IntegerType()),
    StructField("cs_bill_cdemo_sk", IntegerType()),
    StructField("cs_bill_hdemo_sk", IntegerType()),
    StructField("cs_bill_addr_sk", IntegerType()),
    StructField("cs_ship_customer_sk", IntegerType()),
    StructField("cs_ship_cdemo_sk", IntegerType()),
    StructField("cs_ship_hdemo_sk", IntegerType()),
    StructField("cs_ship_addr_sk", IntegerType()),
    StructField("cs_call_center_sk", IntegerType()),
    StructField("cs_catalog_page_sk", IntegerType()),
    StructField("cs_ship_mode_sk", IntegerType()),
    StructField("cs_warehouse_sk", IntegerType()),
    StructField("cs_item_sk", IntegerType(), nullable=False),
    StructField("cs_promo_sk", IntegerType()),
    StructField("cs_order_number", IntegerType(), nullable=False),
    StructField("cs_quantity", IntegerType()),
    StructField("cs_wholesale_cost", DecimalType(7,2)),
    StructField("cs_list_price", DecimalType(7,2)),
    StructField("cs_sales_price", DecimalType(7,2)),
    StructField("cs_ext_discount_amt", DecimalType(7,2)),
    StructField("cs_ext_sales_price", DecimalType(7,2)),
    StructField("cs_ext_wholesale_cost", DecimalType(7,2)),
    StructField("cs_ext_list_price", DecimalType(7,2)),
    StructField("cs_ext_tax", DecimalType(7,2)),
    StructField("cs_coupon_amt", DecimalType(7,2)),
    StructField("cs_ext_ship_cost", DecimalType(7,2)),
    StructField("cs_net_paid", DecimalType(7,2)),
    StructField("cs_net_paid_inc_tax", DecimalType(7,2)),
    StructField("cs_net_paid_inc_ship", DecimalType(7,2)),
    StructField("cs_net_paid_inc_ship_tax", DecimalType(7,2)),
    StructField("cs_net_profit", DecimalType(7,2))
])

SCHEMAS["store_sales"] = StructType([
    StructField("ss_sold_date_sk", IntegerType()),
    StructField("ss_sold_time_sk", IntegerType()),
    StructField("ss_item_sk", IntegerType(), nullable=False),
    StructField("ss_customer_sk", IntegerType()),
    StructField("ss_cdemo_sk", IntegerType()),
    StructField("ss_hdemo_sk", IntegerType()),
    StructField("ss_addr_sk", IntegerType()),
    StructField("ss_store_sk", IntegerType()),
    StructField("ss_promo_sk", IntegerType()),
    StructField("ss_ticket_number", IntegerType(), nullable=False),
    StructField("ss_quantity", IntegerType()),
    StructField("ss_wholesale_cost", DecimalType(7,2)),
    StructField("ss_list_price", DecimalType(7,2)),
    StructField("ss_sales_price", DecimalType(7,2)),
    StructField("ss_ext_discount_amt", DecimalType(7,2)),
    StructField("ss_ext_sales_price", DecimalType(7,2)),
    StructField("ss_ext_wholesale_cost", DecimalType(7,2)),
    StructField("ss_ext_list_price", DecimalType(7,2)),
    StructField("ss_ext_tax", DecimalType(7,2)),
    StructField("ss_coupon_amt", DecimalType(7,2)),
    StructField("ss_net_paid", DecimalType(7,2)),
    StructField("ss_net_paid_inc_tax", DecimalType(7,2)),
    StructField("ss_net_profit", DecimalType(7,2))
])

TABLE_PARTITIONING = {
    'catalog_sales': 'cs_sold_date_sk',
    'catalog_returns': 'cr_returned_date_sk',
    'inventory': 'inv_date_sk',
    'store_sales': 'ss_sold_date_sk',
    'store_returns':'sr_returned_date_sk',
    'web_sales': 'ws_sold_date_sk',
    'web_returns': 'wr_returned_date_sk'
}


def load(filename, schema, delimiter="|", header="false", prefix=""):
    data_path = os.path.join(prefix, filename)
    global session
    return session.read.option("delimiter", delimiter).option("header", header).csv(data_path, schema=schema)

def store(df, filename, prefix=""):
    data_path = os.path.join(prefix, filename)
    if filename in TABLE_PARTITIONING.keys():
        df.repartition(col(TABLE_PARTITIONING[filename])).write.mode("overwrite").partitionBy(TABLE_PARTITIONING[filename]).parquet(data_path)
    else:
        df.coalesce(1).write.mode("overwrite").parquet(data_path)

if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('--output-mode', help='Spark data source output mode for the result (default: overwrite)', default="overwrite")
    parser.add_argument('--input-prefix', help='text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"; the default is empty)', default="")
    parser.add_argument('--input-suffix', help='text to append to every input filename (e.g., ".dat"; the default is empty)', default="")
    parser.add_argument('--output-prefix', help='text to prepend to every output file (e.g., "hdfs:///ds-parquet"; the default is empty)', default="")
    parser.add_argument('--report-file', help='location in which to store a performance report', default='report.txt')
    parser.add_argument('--log-level', help='set log level (default: OFF)', default="OFF")
    #    parser.add_argument('--coalesce-output', help='coalesce output to NUM partitions', default=0, type=int)

    args = parser.parse_args()

    session = pyspark.sql.SparkSession.builder \
        .appName("ds-convert") \
        .getOrCreate()

    session.sparkContext.setLogLevel(args.log_level)

    results = {}

    for fn, schema in SCHEMAS.items():
        results[fn] = timeit.timeit(lambda: store(load(f"{fn}{args.input_suffix}", schema, prefix=args.input_prefix), f"{fn}", args.output_prefix), number=1)
    
    report_text = "Total conversion time for %d tables was %.02fs\n" % (len(results.values()), sum(results.values()))
    for table, duration in results.items():
        report_text += "Time to convert '%s' was %.04fs\n" % (table, duration)

    report_text += "\n\n\nSpark configuration follows:\n\n"

    with open(args.report_file, "w") as report:
        report.write(report_text)
        print(report_text)

        for conf in session.sparkContext.getConf().getAll():
            report.write(str(conf) + "\n")
            print(conf)
