/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

trait TPCDSSchema {

  private val tableColumns = Map(
    "store_sales" ->
      """
        |`ss_sold_date_sk` INT, `ss_sold_time_sk` INT, `ss_item_sk` INT, `ss_customer_sk` INT,
        |`ss_cdemo_sk` INT, `ss_hdemo_sk` INT, `ss_addr_sk` INT, `ss_store_sk` INT,
        |`ss_promo_sk` INT, `ss_ticket_number` INT, `ss_quantity` INT,
        |`ss_wholesale_cost` DECIMAL(7,2), `ss_list_price` DECIMAL(7,2),
        |`ss_sales_price` DECIMAL(7,2), `ss_ext_discount_amt` DECIMAL(7,2),
        |`ss_ext_sales_price` DECIMAL(7,2), `ss_ext_wholesale_cost` DECIMAL(7,2),
        |`ss_ext_list_price` DECIMAL(7,2), `ss_ext_tax` DECIMAL(7,2),
        |`ss_coupon_amt` DECIMAL(7,2), `ss_net_paid` DECIMAL(7,2),
        |`ss_net_paid_inc_tax` DECIMAL(7,2), `ss_net_profit` DECIMAL(7,2)
      """.stripMargin,
    "store_returns" ->
      """
        |`sr_returned_date_sk` BIGINT, `sr_return_time_sk` BIGINT, `sr_item_sk` BIGINT,
        |`sr_customer_sk` BIGINT, `sr_cdemo_sk` BIGINT, `sr_hdemo_sk` BIGINT, `sr_addr_sk` BIGINT,
        |`sr_store_sk` BIGINT, `sr_reason_sk` BIGINT, `sr_ticket_number` BIGINT,
        |`sr_return_quantity` INT, `sr_return_amt` DECIMAL(7,2), `sr_return_tax` DECIMAL(7,2),
        |`sr_return_amt_inc_tax` DECIMAL(7,2), `sr_fee` DECIMAL(7,2),
        |`sr_return_ship_cost` DECIMAL(7,2), `sr_refunded_cash` DECIMAL(7,2),
        |`sr_reversed_charge` DECIMAL(7,2), `sr_store_credit` DECIMAL(7,2),
        |`sr_net_loss` DECIMAL(7,2)
      """.stripMargin,
    "catalog_sales" ->
      """
        |`cs_sold_date_sk` INT, `cs_sold_time_sk` INT, `cs_ship_date_sk` INT,
        |`cs_bill_customer_sk` INT, `cs_bill_cdemo_sk` INT, `cs_bill_hdemo_sk` INT,
        |`cs_bill_addr_sk` INT, `cs_ship_customer_sk` INT, `cs_ship_cdemo_sk` INT,
        |`cs_ship_hdemo_sk` INT, `cs_ship_addr_sk` INT, `cs_call_center_sk` INT,
        |`cs_catalog_page_sk` INT, `cs_ship_mode_sk` INT, `cs_warehouse_sk` INT,
        |`cs_item_sk` INT, `cs_promo_sk` INT, `cs_order_number` INT, `cs_quantity` INT,
        |`cs_wholesale_cost` DECIMAL(7,2), `cs_list_price` DECIMAL(7,2),
        |`cs_sales_price` DECIMAL(7,2), `cs_ext_discount_amt` DECIMAL(7,2),
        |`cs_ext_sales_price` DECIMAL(7,2), `cs_ext_wholesale_cost` DECIMAL(7,2),
        |`cs_ext_list_price` DECIMAL(7,2), `cs_ext_tax` DECIMAL(7,2),
        |`cs_coupon_amt` DECIMAL(7,2), `cs_ext_ship_cost` DECIMAL(7,2),
        |`cs_net_paid` DECIMAL(7,2), `cs_net_paid_inc_tax` DECIMAL(7,2),
        |`cs_net_paid_inc_ship` DECIMAL(7,2), `cs_net_paid_inc_ship_tax` DECIMAL(7,2),
        |`cs_net_profit` DECIMAL(7,2)
      """.stripMargin,
    "catalog_returns" ->
      """
        |`cr_returned_date_sk` INT, `cr_returned_time_sk` INT, `cr_item_sk` INT,
        |`cr_refunded_customer_sk` INT, `cr_refunded_cdemo_sk` INT, `cr_refunded_hdemo_sk` INT,
        |`cr_refunded_addr_sk` INT, `cr_returning_customer_sk` INT, `cr_returning_cdemo_sk` INT,
        |`cr_returning_hdemo_sk` INT, `cr_returning_addr_sk` INT, `cr_call_center_sk` INT,
        |`cr_catalog_page_sk` INT, `cr_ship_mode_sk` INT, `cr_warehouse_sk` INT,
        |`cr_reason_sk` INT,`cr_order_number` INT, `cr_return_quantity` INT,
        |`cr_return_amount` DECIMAL(7,2), `cr_return_tax` DECIMAL(7,2),
        |`cr_return_amt_inc_tax` DECIMAL(7,2), `cr_fee` DECIMAL(7,2),
        |`cr_return_ship_cost` DECIMAL(7,2), `cr_refunded_cash` DECIMAL(7,2),
        |`cr_reversed_charge` DECIMAL(7,2), `cr_store_credit` DECIMAL(7,2),
        |`cr_net_loss` DECIMAL(7,2)
      """.stripMargin,
    "web_sales" ->
      """
        |`ws_sold_date_sk` INT, `ws_sold_time_sk` INT, `ws_ship_date_sk` INT, `ws_item_sk` INT,
        |`ws_bill_customer_sk` INT, `ws_bill_cdemo_sk` INT, `ws_bill_hdemo_sk` INT,
        |`ws_bill_addr_sk` INT, `ws_ship_customer_sk` INT, `ws_ship_cdemo_sk` INT,
        |`ws_ship_hdemo_sk` INT, `ws_ship_addr_sk` INT, `ws_web_page_sk` INT,
        |`ws_web_site_sk` INT, `ws_ship_mode_sk` INT, `ws_warehouse_sk` INT, `ws_promo_sk` INT,
        |`ws_order_number` INT, `ws_quantity` INT, `ws_wholesale_cost` DECIMAL(7,2),
        |`ws_list_price` DECIMAL(7,2), `ws_sales_price` DECIMAL(7,2),
        |`ws_ext_discount_amt` DECIMAL(7,2), `ws_ext_sales_price` DECIMAL(7,2),
        |`ws_ext_wholesale_cost` DECIMAL(7,2), `ws_ext_list_price` DECIMAL(7,2),
        |`ws_ext_tax` DECIMAL(7,2), `ws_coupon_amt` DECIMAL(7,2), `ws_ext_ship_cost` DECIMAL(7,2),
        |`ws_net_paid` DECIMAL(7,2), `ws_net_paid_inc_tax` DECIMAL(7,2),
        |`ws_net_paid_inc_ship` DECIMAL(7,2), `ws_net_paid_inc_ship_tax` DECIMAL(7,2),
        |`ws_net_profit` DECIMAL(7,2)
      """.stripMargin,
    "web_returns" ->
      """
        |`wr_returned_date_sk` BIGINT, `wr_returned_time_sk` BIGINT, `wr_item_sk` BIGINT,
        |`wr_refunded_customer_sk` BIGINT, `wr_refunded_cdemo_sk` BIGINT,
        |`wr_refunded_hdemo_sk` BIGINT, `wr_refunded_addr_sk` BIGINT,
        |`wr_returning_customer_sk` BIGINT, `wr_returning_cdemo_sk` BIGINT,
        |`wr_returning_hdemo_sk` BIGINT, `wr_returning_addr_sk` BIGINT, `wr_web_page_sk` BIGINT,
        |`wr_reason_sk` BIGINT, `wr_order_number` BIGINT, `wr_return_quantity` INT,
        |`wr_return_amt` DECIMAL(7,2), `wr_return_tax` DECIMAL(7,2),
        |`wr_return_amt_inc_tax` DECIMAL(7,2), `wr_fee` DECIMAL(7,2),
        |`wr_return_ship_cost` DECIMAL(7,2), `wr_refunded_cash` DECIMAL(7,2),
        |`wr_reversed_charge` DECIMAL(7,2), `wr_account_credit` DECIMAL(7,2),
        |`wr_net_loss` DECIMAL(7,2)
      """.stripMargin,
    "inventory" ->
      """
        |`inv_date_sk` INT, `inv_item_sk` INT, `inv_warehouse_sk` INT,
        |`inv_quantity_on_hand` INT
      """.stripMargin,
    "store" ->
      """
        |`s_store_sk` INT, `s_store_id` STRING, `s_rec_start_date` DATE,
        |`s_rec_end_date` DATE, `s_closed_date_sk` INT, `s_store_name` STRING,
        |`s_number_employees` INT, `s_floor_space` INT, `s_hours` STRING, `s_manager` STRING,
        |`s_market_id` INT, `s_geography_class` STRING, `s_market_desc` STRING,
        |`s_market_manager` STRING, `s_division_id` INT, `s_division_name` STRING,
        |`s_company_id` INT, `s_company_name` STRING, `s_street_number` STRING,
        |`s_street_name` STRING, `s_street_type` STRING, `s_suite_number` STRING, `s_city` STRING,
        |`s_county` STRING, `s_state` STRING, `s_zip` STRING, `s_country` STRING,
        |`s_gmt_offset` DECIMAL(5,2), `s_tax_percentage` DECIMAL(5,2)
      """.stripMargin,
    "call_center" ->
      """
        |`cc_call_center_sk` INT, `cc_call_center_id` STRING, `cc_rec_start_date` DATE,
        |`cc_rec_end_date` DATE, `cc_closed_date_sk` INT, `cc_open_date_sk` INT, `cc_name` STRING,
        |`cc_class` STRING, `cc_employees` INT, `cc_sq_ft` INT, `cc_hours` STRING,
        |`cc_manager` STRING, `cc_mkt_id` INT, `cc_mkt_class` STRING, `cc_mkt_desc` STRING,
        |`cc_market_manager` STRING, `cc_division` INT, `cc_division_name` STRING,
        |`cc_company` INT, `cc_company_name` STRING, `cc_street_number` STRING,
        |`cc_street_name` STRING, `cc_street_type` STRING, `cc_suite_number` STRING,
        |`cc_city` STRING, `cc_county` STRING, `cc_state` STRING, `cc_zip` STRING,
        |`cc_country` STRING, `cc_gmt_offset` DECIMAL(5,2), `cc_tax_percentage` DECIMAL(5,2)
      """.stripMargin,
    "catalog_page" ->
      """
        |`cp_catalog_page_sk` INT, `cp_catalog_page_id` STRING, `cp_start_date_sk` INT,
        |`cp_end_date_sk` INT, `cp_department` STRING, `cp_catalog_number` INT,
        |`cp_catalog_page_number` INT, `cp_description` STRING, `cp_type` STRING
      """.stripMargin,
    "web_site" ->
      """
        |`web_site_sk` INT, `web_site_id` STRING, `web_rec_start_date` DATE,
        |`web_rec_end_date` DATE, `web_name` STRING, `web_open_date_sk` INT,
        |`web_close_date_sk` INT, `web_class` STRING, `web_manager` STRING, `web_mkt_id` INT,
        |`web_mkt_class` STRING, `web_mkt_desc` STRING, `web_market_manager` STRING,
        |`web_company_id` INT, `web_company_name` STRING, `web_street_number` STRING,
        |`web_street_name` STRING, `web_street_type` STRING, `web_suite_number` STRING,
        |`web_city` STRING, `web_county` STRING, `web_state` STRING, `web_zip` STRING,
        |`web_country` STRING, `web_gmt_offset` DECIMAL(5,2), `web_tax_percentage` DECIMAL(5,2)
      """.stripMargin,
    "web_page" ->
      """
        |`wp_web_page_sk` INT, `wp_web_page_id` STRING,
        |`wp_rec_start_date` DATE, `wp_rec_end_date` DATE, `wp_creation_date_sk` INT,
        |`wp_access_date_sk` INT, `wp_autogen_flag` STRING, `wp_customer_sk` INT,
        |`wp_url` STRING, `wp_type` STRING, `wp_char_count` INT, `wp_link_count` INT,
        |`wp_image_count` INT, `wp_max_ad_count` INT
      """.stripMargin,
    "warehouse" ->
      """
        |`w_warehouse_sk` INT, `w_warehouse_id` STRING, `w_warehouse_name` STRING,
        |`w_warehouse_sq_ft` INT, `w_street_number` STRING, `w_street_name` STRING,
        |`w_street_type` STRING, `w_suite_number` STRING, `w_city` STRING, `w_county` STRING,
        |`w_state` STRING, `w_zip` STRING, `w_country` STRING, `w_gmt_offset` DECIMAL(5,2)
      """.stripMargin,
    "customer" ->
      """
        |`c_customer_sk` INT, `c_customer_id` STRING, `c_current_cdemo_sk` INT,
        |`c_current_hdemo_sk` INT, `c_current_addr_sk` INT, `c_first_shipto_date_sk` INT,
        |`c_first_sales_date_sk` INT, `c_salutation` STRING, `c_first_name` STRING,
        |`c_last_name` STRING, `c_preferred_cust_flag` STRING, `c_birth_day` INT,
        |`c_birth_month` INT, `c_birth_year` INT, `c_birth_country` STRING, `c_login` STRING,
        |`c_email_address` STRING, `c_last_review_date` INT
      """.stripMargin,
    "customer_address" ->
      """
        |`ca_address_sk` INT, `ca_address_id` STRING, `ca_street_number` STRING,
        |`ca_street_name` STRING, `ca_street_type` STRING, `ca_suite_number` STRING,
        |`ca_city` STRING, `ca_county` STRING, `ca_state` STRING, `ca_zip` STRING,
        |`ca_country` STRING, `ca_gmt_offset` DECIMAL(5,2), `ca_location_type` STRING
      """.stripMargin,
    "customer_demographics" ->
      """
        |`cd_demo_sk` INT, `cd_gender` STRING, `cd_marital_status` STRING,
        |`cd_education_status` STRING, `cd_purchase_estimate` INT, `cd_credit_rating` STRING,
        |`cd_dep_count` INT, `cd_dep_employed_count` INT, `cd_dep_college_count` INT
      """.stripMargin,
    "date_dim" ->
      """
        |`d_date_sk` INT, `d_date_id` STRING, `d_date` DATE,
        |`d_month_seq` INT, `d_week_seq` INT, `d_quarter_seq` INT, `d_year` INT, `d_dow` INT,
        |`d_moy` INT, `d_dom` INT, `d_qoy` INT, `d_fy_year` INT, `d_fy_quarter_seq` INT,
        |`d_fy_week_seq` INT, `d_day_name` STRING, `d_quarter_name` STRING, `d_holiday` STRING,
        |`d_weekend` STRING, `d_following_holiday` STRING, `d_first_dom` INT, `d_last_dom` INT,
        |`d_same_day_ly` INT, `d_same_day_lq` INT, `d_current_day` STRING,
        |`d_current_week` STRING, `d_current_month` STRING, `d_current_quarter` STRING,
        |`d_current_year` STRING
      """.stripMargin,
    "household_demographics" ->
      """
        |`hd_demo_sk` INT, `hd_income_band_sk` INT, `hd_buy_potential` STRING, `hd_dep_count` INT,
        |`hd_vehicle_count` INT
      """.stripMargin,
    "item" ->
      """
        |`i_item_sk` INT, `i_item_id` STRING, `i_rec_start_date` DATE,
        |`i_rec_end_date` DATE, `i_item_desc` STRING, `i_current_price` DECIMAL(7,2),
        |`i_wholesale_cost` DECIMAL(7,2), `i_brand_id` INT, `i_brand` STRING, `i_class_id` INT,
        |`i_class` STRING, `i_category_id` INT, `i_category` STRING, `i_manufact_id` INT,
        |`i_manufact` STRING, `i_size` STRING, `i_formulation` STRING, `i_color` STRING,
        |`i_units` STRING, `i_container` STRING, `i_manager_id` INT, `i_product_name` STRING
      """.stripMargin,
    "income_band" ->
      """
        |`ib_income_band_sk` INT, `ib_lower_bound` INT, `ib_upper_bound` INT
      """.stripMargin,
    "promotion" ->
      """
        |`p_promo_sk` INT, `p_promo_id` STRING, `p_start_date_sk` INT, `p_end_date_sk` INT,
        |`p_item_sk` INT, `p_cost` DECIMAL(15,2), `p_response_target` INT, `p_promo_name` STRING,
        |`p_channel_dmail` STRING, `p_channel_email` STRING, `p_channel_catalog` STRING,
        |`p_channel_tv` STRING, `p_channel_radio` STRING, `p_channel_press` STRING,
        |`p_channel_event` STRING, `p_channel_demo` STRING, `p_channel_details` STRING,
        |`p_purpose` STRING, `p_discount_active` STRING
      """.stripMargin,
    "reason" ->
      """
        |`r_reason_sk` INT, `r_reason_id` STRING, `r_reason_desc` STRING
      """.stripMargin,
    "ship_mode" ->
      """
        |`sm_ship_mode_sk` INT, `sm_ship_mode_id` STRING, `sm_type` STRING, `sm_code` STRING,
        |`sm_carrier` STRING, `sm_contract` STRING
      """.stripMargin,
    "time_dim" ->
      """
        |`t_time_sk` INT, `t_time_id` STRING, `t_time` INT, `t_hour` INT, `t_minute` INT,
        |`t_second` INT, `t_am_pm` STRING, `t_shift` STRING, `t_sub_shift` STRING,
        |`t_meal_time` STRING
      """.stripMargin
  )

  val tableNames: Iterable[String] = tableColumns.keys

  def createTable(
      spark: SparkSession,
      tableName: String,
      format: String = "parquet",
      options: Seq[String] = Nil): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |${options.mkString("\n")}
       """.stripMargin)
  }
}
