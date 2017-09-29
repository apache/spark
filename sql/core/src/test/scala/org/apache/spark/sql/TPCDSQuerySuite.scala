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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

/**
 * This test suite ensures all the TPC-DS queries can be successfully analyzed and optimized
 * without hitting the max iteration threshold.
 */
class TPCDSQuerySuite extends QueryTest with SharedSQLContext with BeforeAndAfterAll {

  // When Utils.isTesting is true, the RuleExecutor will issue an exception when hitting
  // the max iteration of analyzer/optimizer batches.
  assert(Utils.isTesting, "spark.testing is not set to true")

  /**
   * Drop all the tables
   */
  protected override def afterAll(): Unit = {
    try {
      spark.sessionState.catalog.reset()
    } finally {
      super.afterAll()
    }
  }

  override def beforeAll() {
    super.beforeAll()
    sql(
      """
        |CREATE TABLE `catalog_page` (
        |`cp_catalog_page_sk` INT, `cp_catalog_page_id` STRING, `cp_start_date_sk` INT,
        |`cp_end_date_sk` INT, `cp_department` STRING, `cp_catalog_number` INT,
        |`cp_catalog_page_number` INT, `cp_description` STRING, `cp_type` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `catalog_returns` (
        |`cr_returned_date_sk` INT, `cr_returned_time_sk` INT, `cr_item_sk` INT,
        |`cr_refunded_customer_sk` INT, `cr_refunded_cdemo_sk` INT, `cr_refunded_hdemo_sk` INT,
        |`cr_refunded_addr_sk` INT, `cr_returning_customer_sk` INT, `cr_returning_cdemo_sk` INT,
        |`cr_returning_hdemo_sk` INT, `cr_returning_addr_sk` INT, `cr_call_center_sk` INT,
        |`cr_catalog_page_sk` INT, `cr_ship_mode_sk` INT, `cr_warehouse_sk` INT, `cr_reason_sk` INT,
        |`cr_order_number` INT, `cr_return_quantity` INT, `cr_return_amount` DECIMAL(7,2),
        |`cr_return_tax` DECIMAL(7,2), `cr_return_amt_inc_tax` DECIMAL(7,2), `cr_fee` DECIMAL(7,2),
        |`cr_return_ship_cost` DECIMAL(7,2), `cr_refunded_cash` DECIMAL(7,2),
        |`cr_reversed_charge` DECIMAL(7,2), `cr_store_credit` DECIMAL(7,2),
        |`cr_net_loss` DECIMAL(7,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `customer` (
        |`c_customer_sk` INT, `c_customer_id` STRING, `c_current_cdemo_sk` INT,
        |`c_current_hdemo_sk` INT, `c_current_addr_sk` INT, `c_first_shipto_date_sk` INT,
        |`c_first_sales_date_sk` INT, `c_salutation` STRING, `c_first_name` STRING,
        |`c_last_name` STRING, `c_preferred_cust_flag` STRING, `c_birth_day` INT,
        |`c_birth_month` INT, `c_birth_year` INT, `c_birth_country` STRING, `c_login` STRING,
        |`c_email_address` STRING, `c_last_review_date` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `customer_address` (
        |`ca_address_sk` INT, `ca_address_id` STRING, `ca_street_number` STRING,
        |`ca_street_name` STRING, `ca_street_type` STRING, `ca_suite_number` STRING,
        |`ca_city` STRING, `ca_county` STRING, `ca_state` STRING, `ca_zip` STRING,
        |`ca_country` STRING, `ca_gmt_offset` DECIMAL(5,2), `ca_location_type` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `customer_demographics` (
        |`cd_demo_sk` INT, `cd_gender` STRING, `cd_marital_status` STRING,
        |`cd_education_status` STRING, `cd_purchase_estimate` INT, `cd_credit_rating` STRING,
        |`cd_dep_count` INT, `cd_dep_employed_count` INT, `cd_dep_college_count` INT)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `date_dim` (
        |`d_date_sk` INT, `d_date_id` STRING, `d_date` STRING,
        |`d_month_seq` INT, `d_week_seq` INT, `d_quarter_seq` INT, `d_year` INT, `d_dow` INT,
        |`d_moy` INT, `d_dom` INT, `d_qoy` INT, `d_fy_year` INT, `d_fy_quarter_seq` INT,
        |`d_fy_week_seq` INT, `d_day_name` STRING, `d_quarter_name` STRING, `d_holiday` STRING,
        |`d_weekend` STRING, `d_following_holiday` STRING, `d_first_dom` INT, `d_last_dom` INT,
        |`d_same_day_ly` INT, `d_same_day_lq` INT, `d_current_day` STRING, `d_current_week` STRING,
        |`d_current_month` STRING, `d_current_quarter` STRING, `d_current_year` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `household_demographics` (
        |`hd_demo_sk` INT, `hd_income_band_sk` INT, `hd_buy_potential` STRING, `hd_dep_count` INT,
        |`hd_vehicle_count` INT)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `inventory` (`inv_date_sk` INT, `inv_item_sk` INT, `inv_warehouse_sk` INT,
        |`inv_quantity_on_hand` INT)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `item` (`i_item_sk` INT, `i_item_id` STRING, `i_rec_start_date` STRING,
        |`i_rec_end_date` STRING, `i_item_desc` STRING, `i_current_price` DECIMAL(7,2),
        |`i_wholesale_cost` DECIMAL(7,2), `i_brand_id` INT, `i_brand` STRING, `i_class_id` INT,
        |`i_class` STRING, `i_category_id` INT, `i_category` STRING, `i_manufact_id` INT,
        |`i_manufact` STRING, `i_size` STRING, `i_formulation` STRING, `i_color` STRING,
        |`i_units` STRING, `i_container` STRING, `i_manager_id` INT, `i_product_name` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `promotion` (
        |`p_promo_sk` INT, `p_promo_id` STRING, `p_start_date_sk` INT, `p_end_date_sk` INT,
        |`p_item_sk` INT, `p_cost` DECIMAL(15,2), `p_response_target` INT, `p_promo_name` STRING,
        |`p_channel_dmail` STRING, `p_channel_email` STRING, `p_channel_catalog` STRING,
        |`p_channel_tv` STRING, `p_channel_radio` STRING, `p_channel_press` STRING,
        |`p_channel_event` STRING, `p_channel_demo` STRING, `p_channel_details` STRING,
        |`p_purpose` STRING, `p_discount_active` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `store` (
        |`s_store_sk` INT, `s_store_id` STRING, `s_rec_start_date` STRING,
        |`s_rec_end_date` STRING, `s_closed_date_sk` INT, `s_store_name` STRING,
        |`s_number_employees` INT, `s_floor_space` INT, `s_hours` STRING, `s_manager` STRING,
        |`s_market_id` INT, `s_geography_class` STRING, `s_market_desc` STRING,
        |`s_market_manager` STRING, `s_division_id` INT, `s_division_name` STRING,
        |`s_company_id` INT, `s_company_name` STRING, `s_street_number` STRING,
        |`s_street_name` STRING, `s_street_type` STRING, `s_suite_number` STRING, `s_city` STRING,
        |`s_county` STRING, `s_state` STRING, `s_zip` STRING, `s_country` STRING,
        |`s_gmt_offset` DECIMAL(5,2), `s_tax_precentage` DECIMAL(5,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `store_returns` (
        |`sr_returned_date_sk` BIGINT, `sr_return_time_sk` BIGINT, `sr_item_sk` BIGINT,
        |`sr_customer_sk` BIGINT, `sr_cdemo_sk` BIGINT, `sr_hdemo_sk` BIGINT, `sr_addr_sk` BIGINT,
        |`sr_store_sk` BIGINT, `sr_reason_sk` BIGINT, `sr_ticket_number` BIGINT,
        |`sr_return_quantity` BIGINT, `sr_return_amt` DECIMAL(7,2), `sr_return_tax` DECIMAL(7,2),
        |`sr_return_amt_inc_tax` DECIMAL(7,2), `sr_fee` DECIMAL(7,2),
        |`sr_return_ship_cost` DECIMAL(7,2), `sr_refunded_cash` DECIMAL(7,2),
        |`sr_reversed_charge` DECIMAL(7,2), `sr_store_credit` DECIMAL(7,2),
        |`sr_net_loss` DECIMAL(7,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `catalog_sales` (
        |`cs_sold_date_sk` INT, `cs_sold_time_sk` INT, `cs_ship_date_sk` INT,
        |`cs_bill_customer_sk` INT, `cs_bill_cdemo_sk` INT, `cs_bill_hdemo_sk` INT,
        |`cs_bill_addr_sk` INT, `cs_ship_customer_sk` INT, `cs_ship_cdemo_sk` INT,
        |`cs_ship_hdemo_sk` INT, `cs_ship_addr_sk` INT, `cs_call_center_sk` INT,
        |`cs_catalog_page_sk` INT, `cs_ship_mode_sk` INT, `cs_warehouse_sk` INT,
        |`cs_item_sk` INT, `cs_promo_sk` INT, `cs_order_number` INT, `cs_quantity` INT,
        |`cs_wholesale_cost` DECIMAL(7,2), `cs_list_price` DECIMAL(7,2),
        |`cs_sales_price` DECIMAL(7,2), `cs_ext_discount_amt` DECIMAL(7,2),
        |`cs_ext_sales_price` DECIMAL(7,2), `cs_ext_wholesale_cost` DECIMAL(7,2),
        |`cs_ext_list_price` DECIMAL(7,2), `cs_ext_tax` DECIMAL(7,2), `cs_coupon_amt` DECIMAL(7,2),
        |`cs_ext_ship_cost` DECIMAL(7,2), `cs_net_paid` DECIMAL(7,2),
        |`cs_net_paid_inc_tax` DECIMAL(7,2), `cs_net_paid_inc_ship` DECIMAL(7,2),
        |`cs_net_paid_inc_ship_tax` DECIMAL(7,2), `cs_net_profit` DECIMAL(7,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `web_sales` (
        |`ws_sold_date_sk` INT, `ws_sold_time_sk` INT, `ws_ship_date_sk` INT, `ws_item_sk` INT,
        |`ws_bill_customer_sk` INT, `ws_bill_cdemo_sk` INT, `ws_bill_hdemo_sk` INT,
        |`ws_bill_addr_sk` INT, `ws_ship_customer_sk` INT, `ws_ship_cdemo_sk` INT,
        |`ws_ship_hdemo_sk` INT, `ws_ship_addr_sk` INT, `ws_web_page_sk` INT, `ws_web_site_sk` INT,
        |`ws_ship_mode_sk` INT, `ws_warehouse_sk` INT, `ws_promo_sk` INT, `ws_order_number` INT,
        |`ws_quantity` INT, `ws_wholesale_cost` DECIMAL(7,2), `ws_list_price` DECIMAL(7,2),
        |`ws_sales_price` DECIMAL(7,2), `ws_ext_discount_amt` DECIMAL(7,2),
        |`ws_ext_sales_price` DECIMAL(7,2), `ws_ext_wholesale_cost` DECIMAL(7,2),
        |`ws_ext_list_price` DECIMAL(7,2), `ws_ext_tax` DECIMAL(7,2),
        |`ws_coupon_amt` DECIMAL(7,2), `ws_ext_ship_cost` DECIMAL(7,2), `ws_net_paid` DECIMAL(7,2),
        |`ws_net_paid_inc_tax` DECIMAL(7,2), `ws_net_paid_inc_ship` DECIMAL(7,2),
        |`ws_net_paid_inc_ship_tax` DECIMAL(7,2), `ws_net_profit` DECIMAL(7,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `store_sales` (
        |`ss_sold_date_sk` INT, `ss_sold_time_sk` INT, `ss_item_sk` INT, `ss_customer_sk` INT,
        |`ss_cdemo_sk` INT, `ss_hdemo_sk` INT, `ss_addr_sk` INT, `ss_store_sk` INT,
        |`ss_promo_sk` INT, `ss_ticket_number` INT, `ss_quantity` INT,
        |`ss_wholesale_cost` DECIMAL(7,2), `ss_list_price` DECIMAL(7,2),
        |`ss_sales_price` DECIMAL(7,2), `ss_ext_discount_amt` DECIMAL(7,2),
        |`ss_ext_sales_price` DECIMAL(7,2), `ss_ext_wholesale_cost` DECIMAL(7,2),
        |`ss_ext_list_price` DECIMAL(7,2), `ss_ext_tax` DECIMAL(7,2),
        |`ss_coupon_amt` DECIMAL(7,2), `ss_net_paid` DECIMAL(7,2),
        |`ss_net_paid_inc_tax` DECIMAL(7,2), `ss_net_profit` DECIMAL(7,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `web_returns` (
        |`wr_returned_date_sk` BIGINT, `wr_returned_time_sk` BIGINT, `wr_item_sk` BIGINT,
        |`wr_refunded_customer_sk` BIGINT, `wr_refunded_cdemo_sk` BIGINT,
        |`wr_refunded_hdemo_sk` BIGINT, `wr_refunded_addr_sk` BIGINT,
        |`wr_returning_customer_sk` BIGINT, `wr_returning_cdemo_sk` BIGINT,
        |`wr_returning_hdemo_sk` BIGINT, `wr_returning_addr_sk` BIGINT, `wr_web_page_sk` BIGINT,
        |`wr_reason_sk` BIGINT, `wr_order_number` BIGINT, `wr_return_quantity` BIGINT,
        |`wr_return_amt` DECIMAL(7,2), `wr_return_tax` DECIMAL(7,2),
        |`wr_return_amt_inc_tax` DECIMAL(7,2), `wr_fee` DECIMAL(7,2),
        |`wr_return_ship_cost` DECIMAL(7,2), `wr_refunded_cash` DECIMAL(7,2),
        |`wr_reversed_charge` DECIMAL(7,2), `wr_account_credit` DECIMAL(7,2),
        |`wr_net_loss` DECIMAL(7,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `web_site` (
        |`web_site_sk` INT, `web_site_id` STRING, `web_rec_start_date` DATE,
        |`web_rec_end_date` DATE, `web_name` STRING, `web_open_date_sk` INT,
        |`web_close_date_sk` INT, `web_class` STRING, `web_manager` STRING, `web_mkt_id` INT,
        |`web_mkt_class` STRING, `web_mkt_desc` STRING, `web_market_manager` STRING,
        |`web_company_id` INT, `web_company_name` STRING, `web_street_number` STRING,
        |`web_street_name` STRING, `web_street_type` STRING, `web_suite_number` STRING,
        |`web_city` STRING, `web_county` STRING, `web_state` STRING, `web_zip` STRING,
        |`web_country` STRING, `web_gmt_offset` STRING, `web_tax_percentage` DECIMAL(5,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `reason` (
        |`r_reason_sk` INT, `r_reason_id` STRING, `r_reason_desc` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `call_center` (
        |`cc_call_center_sk` INT, `cc_call_center_id` STRING, `cc_rec_start_date` DATE,
        |`cc_rec_end_date` DATE, `cc_closed_date_sk` INT, `cc_open_date_sk` INT, `cc_name` STRING,
        |`cc_class` STRING, `cc_employees` INT, `cc_sq_ft` INT, `cc_hours` STRING,
        |`cc_manager` STRING, `cc_mkt_id` INT, `cc_mkt_class` STRING, `cc_mkt_desc` STRING,
        |`cc_market_manager` STRING, `cc_division` INT, `cc_division_name` STRING, `cc_company` INT,
        |`cc_company_name` STRING, `cc_street_number` STRING, `cc_street_name` STRING,
        |`cc_street_type` STRING, `cc_suite_number` STRING, `cc_city` STRING, `cc_county` STRING,
        |`cc_state` STRING, `cc_zip` STRING, `cc_country` STRING, `cc_gmt_offset` DECIMAL(5,2),
        |`cc_tax_percentage` DECIMAL(5,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `warehouse` (
        |`w_warehouse_sk` INT, `w_warehouse_id` STRING, `w_warehouse_name` STRING,
        |`w_warehouse_sq_ft` INT, `w_street_number` STRING, `w_street_name` STRING,
        |`w_street_type` STRING, `w_suite_number` STRING, `w_city` STRING, `w_county` STRING,
        |`w_state` STRING, `w_zip` STRING, `w_country` STRING, `w_gmt_offset` DECIMAL(5,2))
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `ship_mode` (
        |`sm_ship_mode_sk` INT, `sm_ship_mode_id` STRING, `sm_type` STRING, `sm_code` STRING,
        |`sm_carrier` STRING, `sm_contract` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `income_band` (
        |`ib_income_band_sk` INT, `ib_lower_bound` INT, `ib_upper_bound` INT)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `time_dim` (
        |`t_time_sk` INT, `t_time_id` STRING, `t_time` INT, `t_hour` INT, `t_minute` INT,
        |`t_second` INT, `t_am_pm` STRING, `t_shift` STRING, `t_sub_shift` STRING,
        |`t_meal_time` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `web_page` (`wp_web_page_sk` INT, `wp_web_page_id` STRING,
        |`wp_rec_start_date` DATE, `wp_rec_end_date` DATE, `wp_creation_date_sk` INT,
        |`wp_access_date_sk` INT, `wp_autogen_flag` STRING, `wp_customer_sk` INT,
        |`wp_url` STRING, `wp_type` STRING, `wp_char_count` INT, `wp_link_count` INT,
        |`wp_image_count` INT, `wp_max_ad_count` INT)
        |USING parquet
      """.stripMargin)
  }

  val tpcdsQueries = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
    "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
    "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
    "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
    "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
    "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
    "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
    "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
    "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

  tpcdsQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(name) {
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        // Just check the plans can be properly generated
        sql(queryString).queryExecution.executedPlan
      }
    }
  }

  // These queries are from https://github.com/cloudera/impala-tpcds-kit/tree/master/queries
  val modifiedTPCDSQueries = Seq(
    "q3", "q7", "q10", "q19", "q27", "q34", "q42", "q43", "q46", "q52", "q53", "q55", "q59",
    "q63", "q65", "q68", "q73", "q79", "q89", "q98", "ss_max")

  modifiedTPCDSQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds-modifiedQueries/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(s"modified-$name") {
      // Just check the plans can be properly generated
      sql(queryString).queryExecution.executedPlan
    }
  }
}
