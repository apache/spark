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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait TPCDSBase extends SharedSparkSession {

  // The TPCDS queries below are based on v1.4
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

  // This list only includes TPCDS v2.7 queries that are different from v1.4 ones
  val tpcdsQueriesV2_7_0 = Seq(
    "q5a", "q6", "q10a", "q11", "q12", "q14", "q14a", "q18a",
    "q20", "q22", "q22a", "q24", "q27a", "q34", "q35", "q35a", "q36a", "q47", "q49",
    "q51a", "q57", "q64", "q67a", "q70a", "q72", "q74", "q75", "q77a", "q78",
    "q80a", "q86a", "q98")

  // These queries are from https://github.com/cloudera/impala-tpcds-kit/tree/master/queries
  val modifiedTPCDSQueries = Seq(
    "q3", "q7", "q10", "q19", "q27", "q34", "q42", "q43", "q46", "q52", "q53", "q55", "q59",
    "q63", "q65", "q68", "q73", "q79", "q89", "q98", "ss_max")

  private val tableColumns = Map(
    "store_sales" ->
      """
        |`ss_sold_date_sk` INT,
        |`ss_sold_time_sk` INT,
        |`ss_item_sk` INT,
        |`ss_customer_sk` INT,
        |`ss_cdemo_sk` INT,
        |`ss_hdemo_sk` INT,
        |`ss_addr_sk` INT,
        |`ss_store_sk` INT,
        |`ss_promo_sk` INT,
        |`ss_ticket_number` INT,
        |`ss_quantity` INT,
        |`ss_wholesale_cost` DECIMAL(7,2),
        |`ss_list_price` DECIMAL(7,2),
        |`ss_sales_price` DECIMAL(7,2),
        |`ss_ext_discount_amt` DECIMAL(7,2),
        |`ss_ext_sales_price` DECIMAL(7,2),
        |`ss_ext_wholesale_cost` DECIMAL(7,2),
        |`ss_ext_list_price` DECIMAL(7,2),
        |`ss_ext_tax` DECIMAL(7,2),
        |`ss_coupon_amt` DECIMAL(7,2),
        |`ss_net_paid` DECIMAL(7,2),
        |`ss_net_paid_inc_tax` DECIMAL(7,2),
        |`ss_net_profit` DECIMAL(7,2)
      """.stripMargin,
    "store_returns" ->
      """
        |`sr_returned_date_sk` BIGINT,
        |`sr_return_time_sk` BIGINT,
        |`sr_item_sk` BIGINT,
        |`sr_customer_sk` BIGINT,
        |`sr_cdemo_sk` BIGINT,
        |`sr_hdemo_sk` BIGINT,
        |`sr_addr_sk` BIGINT,
        |`sr_store_sk` BIGINT,
        |`sr_reason_sk` BIGINT,
        |`sr_ticket_number` BIGINT,
        |`sr_return_quantity` INT,
        |`sr_return_amt` DECIMAL(7,2),
        |`sr_return_tax` DECIMAL(7,2),
        |`sr_return_amt_inc_tax` DECIMAL(7,2),
        |`sr_fee` DECIMAL(7,2),
        |`sr_return_ship_cost` DECIMAL(7,2),
        |`sr_refunded_cash` DECIMAL(7,2),
        |`sr_reversed_charge` DECIMAL(7,2),
        |`sr_store_credit` DECIMAL(7,2),
        |`sr_net_loss` DECIMAL(7,2)
      """.stripMargin,
    "catalog_sales" ->
      """
        |`cs_sold_date_sk` INT,
        |`cs_sold_time_sk` INT,
        |`cs_ship_date_sk` INT,
        |`cs_bill_customer_sk` INT,
        |`cs_bill_cdemo_sk` INT,
        |`cs_bill_hdemo_sk` INT,
        |`cs_bill_addr_sk` INT,
        |`cs_ship_customer_sk` INT,
        |`cs_ship_cdemo_sk` INT,
        |`cs_ship_hdemo_sk` INT,
        |`cs_ship_addr_sk` INT,
        |`cs_call_center_sk` INT,
        |`cs_catalog_page_sk` INT,
        |`cs_ship_mode_sk` INT,
        |`cs_warehouse_sk` INT,
        |`cs_item_sk` INT,
        |`cs_promo_sk` INT,
        |`cs_order_number` INT,
        |`cs_quantity` INT,
        |`cs_wholesale_cost` DECIMAL(7,2),
        |`cs_list_price` DECIMAL(7,2),
        |`cs_sales_price` DECIMAL(7,2),
        |`cs_ext_discount_amt` DECIMAL(7,2),
        |`cs_ext_sales_price` DECIMAL(7,2),
        |`cs_ext_wholesale_cost` DECIMAL(7,2),
        |`cs_ext_list_price` DECIMAL(7,2),
        |`cs_ext_tax` DECIMAL(7,2),
        |`cs_coupon_amt` DECIMAL(7,2),
        |`cs_ext_ship_cost` DECIMAL(7,2),
        |`cs_net_paid` DECIMAL(7,2),
        |`cs_net_paid_inc_tax` DECIMAL(7,2),
        |`cs_net_paid_inc_ship` DECIMAL(7,2),
        |`cs_net_paid_inc_ship_tax` DECIMAL(7,2),
        |`cs_net_profit` DECIMAL(7,2)
      """.stripMargin,
    "catalog_returns" ->
      """
        |`cr_returned_date_sk` INT,
        |`cr_returned_time_sk` INT,
        |`cr_item_sk` INT,
        |`cr_refunded_customer_sk` INT,
        |`cr_refunded_cdemo_sk` INT,
        |`cr_refunded_hdemo_sk` INT,
        |`cr_refunded_addr_sk` INT,
        |`cr_returning_customer_sk` INT,
        |`cr_returning_cdemo_sk` INT,
        |`cr_returning_hdemo_sk` INT,
        |`cr_returning_addr_sk` INT,
        |`cr_call_center_sk` INT,
        |`cr_catalog_page_sk` INT,
        |`cr_ship_mode_sk` INT,
        |`cr_warehouse_sk` INT,
        |`cr_reason_sk` INT,`cr_order_number` INT,
        |`cr_return_quantity` INT,
        |`cr_return_amount` DECIMAL(7,2),
        |`cr_return_tax` DECIMAL(7,2),
        |`cr_return_amt_inc_tax` DECIMAL(7,2),
        |`cr_fee` DECIMAL(7,2),
        |`cr_return_ship_cost` DECIMAL(7,2),
        |`cr_refunded_cash` DECIMAL(7,2),
        |`cr_reversed_charge` DECIMAL(7,2),
        |`cr_store_credit` DECIMAL(7,2),
        |`cr_net_loss` DECIMAL(7,2)
      """.stripMargin,
    "web_sales" ->
      """
        |`ws_sold_date_sk` INT,
        |`ws_sold_time_sk` INT,
        |`ws_ship_date_sk` INT,
        |`ws_item_sk` INT,
        |`ws_bill_customer_sk` INT,
        |`ws_bill_cdemo_sk` INT,
        |`ws_bill_hdemo_sk` INT,
        |`ws_bill_addr_sk` INT,
        |`ws_ship_customer_sk` INT,
        |`ws_ship_cdemo_sk` INT,
        |`ws_ship_hdemo_sk` INT,
        |`ws_ship_addr_sk` INT,
        |`ws_web_page_sk` INT,
        |`ws_web_site_sk` INT,
        |`ws_ship_mode_sk` INT,
        |`ws_warehouse_sk` INT,
        |`ws_promo_sk` INT,
        |`ws_order_number` INT,
        |`ws_quantity` INT,
        |`ws_wholesale_cost` DECIMAL(7,2),
        |`ws_list_price` DECIMAL(7,2),
        |`ws_sales_price` DECIMAL(7,2),
        |`ws_ext_discount_amt` DECIMAL(7,2),
        |`ws_ext_sales_price` DECIMAL(7,2),
        |`ws_ext_wholesale_cost` DECIMAL(7,2),
        |`ws_ext_list_price` DECIMAL(7,2),
        |`ws_ext_tax` DECIMAL(7,2),
        |`ws_coupon_amt` DECIMAL(7,2),
        |`ws_ext_ship_cost` DECIMAL(7,2),
        |`ws_net_paid` DECIMAL(7,2),
        |`ws_net_paid_inc_tax` DECIMAL(7,2),
        |`ws_net_paid_inc_ship` DECIMAL(7,2),
        |`ws_net_paid_inc_ship_tax` DECIMAL(7,2),
        |`ws_net_profit` DECIMAL(7,2)
      """.stripMargin,
    "web_returns" ->
      """
        |`wr_returned_date_sk` BIGINT,
        |`wr_returned_time_sk` BIGINT,
        |`wr_item_sk` BIGINT,
        |`wr_refunded_customer_sk` BIGINT,
        |`wr_refunded_cdemo_sk` BIGINT,
        |`wr_refunded_hdemo_sk` BIGINT,
        |`wr_refunded_addr_sk` BIGINT,
        |`wr_returning_customer_sk` BIGINT,
        |`wr_returning_cdemo_sk` BIGINT,
        |`wr_returning_hdemo_sk` BIGINT,
        |`wr_returning_addr_sk` BIGINT,
        |`wr_web_page_sk` BIGINT,
        |`wr_reason_sk` BIGINT,
        |`wr_order_number` BIGINT,
        |`wr_return_quantity` INT,
        |`wr_return_amt` DECIMAL(7,2),
        |`wr_return_tax` DECIMAL(7,2),
        |`wr_return_amt_inc_tax` DECIMAL(7,2),
        |`wr_fee` DECIMAL(7,2),
        |`wr_return_ship_cost` DECIMAL(7,2),
        |`wr_refunded_cash` DECIMAL(7,2),
        |`wr_reversed_charge` DECIMAL(7,2),
        |`wr_account_credit` DECIMAL(7,2),
        |`wr_net_loss` DECIMAL(7,2)
      """.stripMargin,
    "inventory" ->
      """
        |`inv_date_sk` INT,
        |`inv_item_sk` INT,
        |`inv_warehouse_sk` INT,
        |`inv_quantity_on_hand` INT
      """.stripMargin,
    "store" ->
      """
        |`s_store_sk` INT,
        |`s_store_id` char(16),
        |`s_rec_start_date` DATE,
        |`s_rec_end_date` DATE,
        |`s_closed_date_sk` INT,
        |`s_store_name` varchar(50),
        |`s_number_employees` INT,
        |`s_floor_space` INT,
        |`s_hours` char(20),
        |`s_manager` varchar(40),
        |`s_market_id` INT,
        |`s_geography_class` varchar(100),
        |`s_market_desc` varchar(100),
        |`s_market_manager` varchar(40),
        |`s_division_id` INT,
        |`s_division_name` varchar(50),
        |`s_company_id` INT,
        |`s_company_name` varchar(50),
        |`s_street_number` varchar(10),
        |`s_street_name` varchar(60),
        |`s_street_type` char(15),
        |`s_suite_number` char(10),
        |`s_city` varchar(60),
        |`s_county` varchar(30),
        |`s_state` char(2),
        |`s_zip` char(10),
        |`s_country` varchar(20),
        |`s_gmt_offset` DECIMAL(5,2),
        |`s_tax_percentage` DECIMAL(5,2)
      """.stripMargin,
    "call_center" ->
      """
        |`cc_call_center_sk` INT,
        |`cc_call_center_id` char(16),
        |`cc_rec_start_date` DATE,
        |`cc_rec_end_date` DATE,
        |`cc_closed_date_sk` INT,
        |`cc_open_date_sk` INT,
        |`cc_name` varchar(50),
        |`cc_class` varchar(50),
        |`cc_employees` INT,
        |`cc_sq_ft` INT,
        |`cc_hours` char(20),
        |`cc_manager` varchar(40),
        |`cc_mkt_id` INT,
        |`cc_mkt_class` char(50),
        |`cc_mkt_desc` varchar(100),
        |`cc_market_manager` varchar(40),
        |`cc_division` INT,
        |`cc_division_name` varchar(50),
        |`cc_company` INT,
        |`cc_company_name` char(50),
        |`cc_street_number` char(10),
        |`cc_street_name` varchar(60),
        |`cc_street_type` char(15),
        |`cc_suite_number` char(10),
        |`cc_city` varchar(60),
        |`cc_county` varchar(30),
        |`cc_state` char(2),
        |`cc_zip` char(10),
        |`cc_country` varchar(20),
        |`cc_gmt_offset` DECIMAL(5,2),
        |`cc_tax_percentage` DECIMAL(5,2)
      """.stripMargin,
    "catalog_page" ->
      """
        |`cp_catalog_page_sk` INT,
        |`cp_catalog_page_id` char(16),
        |`cp_start_date_sk` INT,
        |`cp_end_date_sk` INT,
        |`cp_department` varchar(50),
        |`cp_catalog_number` INT,
        |`cp_catalog_page_number` INT,
        |`cp_description` varchar(100),
        |`cp_type` varchar(100)
      """.stripMargin,
    "web_site" ->
      """
        |`web_site_sk` INT,
        |`web_site_id` char(16),
        |`web_rec_start_date` DATE,
        |`web_rec_end_date` DATE,
        |`web_name` varchar(50),
        |`web_open_date_sk` INT,
        |`web_close_date_sk` INT,
        |`web_class` varchar(50),
        |`web_manager` varchar(40),
        |`web_mkt_id` INT,
        |`web_mkt_class` varchar(50),
        |`web_mkt_desc` varchar(100),
        |`web_market_manager` varchar(40),
        |`web_company_id` INT,
        |`web_company_name` char(50),
        |`web_street_number` char(10),
        |`web_street_name` varchar(60),
        |`web_street_type` char(15),
        |`web_suite_number` char(10),
        |`web_city` varchar(60),
        |`web_county` varchar(30),
        |`web_state` char(2),
        |`web_zip` char(10),
        |`web_country` varchar(20),
        |`web_gmt_offset` DECIMAL(5,2),
        |`web_tax_percentage` DECIMAL(5,2)
      """.stripMargin,
    "web_page" ->
      """
        |`wp_web_page_sk` INT,
        |`wp_web_page_id` char(16),
        |`wp_rec_start_date` DATE,
        |`wp_rec_end_date` DATE,
        |`wp_creation_date_sk` INT,
        |`wp_access_date_sk` INT,
        |`wp_autogen_flag` char(1),
        |`wp_customer_sk` INT,
        |`wp_url` varchar(100),
        |`wp_type` char(50),
        |`wp_char_count` INT,
        |`wp_link_count` INT,
        |`wp_image_count` INT,
        |`wp_max_ad_count` INT
      """.stripMargin,
    "warehouse" ->
      """
        |`w_warehouse_sk` INT,
        |`w_warehouse_id` char(16),
        |`w_warehouse_name` varchar(20),
        |`w_warehouse_sq_ft` INT,
        |`w_street_number` char(10),
        |`w_street_name` varchar(20),
        |`w_street_type` char(15),
        |`w_suite_number` char(10),
        |`w_city` varchar(60),
        |`w_county` varchar(30),
        |`w_state` char(2),
        |`w_zip` char(10),
        |`w_country` varchar(20),
        |`w_gmt_offset` DECIMAL(5,2)
      """.stripMargin,
    "customer" ->
      """
        |`c_customer_sk` INT,
        |`c_customer_id` char(16),
        |`c_current_cdemo_sk` INT,
        |`c_current_hdemo_sk` INT,
        |`c_current_addr_sk` INT,
        |`c_first_shipto_date_sk` INT,
        |`c_first_sales_date_sk` INT,
        |`c_salutation` char(10),
        |`c_first_name` char(20),
        |`c_last_name` char(30),
        |`c_preferred_cust_flag` char(1),
        |`c_birth_day` INT,
        |`c_birth_month` INT,
        |`c_birth_year` INT,
        |`c_birth_country` varchar(20),
        |`c_login` char(13),
        |`c_email_address` char(50),
        |`c_last_review_date` INT
      """.stripMargin,
    "customer_address" ->
      """
        |`ca_address_sk` INT,
        |`ca_address_id` char(16),
        |`ca_street_number` char(10),
        |`ca_street_name` varchar(60),
        |`ca_street_type` char(15),
        |`ca_suite_number` char(10),
        |`ca_city` varchar(60),
        |`ca_county` varchar(30),
        |`ca_state` char(2),
        |`ca_zip` char(10),
        |`ca_country` varchar(20),
        |`ca_gmt_offset` DECIMAL(5,2),
        |`ca_location_type` char(20)
      """.stripMargin,
    "customer_demographics" ->
      """
        |`cd_demo_sk` INT,
        |`cd_gender` char(1),
        |`cd_marital_status` char(1),
        |`cd_education_status` char(20),
        |`cd_purchase_estimate` INT,
        |`cd_credit_rating` char(10),
        |`cd_dep_count` INT,
        |`cd_dep_employed_count` INT,
        |`cd_dep_college_count` INT
      """.stripMargin,
    "date_dim" ->
      """
        |`d_date_sk` INT,
        |`d_date_id` char(16),
        |`d_date` DATE,
        |`d_month_seq` INT,
        |`d_week_seq` INT,
        |`d_quarter_seq` INT,
        |`d_year` INT,
        |`d_dow` INT,
        |`d_moy` INT,
        |`d_dom` INT,
        |`d_qoy` INT,
        |`d_fy_year` INT,
        |`d_fy_quarter_seq` INT,
        |`d_fy_week_seq` INT,
        |`d_day_name` char(9),
        |`d_quarter_name` char(1),
        |`d_holiday` char(1),
        |`d_weekend` char(1),
        |`d_following_holiday` char(1),
        |`d_first_dom` INT,
        |`d_last_dom` INT,
        |`d_same_day_ly` INT,
        |`d_same_day_lq` INT,
        |`d_current_day` char(1),
        |`d_current_week` char(1),
        |`d_current_month` char(1),
        |`d_current_quarter` char(1),
        |`d_current_year` char(1)
      """.stripMargin,
    "household_demographics" ->
      """
        |`hd_demo_sk` INT,
        |`hd_income_band_sk` INT,
        |`hd_buy_potential` char(15),
        |`hd_dep_count` INT,
        |`hd_vehicle_count` INT
      """.stripMargin,
    "item" ->
      """
        |`i_item_sk` INT,
        |`i_item_id` char(16),
        |`i_rec_start_date` DATE,
        |`i_rec_end_date` DATE,
        |`i_item_desc` varchar(200),
        |`i_current_price` DECIMAL(7,2),
        |`i_wholesale_cost` DECIMAL(7,2),
        |`i_brand_id` INT,
        |`i_brand` char(50),
        |`i_class_id` INT,
        |`i_class` char(50),
        |`i_category_id` INT,
        |`i_category` char(50),
        |`i_manufact_id` INT,
        |`i_manufact` char(50),
        |`i_size` char(20),
        |`i_formulation` char(20),
        |`i_color` char(20),
        |`i_units` char(10),
        |`i_container` char(10),
        |`i_manager_id` INT,
        |`i_product_name` char(50)
      """.stripMargin,
    "income_band" ->
      """
        |`ib_income_band_sk` INT,
        |`ib_lower_bound` INT,
        |`ib_upper_bound` INT
      """.stripMargin,
    "promotion" ->
      """
        |`p_promo_sk` INT,
        |`p_promo_id` char(16),
        |`p_start_date_sk` INT,
        |`p_end_date_sk` INT,
        |`p_item_sk` INT,
        |`p_cost` DECIMAL(15,2),
        |`p_response_target` INT,
        |`p_promo_name` char(50),
        |`p_channel_dmail` char(1),
        |`p_channel_email` char(1),
        |`p_channel_catalog` char(1),
        |`p_channel_tv` char(1),
        |`p_channel_radio` char(1),
        |`p_channel_press` char(1),
        |`p_channel_event` char(1),
        |`p_channel_demo` char(1),
        |`p_channel_details` varchar(100),
        |`p_purpose` char(15),
        |`p_discount_active` char(1)
      """.stripMargin,
    "reason" ->
      """
        |`r_reason_sk` INT,
        |`r_reason_id` char(16),
        |`r_reason_desc` char(100)
      """.stripMargin,
    "ship_mode" ->
      """
        |`sm_ship_mode_sk` INT,
        |`sm_ship_mode_id` char(16),
        |`sm_type` char(30),
        |`sm_code` char(10),
        |`sm_carrier` char(20),
        |`sm_contract` char(20)
      """.stripMargin,
    "time_dim" ->
      """
        |`t_time_sk` INT,
        |`t_time_id` char(16),
        |`t_time` INT,
        |`t_hour` INT,
        |`t_minute` INT,
        |`t_second` INT,
        |`t_am_pm` char(2),
        |`t_shift` char(20),
        |`t_sub_shift` char(20),
        |`t_meal_time` char(20)
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

  private val originalCBCEnabled = conf.cboEnabled
  private val originalPlanStatsEnabled = conf.planStatsEnabled
  private val originalJoinReorderEnabled = conf.joinReorderEnabled

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (injectStats) {
      // Sets configurations for enabling the optimization rules that
      // exploit data statistics.
      conf.setConf(SQLConf.CBO_ENABLED, true)
      conf.setConf(SQLConf.PLAN_STATS_ENABLED, true)
      conf.setConf(SQLConf.JOIN_REORDER_ENABLED, true)
    }
    tableNames.foreach { tableName =>
      createTable(spark, tableName)
      if (injectStats) {
        // To simulate plan generation on actual TPC-DS data, injects data stats here
        spark.sessionState.catalog.alterTableStats(
          TableIdentifier(tableName), Some(TPCDSTableStats.sf100TableStats(tableName)))
      }
    }
  }

  override def afterAll(): Unit = {
    conf.setConf(SQLConf.CBO_ENABLED, originalCBCEnabled)
    conf.setConf(SQLConf.PLAN_STATS_ENABLED, originalPlanStatsEnabled)
    conf.setConf(SQLConf.JOIN_REORDER_ENABLED, originalJoinReorderEnabled)
    tableNames.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
    super.afterAll()
  }

  protected def injectStats: Boolean = false
}
