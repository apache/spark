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

trait TPCHBase extends TPCBase {

  override def createTables(): Unit = {
    tpchCreateTable.values.foreach { sql =>
      spark.sql(sql)
    }
  }

  override def dropTables(): Unit = {
    tpchCreateTable.keys.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
  }

  val tpchCreateTable = Map(
    "orders" ->
      """
        |CREATE TABLE `orders` (
        |`o_orderkey` BIGINT, `o_custkey` BIGINT, `o_orderstatus` STRING,
        |`o_totalprice` DECIMAL(10,0), `o_orderdate` DATE, `o_orderpriority` STRING,
        |`o_clerk` STRING, `o_shippriority` INT, `o_comment` STRING)
        |USING parquet
      """.stripMargin,
    "nation" ->
      """
        |CREATE TABLE `nation` (
        |`n_nationkey` BIGINT, `n_name` STRING, `n_regionkey` BIGINT, `n_comment` STRING)
        |USING parquet
      """.stripMargin,
    "region" ->
      """
        |CREATE TABLE `region` (
        |`r_regionkey` BIGINT, `r_name` STRING, `r_comment` STRING)
        |USING parquet
      """.stripMargin,
    "part" ->
      """
        |CREATE TABLE `part` (`p_partkey` BIGINT, `p_name` STRING, `p_mfgr` STRING,
        |`p_brand` STRING, `p_type` STRING, `p_size` INT, `p_container` STRING,
        |`p_retailprice` DECIMAL(10,0), `p_comment` STRING)
        |USING parquet
      """.stripMargin,
    "partsupp" ->
      """
        |CREATE TABLE `partsupp` (`ps_partkey` BIGINT, `ps_suppkey` BIGINT,
        |`ps_availqty` INT, `ps_supplycost` DECIMAL(10,0), `ps_comment` STRING)
        |USING parquet
      """.stripMargin,
    "customer" ->
      """
        |CREATE TABLE `customer` (`c_custkey` BIGINT, `c_name` STRING, `c_address` STRING,
        |`c_nationkey` BIGINT, `c_phone` STRING, `c_acctbal` DECIMAL(10,0),
        |`c_mktsegment` STRING, `c_comment` STRING)
        |USING parquet
      """.stripMargin,
    "supplier" ->
      """
        |CREATE TABLE `supplier` (`s_suppkey` BIGINT, `s_name` STRING, `s_address` STRING,
        |`s_nationkey` BIGINT, `s_phone` STRING, `s_acctbal` DECIMAL(10,0), `s_comment` STRING)
        |USING parquet
      """.stripMargin,
    "lineitem" ->
      """
        |CREATE TABLE `lineitem` (`l_orderkey` BIGINT, `l_partkey` BIGINT, `l_suppkey` BIGINT,
        |`l_linenumber` INT, `l_quantity` DECIMAL(10,0), `l_extendedprice` DECIMAL(10,0),
        |`l_discount` DECIMAL(10,0), `l_tax` DECIMAL(10,0), `l_returnflag` STRING,
        |`l_linestatus` STRING, `l_shipdate` DATE, `l_commitdate` DATE, `l_receiptdate` DATE,
        |`l_shipinstruct` STRING, `l_shipmode` STRING, `l_comment` STRING)
        |USING parquet
      """.stripMargin)

  val tpchQueries = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22")
}
