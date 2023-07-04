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

/**
 * Base trait for TPC-H related tests.
 *
 * Datatype mapping for TPC-H and Spark SQL
 * see more at:
 *   https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf
 *
 *    |---------------|---------------|
 *    |    TPC-H      |  Spark  SQL   |
 *    |---------------|---------------|
 *    |  Identifier   |      INT      |
 *    |---------------|---------------|
 *    |    Integer    |      INT      |
 *    |---------------|---------------|
 *    |   decimal     | Decimal(12, 2)|
 *    |---------------|---------------|
 *    |  fixed text   |    Char(N)    |
 *    |---------------|---------------|
 *    | variable text |  Varchar(N)   |
 *    |---------------|---------------|
 *    |     Date      |     Date      |
 *    |---------------|---------------|
 */
trait TPCHSchema {

  protected val tableColumns: Map[String, String] = Map(
    "part" ->
      """
        |`p_partkey` INT,
        |`p_name` VARCHAR(55),
        |`p_mfgr` CHAR(25),
        |`p_brand` CHAR(10),
        |`p_type` VARCHAR(25),
        |`p_size` INT,
        |`p_container` CHAR(10),
        |`p_retailprice` DECIMAL(12,2),
        |`p_comment` VARCHAR(23)
        |""".stripMargin,
    "supplier" ->
      """
        |`s_suppkey` INT,
        |`s_name` CHAR(25),
        |`s_address` VARCHAR(40),
        |`s_nationkey` INT,
        |`s_phone` CHAR(15),
        |`s_acctbal` DECIMAL(12,2),
        |`s_comment` VARCHAR(101)
        |""".stripMargin,
    "partsupp" ->
      """
        |`ps_partkey` INT,
        |`ps_suppkey` INT,
        |`ps_availqty` INT,
        |`ps_supplycost` DECIMAL(12,2),
        |`ps_comment` VARCHAR(199)
        |""".stripMargin,
    "customer" ->
      """
        |`c_custkey` INT,
        |`c_name` VARCHAR(25),
        |`c_address` VARCHAR(40),
        |`c_nationkey` INT,
        |`c_phone` CHAR(15),
        |`c_acctbal` DECIMAL(12,2),
        |`c_mktsegment` CHAR(10),
        |`c_comment` VARCHAR(117)
        |""".stripMargin,
    "orders" ->
      """
        |`o_orderkey` INT,
        |`o_custkey` INT,
        |`o_orderstatus` CHAR(1),
        |`o_totalprice` DECIMAL(12, 2),
        |`o_orderdate` DATE,
        |`o_orderpriority` CHAR(15),
        |`o_clerk` CHAR(15),
        |`o_shippriority` INT,
        |`o_comment` VARCHAR(79)
        |""".stripMargin,
    "lineitem" ->
      """
        |`l_orderkey` INT,
        |`l_partkey` INT,
        |`l_suppkey` INT,
        |`l_linenumber` INT,
        |`l_quantity` DECIMAL(12,2),
        |`l_extendedprice` DECIMAL(12,2),
        |`l_discount` DECIMAL(12,2),
        |`l_tax` DECIMAL(12,2),
        |`l_returnflag` CHAR(1),
        |`l_linestatus` CHAR(1),
        |`l_shipdate` DATE,
        |`l_commitdate` DATE,
        |`l_receiptdate` DATE,
        |`l_shipinstruct` CHAR(25),
        |`l_shipmode` CHAR(10),
        |`l_comment` VARCHAR(44)
      """.stripMargin,
    "nation" ->
      """
        |`n_nationkey` INT,
        |`n_name` CHAR(25),
        |`n_regionkey` INT,
        |`n_comment` VARCHAR(152)
        |""".stripMargin,
    "region" ->
      """
        |`r_regionkey` INT,
        |`r_name` CHAR(25),
        |`r_comment` VARCHAR(152)
        |""".stripMargin
  )

  // The partition column is consistent with the databricks/spark-sql-perf project.
  protected val tablePartitionColumns = Map(
    "part" -> Seq("`p_brand`"),
    "customer" -> Seq("`c_mktsegment`"),
    "orders" -> Seq("`o_orderdate`"),
    "lineitem" -> Seq("`l_shipdate`")
  )
}
