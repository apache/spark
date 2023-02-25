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
 * see more at:
 * https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf
 *
 * |---------------|---------------|
 * |    TPCH       |  Spark  SQL   |
 * |---------------|---------------|
 * |  Identifier   |     BIGINT    |
 * |---------------|---------------|
 * |    Integer    |      INT      |
 * |---------------|---------------|
 * | Decimal(d, f) | Decimal(d, f) |
 * |---------------|---------------|
 * |    Char(N)    |     STRING    |
 * |---------------|---------------|
 * |  Varchar(N)   |     STRING    |
 * |---------------|---------------|
 * |     Date      |     Date      |
 * |---------------|---------------|
 */

trait TPCHSchema extends TPCSchema {

  override protected val tableColumns: Map[String, String] = Map(
    "part" ->
      """
        |`p_partkey` BIGINT,
        |`p_name` STRING,
        |`p_mfgr` STRING,
        |`p_brand` STRING,
        |`p_type` STRING,
        |`p_size` INT,
        |`p_container` STRING,
        |`p_retailprice` DECIMAL(10,0),
        |`p_comment` STRING
      """.stripMargin,

    "supplier" ->
      """
        |`s_suppkey` BIGINT,
        |`s_name` STRING,
        |`s_address` STRING,
        |`s_nationkey` BIGINT,
        |`s_phone` STRING,
        |`s_acctbal` DECIMAL(10,0),
        |`s_comment` STRING
      """.stripMargin,
    "partsupp" ->
      """
        |`ps_partkey` BIGINT,
        |`ps_suppkey` BIGINT,
        |`ps_availqty` INT,
        |`ps_supplycost` DECIMAL(10,0),
        |`ps_comment` STRING
      """.stripMargin,
    "customer" ->
      """
        |`c_custkey` BIGINT,
        |`c_name` STRING,
        |`c_address` STRING,
        |`c_nationkey` BIGINT,
        |`c_phone` STRING,
        |`c_acctbal` DECIMAL(10,0),
        |`c_mktsegment` STRING,
        |`c_comment` STRING
      """.stripMargin,
    "orders" ->
      """
        |`o_orderkey` BIGINT,
        |`o_custkey` BIGINT,
        |`o_orderstatus` STRING,
        |`o_totalprice` DECIMAL(10,0),
        |`o_orderdate` DATE,
        |`o_orderpriority` STRING,
        |`o_clerk` STRING,
        |`o_shippriority` INT,
        |`o_comment` STRING
      """.stripMargin,
    "lineitem" ->
      """
        |`l_orderkey` BIGINT,
        |`l_partkey` BIGINT,
        |`l_suppkey` BIGINT,
        |`l_linenumber` INT,
        |`l_quantity` DECIMAL(10,0),
        |`l_extendedprice` DECIMAL(10,0),
        |`l_discount` DECIMAL(10,0),
        |`l_tax` DECIMAL(10,0),
        |`l_returnflag` STRING,
        |`l_linestatus` STRING,
        |`l_shipdate` DATE,
        |`l_commitdate` DATE,
        | `l_receiptdate` DATE,
        |`l_shipinstruct` STRING,
        |`l_shipmode` STRING,
        |`l_comment` STRING
      """.stripMargin,
    "nation" ->
      """
        |`n_nationkey` BIGINT,
        |`n_name` STRING,
        |`n_regionkey` BIGINT,
        |`n_comment` STRING
      """.stripMargin,
    "region" ->
      """
        |`r_regionkey` BIGINT,
        | `r_name` STRING,
        | `r_comment` STRING
      """.stripMargin
  )

  override protected val tablePartitionColumns: Map[String, Seq[String]] = Map(
    "part" -> Seq("`p_brand`"),
    "customer" -> Seq("`c_mktsegment`"),
    "orders" -> Seq("`o_orderdate`"),
    "lineitem" -> Seq("`l_shipdate`")
  )

}
