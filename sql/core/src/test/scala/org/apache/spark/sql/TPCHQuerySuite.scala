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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

/**
 * This test suite ensures all the TPC-H queries can be successfully analyzed, optimized
 * and compiled without hitting the max iteration threshold.
 */
class TPCHQuerySuite extends QueryTest with SharedSQLContext with BeforeAndAfterAll {

  // When Utils.isTesting is true, the RuleExecutor will issue an exception when hitting
  // the max iteration of analyzer/optimizer batches.
  assert(Utils.isTesting, "spark.testing is not set to true")

  /**
   * Drop all the tables
   */
  protected override def afterAll(): Unit = {
    try {
      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())
      spark.sessionState.catalog.reset()
    } finally {
      super.afterAll()
    }
  }

  override def beforeAll() {
    super.beforeAll()
    RuleExecutor.resetTime()

    sql(
      """
        |CREATE TABLE `orders` (
        |`o_orderkey` BIGINT, `o_custkey` BIGINT, `o_orderstatus` STRING,
        |`o_totalprice` DECIMAL(10,0), `o_orderdate` DATE, `o_orderpriority` STRING,
        |`o_clerk` STRING, `o_shippriority` INT, `o_comment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `nation` (
        |`n_nationkey` BIGINT, `n_name` STRING, `n_regionkey` BIGINT, `n_comment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `region` (
        |`r_regionkey` BIGINT, `r_name` STRING, `r_comment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `part` (`p_partkey` BIGINT, `p_name` STRING, `p_mfgr` STRING,
        |`p_brand` STRING, `p_type` STRING, `p_size` INT, `p_container` STRING,
        |`p_retailprice` DECIMAL(10,0), `p_comment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `partsupp` (`ps_partkey` BIGINT, `ps_suppkey` BIGINT,
        |`ps_availqty` INT, `ps_supplycost` DECIMAL(10,0), `ps_comment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `customer` (`c_custkey` BIGINT, `c_name` STRING, `c_address` STRING,
        |`c_nationkey` STRING, `c_phone` STRING, `c_acctbal` DECIMAL(10,0),
        |`c_mktsegment` STRING, `c_comment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `supplier` (`s_suppkey` BIGINT, `s_name` STRING, `s_address` STRING,
        |`s_nationkey` BIGINT, `s_phone` STRING, `s_acctbal` DECIMAL(10,0), `s_comment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `lineitem` (`l_orderkey` BIGINT, `l_partkey` BIGINT, `l_suppkey` BIGINT,
        |`l_linenumber` INT, `l_quantity` DECIMAL(10,0), `l_extendedprice` DECIMAL(10,0),
        |`l_discount` DECIMAL(10,0), `l_tax` DECIMAL(10,0), `l_returnflag` STRING,
        |`l_linestatus` STRING, `l_shipdate` DATE, `l_commitdate` DATE, `l_receiptdate` DATE,
        |`l_shipinstruct` STRING, `l_shipmode` STRING, `l_comment` STRING)
        |USING parquet
      """.stripMargin)
  }

  val tpchQueries = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22")

  private def checkGeneratedCode(plan: SparkPlan): Unit = {
    val codegenSubtrees = new collection.mutable.HashSet[WholeStageCodegenExec]()
    plan foreach {
      case s: WholeStageCodegenExec =>
        codegenSubtrees += s
      case s => s
    }
    codegenSubtrees.toSeq.foreach { subtree =>
      val code = subtree.doCodeGen()._2
      try {
        // Just check the generated code can be properly compiled
        CodeGenerator.compile(code)
      } catch {
        case e: Exception =>
          val msg =
            s"""
               |failed to compile:
               |Subtree:
               |$subtree
               |Generated code:
               |${CodeFormatter.format(code)}
             """.stripMargin
          throw new Exception(msg, e)
      }
    }
  }

  tpchQueries.foreach { name =>
    val queryString = resourceToString(s"tpch/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(name) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan)
    }
  }
}
