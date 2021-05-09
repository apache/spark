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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf

/**
 * This test suite ensures all the TPC-DS queries can be successfully analyzed, optimized
 * and compiled without hitting the max iteration threshold.
 */
class TPCDSQuerySuite extends BenchmarkQueryTest with TPCDSBase {

  val sqlConfgs: Seq[(String, String)] = Nil

  tpcdsQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(name) {
      withSQLConf(sqlConfgs: _*) {
        // check the plans can be properly generated
        val plan = sql(queryString).queryExecution.executedPlan
        checkGeneratedCode(plan)
      }
    }
  }

  tpcdsQueriesV2_7_0.foreach { name =>
    val queryString = resourceToString(s"tpcds-v2.7.0/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(s"$name-v2.7") {
      withSQLConf(sqlConfgs: _*) {
        // check the plans can be properly generated
        val plan = sql(queryString).queryExecution.executedPlan
        checkGeneratedCode(plan)
      }
    }
  }

  // List up the known queries having too large code in a generated function.
  // A JIRA file for `modified-q3` is as follows;
  // [SPARK-29128] Split predicate code in OR expressions
  val blackListForMethodCodeSizeCheck = Set("modified-q3")

  modifiedTPCDSQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds-modifiedQueries/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val testName = s"modified-$name"
    test(testName) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan, !blackListForMethodCodeSizeCheck.contains(testName))
    }
  }
}

class TPCDSQueryWithStatsSuite extends TPCDSQuerySuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    for (tableName <- tableNames) {
      // To simulate plan generation on actual TPCDS data, injects data stats here
      spark.sessionState.catalog.alterTableStats(
        TableIdentifier(tableName), Some(TPCDSTableStats.sf100TableStats(tableName)))
    }
  }

  // Sets configurations for enabling the optimization rules that
  // exploit data statistics.
  override val sqlConfgs = Seq(
    SQLConf.CBO_ENABLED.key -> "true",
    SQLConf.PLAN_STATS_ENABLED.key -> "true",
    SQLConf.JOIN_REORDER_ENABLED.key -> "true"
  )
}

class TPCDSQueryANSISuite extends TPCDSQuerySuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
