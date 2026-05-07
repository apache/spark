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
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

/**
 * This test suite ensures all the TPC-DS queries can be successfully analyzed, optimized
 * and compiled without hitting the max iteration threshold.
 */
@ExtendedSQLTest
class TPCDSQuerySuite extends BenchmarkQueryTest with TPCDSBase {

  override protected def sparkConf: SparkConf =
    // Disable read-side char padding so that the generated code is less than 8000.
    super.sparkConf.set(SQLConf.READ_SIDE_CHAR_PADDING, false)

  tpcdsQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(name) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan)
    }
  }

  tpcdsQueriesV2_7_0.foreach { name =>
    val queryString = resourceToString(s"tpcds-v2.7.0/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(s"$name-v2.7") {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan)
    }
  }

  // List up the known queries having too large code in a generated function.
  // A JIRA file for `modified-q3` is as follows;
  // [SPARK-29128] Split predicate code in OR expressions
  val excludeListForMethodCodeSizeCheck = Set("modified-q3")

  modifiedTPCDSQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds-modifiedQueries/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val testName = s"modified-$name"
    test(testName) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan, !excludeListForMethodCodeSizeCheck.contains(testName))
    }
  }
}

@ExtendedSQLTest
class TPCDSQueryWithStatsSuite extends TPCDSQuerySuite {
  override def injectStats: Boolean = true
}

@ExtendedSQLTest
class TPCDSQueryANSISuite extends TPCDSQuerySuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
