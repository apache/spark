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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

class AnalyzerRuleStrategySuite extends SparkFunSuite with LocalSparkContext {

  test("Analyzer raise error when exceed maxIterations") {
    var spark: SparkSession = null
    // RuleExecutor only throw exception or log warning when the rule is supposed to run
    // more than once.
    val maxIterations = 2

    // The configuration ANALYZER_MAX_ITERATIONS will build immutable FixPoint in Analyzer,
    // thus we need to set it before SparkSession was created.
    sc = new SparkContext(
      new SparkConf().setAppName("test").setMaster("local")
        .set(SQLConf.ANALYZER_MAX_ITERATIONS.key, maxIterations.toString))

    try {
      spark = new TestSparkSession(sc)
      SparkSession.setActiveSession(spark)
      val message = intercept[TreeNodeException[LogicalPlan]] {
        spark.sql(s"CREATE TABLE tbl(i INT, j STRING) USING parquet")
      }.getMessage
      assert(message.startsWith(s"Max iterations ($maxIterations) reached for batch Resolution"))
    } finally {
      if (spark != null) {
        spark.stop()
      }
      if (sc != null) {
        sc.stop()
      }
    }
  }
}
