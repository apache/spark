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

import org.apache.spark.sql.catalyst.expressions.BinaryComparison
import org.apache.spark.sql.catalyst.optimizer.BooleanSimplification
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class IntegralRangeQuerySuite extends QueryTest with SharedSparkSession {
  test("SPARK-31760: range simplification preserves filter and projection results") {
    withTempView("ranges") {
      val rows = Seq(null, Int.MinValue, -1, 0, 1, 2, 3, 4, 5, 6, 6, Int.MaxValue).map(Row(_))
      spark.createDataFrame(spark.sparkContext.parallelize(rows, 2),
        StructType(Seq(StructField("a", IntegerType, nullable = true))))
        .createOrReplaceTempView("ranges")

      for {
        aqe <- Seq(false, true)
        condition <- Seq("a > 5 AND a > 0", "a > 1 OR a > 2",
          "a > 1 OR (a > 2 AND a < 4)", "5 >= a AND a < 5",
          "a > (2 + 2) AND a > (1 - 1)", "a > (1 + 1) OR a > (2 * 2)")
        sqlText <- Seq(s"SELECT a FROM ranges WHERE $condition",
          s"SELECT $condition AS result FROM ranges")
      } {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          val expected = withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
              BooleanSimplification.ruleName) {
            spark.sql(sqlText).collect().toSeq
          }
          val query = spark.sql(sqlText)
          val comparisons = query.queryExecution.optimizedPlan.flatMap { plan =>
            plan.expressions.flatMap(_.collect { case comparison: BinaryComparison => comparison })
          }
          assert(comparisons.size == 1, query.queryExecution.optimizedPlan.toString)
          checkAnswer(query, expected)
        }
      }
    }
  }
}
