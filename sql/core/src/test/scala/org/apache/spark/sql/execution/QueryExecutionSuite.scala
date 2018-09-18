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
package org.apache.spark.sql.execution

import scala.io.Source

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.test.SharedSQLContext

class QueryExecutionSuite extends SharedSQLContext {
  def checkDumpedPlans(path: String, expected: Int): Unit = {
    assert(Source.fromFile(path).getLines.toList
      .takeWhile(_ != "== Whole Stage Codegen ==") == List(
      "== Parsed Logical Plan ==",
      s"Range (0, $expected, step=1, splits=Some(2))",
      "",
      "== Analyzed Logical Plan ==",
      "id: bigint",
      s"Range (0, $expected, step=1, splits=Some(2))",
      "",
      "== Optimized Logical Plan ==",
      s"Range (0, $expected, step=1, splits=Some(2))",
      "",
      "== Physical Plan ==",
      s"*(1) Range (0, $expected, step=1, splits=2)",
      ""))
  }
  test("dumping query execution info to a file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/plans.txt"
      val df = spark.range(0, 10)
      df.queryExecution.debug.toFile(path)

      checkDumpedPlans(path, expected = 10)
    }
  }

  test("dumping query execution info to an existing file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/plans.txt"
      val df = spark.range(0, 10)
      df.queryExecution.debug.toFile(path)

      val df2 = spark.range(0, 1)
      df2.queryExecution.debug.toFile(path)
      checkDumpedPlans(path, expected = 1)
    }
  }

  test("dumping query execution info to non-existing folder") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/newfolder/plans.txt"
      val df = spark.range(0, 100)
      df.queryExecution.debug.toFile(path)
      checkDumpedPlans(path, expected = 100)
    }
  }

  test("dumping query execution info by invalid path") {
    val path = "1234567890://plans.txt"
    val exception = intercept[IllegalArgumentException] {
      spark.range(0, 100).queryExecution.debug.toFile(path)
    }

    assert(exception.getMessage.contains("Illegal character in scheme name"))
  }

  test("toString() exception/error handling") {
    spark.experimental.extraStrategies = Seq(
        new SparkStrategy {
          override def apply(plan: LogicalPlan): Seq[SparkPlan] = Nil
        })

    def qe: QueryExecution = new QueryExecution(spark, OneRowRelation())

    // Nothing!
    assert(qe.toString.contains("OneRowRelation"))

    // Throw an AnalysisException - this should be captured.
    spark.experimental.extraStrategies = Seq(
      new SparkStrategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] =
          throw new AnalysisException("exception")
      })
    assert(qe.toString.contains("org.apache.spark.sql.AnalysisException"))

    // Throw an Error - this should not be captured.
    spark.experimental.extraStrategies = Seq(
      new SparkStrategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] =
          throw new Error("error")
      })
    val error = intercept[Error](qe.toString)
    assert(error.getMessage.contains("error"))
  }
}
