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

import java.nio.file.{Files, Paths}

import scala.io.Source
import scala.reflect.io.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.test.SharedSQLContext

class QueryExecutionSuite extends SharedSQLContext {
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

  test("debug to file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + s"/plans.txt"
      val df = spark.range(0, 10)
      df.queryExecution.debug.toFile(path)

      assert(Source.fromFile(path).getLines.toList == List(
        "== Parsed Logical Plan ==",
        "Range (0, 10, step=1, splits=Some(2))",
        "== Analyzed Logical Plan ==",
        "Range (0, 10, step=1, splits=Some(2))",
        "== Optimized Logical Plan ==",
        "Range (0, 10, step=1, splits=Some(2))",
        "== Physical Plan ==", "WholeStageCodegen",
        "+- Range (0, 10, step=1, splits=2)"))
    }
  }
}
