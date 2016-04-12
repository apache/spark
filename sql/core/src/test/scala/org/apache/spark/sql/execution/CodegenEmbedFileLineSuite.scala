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

package org.apache.spark.sql.core.expressions.codegen

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.debug.codegenString
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.ResetSystemProperties



class CodegenEmbedFileLineSuite extends PlanTest with SharedSQLContext
  with ResetSystemProperties {
  import testImplicits._

  test("filter String") {
    val df = sparkContext.parallelize(1 to 1, 1).map(i => (i, -i)).toDF("k", "v")
      .filter("k > 0")
    validate(df, Array(" > 0\\) @ filter at CodegenEmbedFileLineSuite.scala:35"))
  }

  test("select Column") {
    val df = sparkContext.parallelize(1 to 1, 1).toDF
      .select($"value" + 1)
    validate(df, Array(" \\+ 1\\) @ select at CodegenEmbedFileLineSuite.scala:41"))
  }

  test("selectExpr String") {
    val df = sparkContext.parallelize(1 to 1, 1).toDF
      .selectExpr("value + 2")
    validate(df, Array(" \\+ 2\\) @ selectExpr at CodegenEmbedFileLineSuite.scala:47"))
  }

  test("filter Strings (two filters are combined into one plan") {
    val df = sparkContext.parallelize(1 to 1, 1).map(i => (i, -i)).toDF("k", "v")
      .filter("k > 0")
      .filter("v > 1")
    validate(df,
      Array(" > 0\\) @ filter at CodegenEmbedFileLineSuite.scala:53",
            " > 1\\) @ filter at CodegenEmbedFileLineSuite.scala:54"),
      Array(" > 1\\) @ filter at CodegenEmbedFileLineSuite.scala:53",
            " > 0\\) @ filter at CodegenEmbedFileLineSuite.scala:54"))
  }

  test("selectExpr Strings") {
    val df = sparkContext.parallelize(1 to 1, 1).map(i => (i, -i)).toDF("k", "v")
      .selectExpr("k + 2", "v - 2")
    validate(df,
      Array(" \\+ 2\\) @ selectExpr at CodegenEmbedFileLineSuite.scala:64",
            " - 2\\) @ selectExpr at CodegenEmbedFileLineSuite.scala:64"))
  }

  test("select and selectExpr") {
    val df = sparkContext.parallelize(1 to 1, 1).toDF
    val df1 = df.select($"value" + 1)
    val df2 = df.selectExpr("value + 2")
    validate(df1,
      Array(" \\+ 1\\) @ select at CodegenEmbedFileLineSuite.scala:72"),
      Array(" \\+ 2\\) @ select at CodegenEmbedFileLineSuite.scala:73"))
    validate(df2,
      Array(" \\+ 2\\) @ selectExpr at CodegenEmbedFileLineSuite.scala:73"),
      Array(" \\+ 1\\) @ selectExpr at CodegenEmbedFileLineSuite.scala:72"))
  }

  test("filter and select") {
    val df = sparkContext.parallelize(1 to 1, 1).toDF
    val df1 = df.filter("value > 0")
    val df2 = df1.select($"value" * 2)
    validate(df2,
      Array(" > 0\\) @ filter at CodegenEmbedFileLineSuite.scala:84",
            " \\* 2\\) @ select at CodegenEmbedFileLineSuite.scala:85"))
  }

  test("no transformation") {
    val df = sparkContext.parallelize(1 to 1, 1).toDF
    validate(df,
      Array.empty,
      Array("CodegenEmbedFileLineSuite.scala"))
  }


  def validate(df: DataFrame,
     expected: Array[String] = Array.empty, unexpected: Array[String] = Array.empty): Unit = {
    val logicalPlan = df.logicalPlan
    // As LogicalPlan.resolveOperators does,
    // this routine also updates CurrentOrigin by logicalPlan.origin
    val cg = CurrentOrigin.withOrigin(logicalPlan.origin) {
      val queryExecution = sqlContext.executePlan(logicalPlan)
      codegenString(queryExecution.executedPlan)
    }

    if (cg.contains("Found 0 WholeStageCodegen subtrees")) {
      return
    }

    expected.foreach { string =>
      if (!string.r.findFirstIn(cg).isDefined) {
        fail(
          s"""
          |=== FAIL: generated code must include: "$string" ===
          |$cg
         """.stripMargin
        )
      }
    }
    unexpected.foreach { string =>
      if (string.r.findFirstIn(cg).isDefined) {
        fail(
          s"""
          |=== FAIL: generated code must not include: "$string" ===
          |$cg
         """.stripMargin
        )
      }
    }
  }

  override def beforeEach() {
    super.beforeEach()
    System.setProperty("spark.callstack.testClass", this.getClass.getName)
  }
}
