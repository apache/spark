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

import org.apache.spark.sql.catalyst.expressions.{DelegateExpression, ImplicitCastInput, MultiGetJsonObject, TypeCheckInput}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end proof for the delegate-expression redesign: `right()` is built as a
 * [[DelegateExpression]] -- a logical-phase wrapper that stays readable in the optimized plan and is
 * lowered to its real definition (by `LowerDelegateExpression`) before physical execution, so the
 * planner, pushdown, columnar rules and codegen see the actual executed expression.
 */
class DelegateExpressionQuerySuite extends QueryTest with SharedSparkSession {

  test("right() is a DelegateExpression in the optimized plan, lowered before execution") {
    val df = spark.range(0, 3).selectExpr("right(concat('row', cast(id as string)), 1) as r")
    checkAnswer(df, Seq(Row("0"), Row("1"), Row("2")))

    // Readable in the optimized logical plan ...
    assert(df.queryExecution.optimizedPlan.exists(
      _.expressions.exists(_.exists(_.isInstanceOf[DelegateExpression]))),
      s"expected a DelegateExpression in the optimized plan:\n${df.queryExecution.optimizedPlan}")
    // ... but lowered away before physical execution, so engines see the real expression.
    val executed = df.queryExecution.executedPlan
    assert(!executed.exists(_.expressions.exists(_.exists(_.isInstanceOf[DelegateExpression]))),
      s"DelegateExpression should be lowered before execution:\n$executed")
    assert(executed.exists(_.isInstanceOf[WholeStageCodegenExec]),
      s"expected whole-stage codegen in the executed plan:\n$executed")
  }

  test("right() implicit-casts a non-string arg via the standard coercion rule (no extra step)") {
    // The old plain-form `right` was ImplicitCastInputTypes; the delegate form preserves this by
    // wrapping the arg in a ImplicitCastInput shim that the standard TypeCoercion rule handles.
    checkAnswer(spark.sql("SELECT right(12345, 2)"), Row("45"))
  }

  test("internal input shims are stripped at the end of analysis") {
    val df = spark.range(0, 3).selectExpr("right(concat('row', cast(id as string)), 2) as r")
    val analyzed = df.queryExecution.analyzed
    // The high-level delegate remains in the plan ...
    assert(analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[DelegateExpression]))))
    // ... but the internal coercion shims are gone (they were inserted, then stripped).
    assert(!analyzed.exists(_.expressions.exists(_.exists(e =>
      e.isInstanceOf[ImplicitCastInput] || e.isInstanceOf[TypeCheckInput]))),
      s"input shims should be stripped after analysis:\n$analyzed")
  }

  test("right() produces identical results with whole-stage codegen on and off") {
    Seq("true", "false").foreach { flag =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> flag) {
        checkAnswer(
          spark.range(0, 3).selectExpr("right(concat('row', cast(id as string)), 2) as r"),
          Seq(Row("w0"), Row("w1"), Row("w2")))
      }
    }
  }

  test("optimizer-inserted MultiGetJsonObject is a delegate in the optimized plan, lowered " +
    "before execution") {
    import testImplicits._
    withSQLConf(SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> "true") {
      val df = Seq("""{"a":1,"b":2}""").toDF("j")
        .selectExpr("get_json_object(j, '$.a') as a", "get_json_object(j, '$.b') as b")
      checkAnswer(df, Row("1", "2"))

      // The two sibling get_json_object calls were shared into one delegate, readable in the
      // optimized plan ...
      assert(df.queryExecution.optimizedPlan.exists(
        _.expressions.exists(_.exists(MultiGetJsonObject.isInstance))),
        s"expected a multi_get_json_object delegate in the optimized plan")
      // ... and lowered to its Invoke definition before execution.
      val executed = df.queryExecution.executedPlan
      assert(!executed.exists(_.expressions.exists(_.exists(MultiGetJsonObject.isInstance))),
        s"delegate should be lowered before execution:\n$executed")
      assert(executed.exists(_.isInstanceOf[WholeStageCodegenExec]))
    }
  }
}
