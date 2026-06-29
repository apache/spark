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

import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, DelegateExpression, ImplicitCastInput, Literal, MultiGetJsonObject, TypeCheckInput}
import org.apache.spark.sql.catalyst.analysis.resolver.ResolverRunner
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.execution.{LowerDelegateExpression, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

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
    // wrapping the arg in an ImplicitCastInput shim that the standard TypeCoercion rule handles.
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

  test("right() resolves cleanly under the single-pass resolver (input-type markers stripped)") {
    // The single-pass resolver builds DelegateFunctions through the same registry path (inserting
    // the input-type markers) but has no fixed-point batch to strip them; FunctionResolver must
    // remove them after coercion, else the Unevaluable markers would reach execution. We assert at
    // the analyzed-plan level (where the fix lives): single-pass does not yet support the
    // DeserializeToObject operator a typed `collect`/`checkAnswer` introduces, so the right()
    // execution results stay covered by the fixed-point tests above.
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      // 12345 (int) exercises the ImplicitCastInput path: it must be cast to string.
      val analyzed = spark.sql("SELECT right(12345, 2) AS r").queryExecution.analyzed
      // single-pass actually ran (there is no fallback when the conf is on) ...
      assert(analyzed.getTagValue(ResolverRunner.SINGLE_PASS_ANALYSIS_MARKER).contains(true),
        s"expected single-pass analysis to run:\n$analyzed")
      assert(analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[DelegateExpression]))),
        s"expected the right() delegate in the analyzed plan:\n$analyzed")
      // ... the DelegateFunction's input-type markers were stripped ...
      assert(!analyzed.exists(_.expressions.exists(_.exists(e =>
        e.isInstanceOf[ImplicitCastInput] || e.isInstanceOf[TypeCheckInput]))),
        s"input shims should be stripped under single-pass:\n$analyzed")
      // ... and the implicit cast the marker drove still applies (the marker was removed, not the Cast).
      assert(analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[Cast]))),
        s"expected the implicit Cast to survive marker removal:\n$analyzed")
    }
  }

  test("LowerDelegateExpression fully unwraps a directly-nested delegate-of-delegate") {
    // A delegate whose `definition` is itself a delegate (e.g. one delegate function composing
    // another). transformDown does not re-apply the rule to the replacement it produces, so the
    // rule must unwrap the chain itself -- otherwise the inner wrapper would reach the planner.
    val inner = DelegateExpression("inner", Seq(Literal(1)), Literal(1))
    val outer = DelegateExpression("outer", Seq(Literal(1)), inner)
    val lowered = LowerDelegateExpression(Project(Seq(Alias(outer, "c")()), OneRowRelation()))
    assert(!lowered.exists(_.expressions.exists(_.exists(_.isInstanceOf[DelegateExpression]))),
      s"nested delegates should be fully lowered:\n$lowered")
  }

  test("right() preserves the input column's collation in its output type") {
    // `Right.lower` builds the null/empty `If` branches as plain StringType literals (it cannot read
    // the not-yet-coerced arg's dataType); type coercion then re-unifies the branches to the
    // column's collation, since string literals carry the weakest collation strength.
    val df = spark.sql("SELECT right('Hello' COLLATE UTF8_LCASE, 3) AS r")
    assert(df.schema("r").dataType === StringType("UTF8_LCASE"),
      s"right() should preserve the UTF8_LCASE collation, got ${df.schema("r").dataType}")
    checkAnswer(df, Row("llo"))
  }

  test("right() preserves the input CHAR/VARCHAR type with preserveCharVarcharTypeInfo") {
    // `Right.lower` types its null/empty `If` branch literals with `str.dataType` (the resolved input
    // type the marker delegates), so the result keeps CHAR(N) instead of being widened to plain string
    // when type coercion unifies the branches.
    withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      checkAnswer(spark.sql("SELECT typeof(right(CAST('abc' AS CHAR(5)), 2)) AS t"), Row("char(5)"))
    }
  }

  test("right() rejects a wrong number of arguments with WRONG_NUM_ARGS") {
    // `DelegateFunction.build` validates arity before lowering, so too few/too many arguments fail
    // with the structured error rather than an IndexOutOfBounds or a silently ignored extra argument.
    Seq("SELECT right('abcd')", "SELECT right('abcd', 1, 99)").foreach { q =>
      val e = intercept[AnalysisException](spark.sql(q))
      assert(e.getCondition == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
        s"unexpected error condition for `$q`: ${e.getCondition}")
    }
  }
}
