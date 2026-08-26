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

package org.apache.spark.sql.execution.python

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkArithmeticException
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Expression, Multiply, TranspiledPythonUDF, TranspiledUDFParameter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BaseEvalPython}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.functions.{array, col, max, rand, sum, transform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Tests how `UserDefinedPythonFunction.builder` turns the `_udf_param_N` placeholders into
 * references to the UDF's arguments (SPARK-58626). The builder owns this because it is the last
 * place that still sees the placeholders.
 *
 * Plus the operator shapes that decide whether an argument gets pre-evaluated: a Project and a
 * MergeRows host the column, an Aggregate is split so both halves can, and a lambda is not
 * lowered at all.
 * Asserted on the plan where a deterministic argument gives the same answer either way, and on a
 * raised error under MERGE, where an argument that mods by zero only blows up if the column is
 * really there.
 *
 * A nondeterministic argument is the case where the column is not an optimization: with no column
 * to read, a body reading the parameter twice would draw twice, so we leave the Python UDF.
 */
class TranspiledUDFParameterSuite extends QueryTest with SharedSparkSession {

  private val argA = AttributeReference("a", LongType)()
  private val argB = AttributeReference("b", LongType)()

  private def param(index: Int): Expression = UnresolvedAttribute(s"_udf_param_$index")

  /** A nondeterministic argument, LONG-typed to match what `udfWith` declares. A `def`, so two
   *  uses are two draws with their own seeds. */
  private def draw: Column = (rand() * 3).cast(LongType)

  /**
   * A transpiled UDF of `arity` numeric parameters carrying the given option body. `func` is null
   * because a call that transpiles never reaches Python -- so a fallback shows up as an NPE rather
   * than as a quietly passing test.
   */
  private def udfWith(option: Expression, arity: Int = 2): UserDefinedPythonFunction =
    UserDefinedPythonFunction(
      "udf",
      null,
      LongType,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true,
      List(Column(option)).asJava,
      List(List.fill(arity)("numeric").asJava).asJava)

  /** The argument indexes the single option the builder produced refers to, in tree order. */
  private def parameterIndexes(option: Expression, args: Expression*): Seq[Int] =
    udfWith(option).builder(args) match {
      case t: TranspiledPythonUDF =>
        t.transpiledOptions.head.collect { case p: TranspiledUDFParameter => p.index }
      case other => fail(s"Expected a TranspiledPythonUDF, got: $other")
    }

  private def transpileOn(f: => Unit): Unit = withSQLConf(
    SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
    SQLConf.ANSI_ENABLED.key -> "true")(f)

  test("refers to every parameter the body reads, once per read") {
    // `lambda a, b: a * a + b`: two refs to `a`, one to `b`. Binding both parameters to the same
    // argument changes nothing -- the option refers to parameters, and which argument each stands
    // for is settled later.
    val option = Add(Multiply(param(0), param(0)), param(1))
    Seq(argB, argA).foreach { second =>
      assert(parameterIndexes(option, argA, second) == Seq(0, 0, 1),
        s"Expected a, a, b referenced when b is bound to $second")
    }
  }

  test("drops a parameter the body never uses") {
    // `lambda a, b: a + a`: b never reaches the option, so nothing evaluates it.
    val option = Add(param(0), param(0))
    assert(parameterIndexes(option, argA, argB) == Seq(0, 0))
  }

  test("pre-evaluates an argument two references read") {
    transpileOn {
      // `id % 3` rather than a bare column, or it's cheap and gets left inline. Count definitions,
      // not reads: one `AS _udf_param_` read twice is the shape we want. The name carries the
      // column's ExprId, so match the prefix rather than a number.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).select(square(col("id") % 3).as("sq"))
      val plan = df.queryExecution.optimizedPlan.toString
      assert("AS _udf_param_".r.findAllIn(plan).length == 1,
        s"Expected exactly one pre-evaluated argument column:\n$plan")
      checkAnswer(df, Seq(Row(0L), Row(1L), Row(4L), Row(0L), Row(1L), Row(4L)))
    }
  }

  test("reads the grouping key's column for an argument equal to it") {
    transpileOn {
      // An Aggregate is split before conversion, so an argument in a result expression is already
      // the grouping key's attribute when we look -- cheap, and no column of its own.
      //
      // The grouping key has to be the bare argument, not the call -- with the call as the key both
      // it and the result would read the same column and bind fine, proving nothing. And `id % 3`
      // rather than a bare column, or it's left inline for being cheap and the assert passes for
      // the wrong reason.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).groupBy(col("id") % 3).agg(square(col("id") % 3).as("sq"))
      val plan = df.queryExecution.optimizedPlan.toString
      assert(!plan.contains("_udf_param_"), s"Expected no column of its own:\n$plan")
      checkAnswer(df, Seq(Row(0L, 0L), Row(1L, 1L), Row(2L, 4L)))
    }
  }

  test("splits an Aggregate so a nondeterministic argument gets a column") {
    transpileOn {
      // An Aggregate holds no Project of its own, so the rule splits it -- results into a Project
      // above -- and the column goes there. Without the split `rand()` has nowhere to go and the
      // call falls back to Python.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).groupBy(col("id") % 3).agg(square(draw).as("sq"))
      val plan = df.queryExecution.optimizedPlan
      assert("AS _udf_param_".r.findAllIn(plan.toString).length == 1,
        s"Expected one pre-evaluated column:\n$plan")
      assert(!plan.exists(_.isInstanceOf[BaseEvalPython]), s"Expected no fallback:\n$plan")
      assert(df.collect().length == 3)
    }
  }

  test("leaves an Aggregate alone when its only call is inside a subquery") {
    transpileOn {
      // The split fires on the operator's own expressions, not the tree-pattern bit: a
      // SubqueryExpression carries its inner plan's bits, so this Aggregate looked like it held a
      // call, split on every pass without ever clearing them, and died with a StackOverflowError.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val inner = spark.range(0, 3).select(square(col("id") % 3).as("m")).agg(max(col("m")))
      val df = spark.range(0, 6).groupBy(col("id") % 2).agg(sum(col("id") + inner.scalar()))
      assert(df.queryExecution.optimizedPlan.collect { case a: Aggregate => a }.nonEmpty)
      assert(df.collect().length == 2)
    }
  }

  test("keeps the Python UDF inside a higher-order function's lambda") {
    transpileOn {
      // Lambdas are out of scope for lowering: Spark applies a Python UDF over the whole array
      // there and this rule leaves that to it. The Python eval node is what says we did.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).select(
        transform(array(col("id")), _ => square(draw)).as("sq"))
      val plan = df.queryExecution.optimizedPlan
      assert(plan.exists(_.isInstanceOf[BaseEvalPython]), s"Expected a Python fallback:\n$plan")
      assert(!plan.toString.contains("_udf_param_"), s"Expected no column:\n$plan")
    }
  }

  test("gives two calls' nondeterministic arguments a column each") {
    transpileOn {
      // Two calls in one Project, each reading its own draw twice: a column apiece. Keying a
      // column on the parameter index alone made both bodies read one draw, which is not an answer
      // any evaluation order produces.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).select(square(draw).as("a"), square(draw).as("b"))
      val plan = df.queryExecution.optimizedPlan.toString
      assert("AS _udf_param_".r.findAllIn(plan).length == 2,
        s"Expected one pre-evaluated column per call:\n$plan")
    }
  }

  test("pre-evaluates a MERGE instruction condition's argument") {
    // MergeRows is a plain unary node, so it hosts the Project -- and it has to, or a transpiled
    // MERGE ends up lazier than an interpreted one. Measured: an interpreted UDF in `WHEN MATCHED
    // AND` raises under ANSI on a row with no match at all, since its inputs are computed below
    // MergeRows. First-match-wins instructions don't make it lazy.
    //
    // Also the only shape here whose `output` is a constructor field, so no restoring Project gets
    // added and the `__row_id` lookup has to still find its own past the one we appended.
    withSQLConf(
        "spark.sql.catalog.rowcat" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      transpileOn {
        val square = udfWith(Multiply(param(0), param(0)), arity = 1)
        val target = "rowcat.ns.t"
        // The DataFrame API rather than SQL because `udfWith` leaves `func` null on purpose, and
        // registering a UDF by name dereferences it. Applying the call as a Column skips that.
        // `%` and not `/` so the argument stays LONG and matches the option's declared return
        // type. Modulo by zero raises under ANSI too, just as REMAINDER_BY_ZERO.
        def runMerge(): Unit =
          sql("SELECT 1 AS id").as("s")
            .mergeInto(target, col("s.id") === col(s"$target.id"))
            .whenMatched(square(col(s"$target.a") % col(s"$target.b")) > 3)
            .delete()
            .merge()
        def create(rows: String): Unit = {
          sql(s"CREATE TABLE $target (id INT, a LONG, b LONG) USING foo")
          sql(s"INSERT INTO $target VALUES $rows")
        }
        withTable(target) {
          create("(1, 6, 4), (2, 6, 1)")
          runMerge()
          // id=1 matches, (6 % 4) squared is 4 > 3, so it goes. id=2 never matches.
          checkAnswer(sql(s"SELECT id FROM $target"), Seq(Row(2)))
        }
        withTable(target) {
          // id=3 has b=0 and no matching source row, so its condition never fires. It raises
          // anyway, because the column is computed for every row of the join -- which is also what
          // proves the column is there, since inline the instruction would never evaluate it.
          create("(1, 6, 4), (3, 6, 0)")
          val e = intercept[SparkArithmeticException](runMerge())
          assert(e.getCondition == "REMAINDER_BY_ZERO", s"Unexpected error: ${e.getCondition}")
        }
      }
    }
  }
}
