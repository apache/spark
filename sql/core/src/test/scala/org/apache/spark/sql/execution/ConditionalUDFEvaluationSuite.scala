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

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericInternalRow}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType}

/**
 * Drives `ConditionalUDFEvaluation` over real task partitions with a plain (non-Python)
 * `evaluate`, to check row order and the row-count checks without a Python worker.
 */
class ConditionalUDFEvaluationSuite extends SharedSparkSession {

  private val a = AttributeReference("a", IntegerType, nullable = false)()
  private val keep = AttributeReference("keep", BooleanType, nullable = false)()
  private val childOutput: Seq[Attribute] = Seq(a, keep)
  private val passThroughOutput: Seq[Attribute] = Seq(a)
  private val resultOutput: Seq[Attribute] =
    Seq(AttributeReference("r", IntegerType, nullable = true)())

  // `evaluate` returns, for each row it is given, its pass-through column `a` and a result of
  // `a * 10`. A test can wrap it to return too many or too few rows. It lives in the companion
  // object so the task closure does not capture the suite.
  import ConditionalUDFEvaluationSuite.evalToTenX

  private def run(
      rows: Seq[(Int, Boolean)],
      numSlices: Int,
      lockFree: Boolean = false,
      resultIsConstant: Boolean = false)(
      evaluate: Iterator[InternalRow] => Iterator[InternalRow]): Seq[(Int, Option[Int])] = {
    val co = childOutput
    val pto = passThroughOutput
    val ro = resultOutput
    val cond = keep
    spark.sparkContext.parallelize(rows, numSlices).mapPartitions { it =>
      val input = it.map { case (v, k) => new GenericInternalRow(Array[Any](v, k)): InternalRow }
      ConditionalUDFEvaluation(
        input, cond, co, pto, ro, TaskContext.get(), lockFree, resultIsConstant)(evaluate)
        .map(out => (out.getInt(0), if (out.isNullAt(1)) None else Some(out.getInt(1))))
    }.collect().toSeq
  }

  private def expected(rows: Seq[(Int, Boolean)]): Seq[(Int, Option[Int])] =
    rows.map { case (v, k) => (v, if (k) Some(v * 10) else None) }

  test("keeps input order for kept/skipped-row patterns, with either queue") {
    val patterns = Map(
      "all kept" -> Seq((1, true), (2, true), (3, true)),
      "none kept" -> Seq((1, false), (2, false), (3, false)),
      "alternating" -> Seq((1, false), (2, true), (3, false), (4, true), (5, false)),
      "first only" -> Seq((1, true), (2, false), (3, false)),
      "last only" -> Seq((1, false), (2, false), (3, true)),
      "runs" -> Seq((1, false), (2, false), (3, true), (4, true), (5, false), (6, true)))
    for ((name, rows) <- patterns; slices <- Seq(1, 2, 3); lockFree <- Seq(false, true)) {
      withClue(s"$name, $slices slices, lockFree=$lockFree: ") {
        assert(run(rows, slices, lockFree)(evalToTenX) == expected(rows))
      }
    }
  }

  test("empty input produces no rows") {
    assert(run(Seq.empty, 1)(evalToTenX).isEmpty)
  }

  test("a UDF that returns more rows than it was given fails") {
    val rows = Seq((1, false), (2, true), (3, false))
    val e = intercept[SparkException] {
      run(rows, 1)(it => evalToTenX(it).flatMap(r => Seq(r, r)))
    }
    assert(e.getMessage.contains("more rows than it was given"))
  }

  test("a UDF that returns fewer rows than it was given fails") {
    val rows = Seq((1, false), (2, true), (3, true), (4, false))
    val e = intercept[SparkException] {
      run(rows, 1)(it => evalToTenX(it).take(1))
    }
    assert(e.getMessage.contains("fewer rows than it was given"))
  }

  test("fewer rows is detected even when all input was consumed") {
    // The evaluator consumes every row it is given but drops the last result, so the input is
    // exhausted (no leftover to notice) and the shortfall is only visible from the pending gaps.
    val rows = Seq((1, false), (2, true), (3, true), (4, false))
    val e = intercept[SparkException] {
      run(rows, 1)(it => evalToTenX(it).toVector.dropRight(1).iterator)
    }
    assert(e.getMessage.contains("fewer rows than it was given"))
  }

  test("the evaluator is not invoked, and no row is sent to it, for the leading skipped rows") {
    val evaluatedRows = spark.sparkContext.longAccumulator("evaluated rows")
    val evaluatorCalls = spark.sparkContext.longAccumulator("evaluator calls")
    def counting(it: Iterator[InternalRow]): Iterator[InternalRow] = {
      evaluatorCalls.add(1L)
      ConditionalUDFEvaluationSuite.evalToTenX(it).map { r => evaluatedRows.add(1L); r }
    }
    // A partition whose rows all skip the UDF: the evaluator is never created or called.
    assert(run(Seq((1, false), (2, false), (3, false)), 1)(counting) ==
      Seq((1, None), (2, None), (3, None)))
    assert(evaluatorCalls.value == 0L)
    assert(evaluatedRows.value == 0L)
    // With one kept row, only that row is sent to the evaluator.
    evaluatedRows.reset()
    evaluatorCalls.reset()
    assert(run(Seq((1, false), (2, false), (3, true), (4, false)), 1)(counting) ==
      Seq((1, None), (2, None), (3, Some(30)), (4, None)))
    assert(evaluatorCalls.value == 1L)
    assert(evaluatedRows.value == 1L)
  }

  test("reuses the first evaluated result for every kept row when resultIsConstant") {
    val sent = spark.sparkContext.longAccumulator("rows sent to evaluate")
    val drained = spark.sparkContext.longAccumulator("times the evaluator was read to the end")
    def countingEval(it: Iterator[InternalRow]): Iterator[InternalRow] = {
      val base = ConditionalUDFEvaluationSuite.evalToTenX(it)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val h = base.hasNext
          if (!h) drained.add(1L)
          h
        }
        override def next(): InternalRow = { sent.add(1L); base.next() }
      }
    }
    // evalToTenX returns v * 10; with reuse, every kept row gets the FIRST kept row's result
    // (2 * 10 = 20), not its own, which pins that one result is reused.
    val rows = Seq((1, false), (2, true), (3, false), (4, true), (5, true))
    val out = run(rows, 1, resultIsConstant = true)(countingEval)
    assert(out == Seq((1, None), (2, Some(20)), (3, None), (4, Some(20)), (5, Some(20))))
    // Only the first kept row is evaluated, and the evaluator is read to the end (so a Python
    // worker is released and accumulator updates are received).
    assert(sent.value == 1L)
    assert(drained.value == 1L)
    // A partition with no kept row evaluates nothing.
    sent.reset()
    assert(run(Seq((1, false), (2, false)), 1, resultIsConstant = true)(countingEval) ==
      Seq((1, None), (2, None)))
    assert(sent.value == 0L)
  }

  test("the constant path fails if the evaluator returns no row") {
    val e = intercept[SparkException] {
      run(Seq((1, false), (2, true)), 1, resultIsConstant = true)(_ => Iterator.empty)
    }
    assert(e.getMessage.contains("no row"))
  }

  test("the constant path fails if the evaluator returns more rows than one") {
    val e = intercept[SparkException] {
      run(Seq((1, false), (2, true), (3, true)), 1, resultIsConstant = true)(
        it => evalToTenX(it).flatMap(r => Seq(r, r)))
    }
    assert(e.getMessage.contains("more rows than it was given"))
  }
}

object ConditionalUDFEvaluationSuite {
  // Kept out of the suite so the serialized task closure does not capture the test instance.
  def evalToTenX(rows: Iterator[InternalRow]): Iterator[InternalRow] =
    rows.map(row => new GenericInternalRow(Array[Any](row.getInt(0), row.getInt(0) * 10)))
}
