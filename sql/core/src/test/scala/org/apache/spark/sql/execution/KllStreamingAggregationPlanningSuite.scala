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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.MapObjects
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class KllStreamingAggregationPlanningSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val legacySharingKey = "spark.sql.streaming.aggregation.legacyKllSharing.enabled"

  private def checkPlan(
      logical: Aggregate,
      legacySharing: Boolean,
      retained: Seq[Int],
      results: Seq[Int]): Unit = {
    val original = logical.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    withSQLConf(
      legacySharingKey -> legacySharing.toString,
      SQLConf.STREAMING_USE_STREAMLINE_AGGREGATOR.key -> "false") {
      val planned = spark.sessionState.planner.StatefulAggregationStrategy(logical)
      assert(planned.size == 1)
      val aggregates = planned.head.collect { case a: BaseAggregateExec => a }
      assert(aggregates.nonEmpty)
      // Every stage must use the same buffer order, including aggregates between KLL calls.
      aggregates.foreach { aggregate =>
        assert(aggregate.aggregateExpressions.map(_.resultId) == retained.map(original(_).resultId))
      }
      val finalAggregate = aggregates.head
      assert(finalAggregate.output.map(_.exprId) == logical.output.map(_.exprId))
      assert(finalAggregate.resultExpressions.map(_.references.iterator.map(_.exprId).toSet) ==
        results.map(index => Set(original(index).resultId)))
    }
  }

  for (valueType <- Seq("bigint", "float", "double");
      merge <- Seq(false, true);
      k <- Seq("", ", 8")) {
    val function = if (merge) s"kll_merge_agg_$valueType" else s"kll_sketch_agg_$valueType"
    test(s"$function with k='$k' shares buffers only for legacy streaming queries") {
      val input = if (merge) MemoryStream[Array[Byte]](spark).toDF()
        else MemoryStream[Long](spark).toDF()
      val argument = if (merge) "value" else s"CAST(value AS $valueType)"
      val logical = input.agg(
        expr(s"$function($argument$k)").as("a"),
        expr(s"$function($argument$k)").as("b"))
        .queryExecution.analyzed.asInstanceOf[Aggregate]
      checkPlan(logical, legacySharing = false, retained = Seq(0, 1), results = Seq(0, 1))
      checkPlan(logical, legacySharing = true, retained = Seq(0), results = Seq(0, 0))
    }
  }

  test("legacy KLL sharing preserves interleaved buffers and distinct k and filters") {
    val input = MemoryStream[Long](spark).toDF()
    val filtered = "kll_sketch_agg_bigint(value, 8) FILTER (WHERE value > 0)"
    val logical = input.agg(
      expr(filtered).as("a"),
      expr("count(*)").as("n"),
      expr(filtered).as("b"),
      expr("kll_sketch_agg_bigint(value, 16) FILTER (WHERE value > 0)").as("c"),
      expr("count(*)").as("m"),
      expr("kll_sketch_agg_bigint(value, 8) FILTER (WHERE value < 0)").as("d"))
      .queryExecution.analyzed.asInstanceOf[Aggregate]
    checkPlan(logical, legacySharing = false,
      retained = Seq(0, 1, 2, 3, 5), results = Seq(0, 1, 2, 3, 1, 5))
    checkPlan(logical, legacySharing = true,
      retained = Seq(0, 1, 3, 5), results = Seq(0, 1, 0, 3, 1, 5))
  }

  test("legacy KLL sharing preserves repeated references to one aggregate result") {
    val input = MemoryStream[Long](spark).toDF().queryExecution.analyzed
    val aggregate = KllSketchAggBigint(input.output.head).toAggregateExpression()
    val logical = Aggregate(Nil, Seq(Alias(aggregate, "a")(), Alias(aggregate, "b")()), input)
    checkPlan(logical, legacySharing = true, retained = Seq(0), results = Seq(0, 0))
  }

  test("legacy KLL sharing does not combine nondeterministic inputs or scoped object mappings") {
    val input = MemoryStream[Long](spark).toDF().queryExecution.analyzed
    val value = input.output.head
    val mapped = Size(MapObjects(identity[Expression], CreateArray(Seq(value)), LongType,
      elementNullable = false))
    val cases = Seq[(Function0[AggregateFunction], Option[Expression])](
      (() => KllSketchAggDouble(Rand(1L)), None),
      (() => KllSketchAggBigint(value), Some(GreaterThan(Rand(1L), Literal(0.5d)))),
      (() => KllSketchAggBigint(mapped), None))

    cases.foreach { case (function, filter) =>
      // Construct resolved planner inputs directly: analysis rejects random aggregate inputs,
      // but the compatibility planner must not make them eligible for historical sharing.
      val logical = Aggregate(Nil, Seq("a", "b").map { name =>
        Alias(function().toAggregateExpression(isDistinct = false, filter = filter), name)()
      }, input)
      assert(logical.resolved)
      checkPlan(logical, legacySharing = true, retained = Seq(0, 1), results = Seq(0, 1))
    }
  }

  test("legacy KLL sharing preserves separate buffers for different expression heights") {
    val input = MemoryStream[(Long, Long, Long, Long)](spark).toDF().queryExecution.analyzed
    val Seq(a, b, c, d) = input.output
    val leftDeep = Add(Add(Add(a, b), c), d)
    val balanced = Add(Add(a, b), Add(c, d))
    assert(leftDeep.canonicalized == balanced.canonicalized)
    assert(leftDeep.height != balanced.height)
    val logical = Aggregate(Nil, Seq(leftDeep, balanced).zipWithIndex.map { case (child, i) =>
      Alias(KllSketchAggBigint(child).toAggregateExpression(), s"sketch_$i")()
    }, input)
    checkPlan(logical, legacySharing = true, retained = Seq(0, 1), results = Seq(0, 1))
  }
}
