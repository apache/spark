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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkFunSuite, SparkNumberFormatException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeMap, AttributeReference, BindReferences, Cast, EqualTo, EvalMode, Expression, GetArrayItem, LessThan, Literal, Not, Or, Rand, StringSplit}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Filter}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.connector.{PartitionPredicateField, PartitionPredicateImpl}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.unsafe.types.UTF8String

class PushDownUtilsSuite extends SparkFunSuite {

  private val a = AttributeReference("a", IntegerType)()
  private val s = AttributeReference("s", StringType)()
  private val first = EqualTo(a, Literal(1))
  private val second = EqualTo(a, Literal(2))
  private val unsupported = EqualTo(
    GetArrayItem(StringSplit(s, Literal(","), Literal(-1)), Literal(0), false), Literal("fred"))

  private def createScanBuilder(
      useV2: Boolean,
      returnResidual: Boolean): ScanBuilder = {
    if (useV2) {
      new SupportsPushDownV2Filters {
        private var predicates = Array.empty[Predicate]
        override def build(): Scan = throw new UnsupportedOperationException
        override def pushPredicates(filters: Array[Predicate]): Array[Predicate] = {
          predicates = filters
          if (returnResidual) filters else Array.empty
        }
        override def pushedPredicates(): Array[Predicate] = predicates
      }
    } else {
      new SupportsPushDownFilters {
        private var predicates = Array.empty[sources.Filter]
        override def build(): Scan = throw new UnsupportedOperationException
        override def pushFilters(filters: Array[sources.Filter]): Array[sources.Filter] = {
          predicates = filters
          if (returnResidual) filters else Array.empty
        }
        override def pushedFilters(): Array[sources.Filter] = predicates
      }
    }
  }

  private def checkPushdown(
      input: Expression,
      expected: Option[Expression],
      fullyTranslated: Boolean = false): Unit = {
    for (useV2 <- Seq(false, true); returnResidual <- Seq(false, true)) {
      val builder = createScanBuilder(useV2, returnResidual)
      val (pushed, residual) = PushDownUtils.pushFilters(builder, Seq(input), None)
      if (useV2) {
        assert(pushed.toOption.get == expected.toSeq.map { e =>
          DataSourceV2Strategy.translateFilterV2(e).get
        })
      } else {
        assert(pushed.swap.toOption.get == expected.toSeq.map { e =>
          DataSourceStrategy.translateFilter(e, supportNestedPredicatePushdown = true).get
        })
      }
      if (fullyTranslated && !returnResidual) {
        assert(residual.isEmpty)
      } else {
        assert(residual == Seq(input))
      }

      if (input.deterministic) {
        def passes(e: Expression, row: InternalRow): Boolean =
          BindReferences.bindReference(e, Seq(a, s)).eval(row) == true
        for (value <- Seq(null, 0, 1, 2, 3); text <- Seq(null, "fred", "other", "fred,other")) {
          val row = InternalRow(value, if (text == null) null else UTF8String.fromString(text))
          val actual = expected.forall(passes(_, row)) && residual.forall(passes(_, row))
          assert(actual == passes(input, row), s"$input, value=$value, text=$text")
        }
      }
    }
  }

  test("SPARK-40608: push a necessary OR predicate and retain the original filter") {
    checkPushdown(Or(first, And(second, unsupported)), Some(Or(first, second)))
    checkPushdown(Or(And(unsupported, second), first), Some(Or(second, first)))
    checkPushdown(Or(And(first, unsupported), And(second, unsupported)),
      Some(Or(first, second)))
    checkPushdown(And(unsupported, first), Some(first))
  }

  test("SPARK-40608: do not partially push NOT or an OR with an unsupported branch") {
    checkPushdown(Not(Or(first, And(second, unsupported))), None)
    checkPushdown(Or(first, unsupported), None)
    checkPushdown(And(unsupported, unsupported), None)
    checkPushdown(Or(first, And(second, LessThan(Rand(0), Literal(0.5)))), None)
  }

  test("SPARK-40608: keep full pushdown of supported predicates") {
    val input = Or(first, second)
    checkPushdown(input, Some(input), fullyTranslated = true)
  }

  test("SPARK-40608: partial pushdown does not discount residual statistics twice") {
    val input = Or(first, And(second, unsupported))
    val columnStat = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val relation = StatsTestPlan(Seq(a, s), 10000, AttributeMap(Seq(a -> columnStat)))
    val expected = FilterEstimation(Filter(input, relation)).estimate.get.rowCount
    assert(expected.nonEmpty)

    for (useV2 <- Seq(false, true)) {
      val builder = createScanBuilder(useV2, returnResidual = true)
      val (_, residual) = PushDownUtils.pushFilters(builder, Seq(input), None)
      val actual = FilterEstimation(Filter(residual.reduceLeft(And), relation)).estimate.get
      assert(actual.rowCount == expected)
    }
  }

  test("SPARK-40608: keep an original filter that is also extracted from another filter") {
    val input = Or(first, And(second, unsupported))
    val extracted = Or(first, second)
    for (useV2 <- Seq(false, true); filters <- Seq(Seq(input, extracted), Seq(extracted, input))) {
      val builder = createScanBuilder(useV2, returnResidual = true)
      val (_, residual) = PushDownUtils.pushFilters(builder, filters, None)
      assert(residual == Seq(extracted, input))
    }
  }

  test("SPARK-40608: omit extracted residuals before iterative partition pushdown") {
    val input = Or(first, And(second, unsupported))
    val extracted = DataSourceV2Strategy.translateFilterV2(Or(first, second)).get
    val fields = Seq(a, s).map(attr => PartitionPredicateField(Seq(attr.name), Some(attr)))
    for (reportPushed <- Seq(false, true); acceptPartition <- Seq(false, true)) {
      val calls = ArrayBuffer.empty[Seq[Predicate]]
      val pushed = ArrayBuffer.empty[Predicate]
      val builder = new SupportsPushDownV2Filters {
        override def build(): Scan = throw new UnsupportedOperationException
        override def supportsIterativePushdown(): Boolean = true
        override def pushedPredicates(): Array[Predicate] = pushed.toArray
        override def pushPredicates(filters: Array[Predicate]): Array[Predicate] = {
          calls += filters.toSeq
          if (calls.size == 1) {
            if (reportPushed) pushed ++= filters
            filters
          } else if (acceptPartition) {
            pushed ++= filters
            Array.empty
          } else {
            filters
          }
        }
      }
      val (_, residual) = PushDownUtils.pushFilters(builder, Seq(input), Some(fields))
      assert(calls.size == 2)
      assert(calls.head == Seq(extracted))
      assert(calls(1).map(_.asInstanceOf[PartitionPredicateImpl].expression) == Seq(input))
      assert(residual == (if (acceptPartition) Nil else Seq(input)))
    }
  }

  test("SPARK-59572: only a runtime partition predicate keeps a key it cannot evaluate") {
    val ref = DataTypeUtils.toAttribute(StructField("p", StringType, nullable = true))
    val fields = Seq(PartitionPredicateField(Seq("p"), Some(ref)))
    // An ANSI cast of a non-numeric string throws when evaluated.
    val filter = EqualTo(Cast(ref, IntegerType, None, EvalMode.ANSI), Literal(1))
    val failing = InternalRow(UTF8String.fromString("hr"))

    // A runtime filter only prunes, so a partition it cannot evaluate is kept.
    val runtime = PushDownUtils.createRuntimePartitionPredicates(Seq(filter), fields)
    assert(runtime.size === 1)
    assert(runtime.head.eval(failing) === true)

    // Everywhere else Spark drops the filter the connector accepts, so the failure must surface.
    val (pushed, _) = PushDownUtils.createPartitionPredicates(Seq(filter), fields)
    assert(pushed.size === 1)
    intercept[SparkNumberFormatException](pushed.head.eval(failing))
  }
}
