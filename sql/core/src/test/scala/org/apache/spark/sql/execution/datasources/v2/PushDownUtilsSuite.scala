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

import org.apache.spark.{SparkFunSuite, SparkNumberFormatException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BindReferences, Cast, EqualTo, EvalMode, Expression, GetArrayItem, LessThan, Literal, Not, Or, Rand, StringSplit}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.connector.PartitionPredicateField
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

  private def checkPushdown(
      input: Expression,
      expected: Option[Expression],
      fullyTranslated: Boolean = false): Unit = {
    for (useV2 <- Seq(false, true); returnResidual <- Seq(false, true)) {
      val builder: ScanBuilder = if (useV2) {
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
        assert(residual.contains(input))
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
