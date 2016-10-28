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

package org.apache.spark.sql.catalyst.analysis

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project, Union}
import org.apache.spark.sql.types._


class DecimalPrecisionSuite extends PlanTest with BeforeAndAfter {
  private val conf = new SimpleCatalystConf(caseSensitiveAnalysis = true)
  private val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  private val analyzer = new Analyzer(catalog, conf)

  private val relation = LocalRelation(
    AttributeReference("i", IntegerType)(),
    AttributeReference("d1", DecimalType(2, 1))(),
    AttributeReference("d2", DecimalType(5, 2))(),
    AttributeReference("u", DecimalType.SYSTEM_DEFAULT)(),
    AttributeReference("f", FloatType)(),
    AttributeReference("b", DoubleType)()
  )

  private val i: Expression = UnresolvedAttribute("i")
  private val d1: Expression = UnresolvedAttribute("d1")
  private val d2: Expression = UnresolvedAttribute("d2")
  private val u: Expression = UnresolvedAttribute("u")
  private val f: Expression = UnresolvedAttribute("f")
  private val b: Expression = UnresolvedAttribute("b")

  before {
    catalog.createTempView("table", relation, overrideIfExists = true)
  }

  private def checkType(expression: Expression, expectedType: DataType): Unit = {
    val plan = Project(Seq(Alias(expression, "c")()), relation)
    assert(analyzer.execute(plan).schema.fields(0).dataType === expectedType)
  }

  private def checkComparison(expression: Expression, expectedType: DataType): Unit = {
    val plan = Project(Alias(expression, "c")() :: Nil, relation)
    val comparison = analyzer.execute(plan).collect {
      case Project(Alias(e: BinaryComparison, _) :: Nil, _) => e
    }.head
    assert(comparison.left.dataType === expectedType)
    assert(comparison.right.dataType === expectedType)
  }

  private def checkUnion(left: Expression, right: Expression, expectedType: DataType): Unit = {
    val plan =
      Union(Project(Seq(Alias(left, "l")()), relation),
        Project(Seq(Alias(right, "r")()), relation))
    val (l, r) = analyzer.execute(plan).collect {
      case Union(Seq(child1, child2)) => (child1.output.head, child2.output.head)
    }.head
    assert(l.dataType === expectedType)
    assert(r.dataType === expectedType)
  }

  test("basic operations") {
    checkType(Add(d1, d2), DecimalType(6, 2))
    checkType(Subtract(d1, d2), DecimalType(6, 2))
    checkType(Multiply(d1, d2), DecimalType(8, 3))
    checkType(Divide(d1, d2), DecimalType(10, 7))
    checkType(Divide(d2, d1), DecimalType(10, 6))
    checkType(Remainder(d1, d2), DecimalType(3, 2))
    checkType(Remainder(d2, d1), DecimalType(3, 2))
    checkType(Sum(d1), DecimalType(12, 1))
    checkType(Average(d1), DecimalType(6, 5))

    checkType(Add(Add(d1, d2), d1), DecimalType(7, 2))
    checkType(Add(Add(Add(d1, d2), d1), d2), DecimalType(8, 2))
    checkType(Add(Add(d1, d2), Add(d1, d2)), DecimalType(7, 2))
  }

  test("Comparison operations") {
    checkComparison(EqualTo(i, d1), DecimalType(11, 1))
    checkComparison(EqualNullSafe(d2, d1), DecimalType(5, 2))
    checkComparison(LessThan(i, d1), DecimalType(11, 1))
    checkComparison(LessThanOrEqual(d1, d2), DecimalType(5, 2))
    checkComparison(GreaterThan(d2, u), DecimalType.SYSTEM_DEFAULT)
    checkComparison(GreaterThanOrEqual(d1, f), DoubleType)
    checkComparison(GreaterThan(d2, d2), DecimalType(5, 2))
  }

  test("decimal precision for union") {
    checkUnion(d1, i, DecimalType(11, 1))
    checkUnion(i, d2, DecimalType(12, 2))
    checkUnion(d1, d2, DecimalType(5, 2))
    checkUnion(d2, d1, DecimalType(5, 2))
    checkUnion(d1, f, DoubleType)
    checkUnion(f, d2, DoubleType)
    checkUnion(d1, b, DoubleType)
    checkUnion(b, d2, DoubleType)
    checkUnion(d1, u, DecimalType.SYSTEM_DEFAULT)
    checkUnion(u, d2, DecimalType.SYSTEM_DEFAULT)
  }

  test("bringing in primitive types") {
    checkType(Add(d1, i), DecimalType(12, 1))
    checkType(Add(d1, f), DoubleType)
    checkType(Add(i, d1), DecimalType(12, 1))
    checkType(Add(f, d1), DoubleType)
    checkType(Add(d1, Cast(i, LongType)), DecimalType(22, 1))
    checkType(Add(d1, Cast(i, ShortType)), DecimalType(7, 1))
    checkType(Add(d1, Cast(i, ByteType)), DecimalType(5, 1))
    checkType(Add(d1, Cast(i, DoubleType)), DoubleType)
  }

  test("maximum decimals") {
    for (expr <- Seq(d1, d2, i, u)) {
      checkType(Add(expr, u), DecimalType.SYSTEM_DEFAULT)
      checkType(Subtract(expr, u), DecimalType.SYSTEM_DEFAULT)
    }

    checkType(Multiply(d1, u), DecimalType(38, 19))
    checkType(Multiply(d2, u), DecimalType(38, 20))
    checkType(Multiply(i, u), DecimalType(38, 18))
    checkType(Multiply(u, u), DecimalType(38, 36))

    checkType(Divide(u, d1), DecimalType(38, 18))
    checkType(Divide(u, d2), DecimalType(38, 19))
    checkType(Divide(u, i), DecimalType(38, 23))
    checkType(Divide(u, u), DecimalType(38, 18))

    checkType(Remainder(d1, u), DecimalType(19, 18))
    checkType(Remainder(d2, u), DecimalType(21, 18))
    checkType(Remainder(i, u), DecimalType(28, 18))
    checkType(Remainder(u, u), DecimalType.SYSTEM_DEFAULT)

    for (expr <- Seq(f, b)) {
      checkType(Add(expr, u), DoubleType)
      checkType(Subtract(expr, u), DoubleType)
      checkType(Multiply(expr, u), DoubleType)
      checkType(Divide(expr, u), DoubleType)
      checkType(Remainder(expr, u), DoubleType)
    }
  }

  test("DecimalType.isWiderThan") {
    val d0 = DecimalType(2, 0)
    val d1 = DecimalType(2, 1)
    val d2 = DecimalType(5, 2)
    val d3 = DecimalType(15, 3)
    val d4 = DecimalType(25, 4)

    assert(d0.isWiderThan(d1) === false)
    assert(d1.isWiderThan(d0) === false)
    assert(d1.isWiderThan(d2) === false)
    assert(d2.isWiderThan(d1) === true)
    assert(d2.isWiderThan(d3) === false)
    assert(d3.isWiderThan(d2) === true)
    assert(d4.isWiderThan(d3) === true)

    assert(d1.isWiderThan(ByteType) === false)
    assert(d2.isWiderThan(ByteType) === true)
    assert(d2.isWiderThan(ShortType) === false)
    assert(d3.isWiderThan(ShortType) === true)
    assert(d3.isWiderThan(IntegerType) === true)
    assert(d3.isWiderThan(LongType) === false)
    assert(d4.isWiderThan(LongType) === true)
    assert(d4.isWiderThan(FloatType) === false)
    assert(d4.isWiderThan(DoubleType) === false)
  }

  test("strength reduction for integer/decimal comparisons - basic test") {
    Seq(ByteType, ShortType, IntegerType, LongType).foreach { dt =>
      val int = AttributeReference("a", dt)()

      ruleTest(int > Literal(Decimal(4)), int > Literal(4L))
      ruleTest(int > Literal(Decimal(4.7)), int > Literal(4L))

      ruleTest(int >= Literal(Decimal(4)), int >= Literal(4L))
      ruleTest(int >= Literal(Decimal(4.7)), int >= Literal(5L))

      ruleTest(int < Literal(Decimal(4)), int < Literal(4L))
      ruleTest(int < Literal(Decimal(4.7)), int < Literal(5L))

      ruleTest(int <= Literal(Decimal(4)), int <= Literal(4L))
      ruleTest(int <= Literal(Decimal(4.7)), int <= Literal(4L))

      ruleTest(Literal(Decimal(4)) > int, Literal(4L) > int)
      ruleTest(Literal(Decimal(4.7)) > int, Literal(5L) > int)

      ruleTest(Literal(Decimal(4)) >= int, Literal(4L) >= int)
      ruleTest(Literal(Decimal(4.7)) >= int, Literal(4L) >= int)

      ruleTest(Literal(Decimal(4)) < int, Literal(4L) < int)
      ruleTest(Literal(Decimal(4.7)) < int, Literal(4L) < int)

      ruleTest(Literal(Decimal(4)) <= int, Literal(4L) <= int)
      ruleTest(Literal(Decimal(4.7)) <= int, Literal(5L) <= int)

    }
  }

  test("strength reduction for integer/decimal comparisons - overflow test") {
    val maxValue = Literal(Decimal(Long.MaxValue))
    val overflow = Literal(Decimal(Long.MaxValue) + Decimal(0.1))
    val minValue = Literal(Decimal(Long.MinValue))
    val underflow = Literal(Decimal(Long.MinValue) - Decimal(0.1))

    Seq(ByteType, ShortType, IntegerType, LongType).foreach { dt =>
      val int = AttributeReference("a", dt)()

      ruleTest(int > maxValue, int > Literal(Long.MaxValue))
      ruleTest(int > overflow, FalseLiteral)
      ruleTest(int > minValue, int > Literal(Long.MinValue))
      ruleTest(int > underflow, TrueLiteral)

      ruleTest(int >= maxValue, int >= Literal(Long.MaxValue))
      ruleTest(int >= overflow, FalseLiteral)
      ruleTest(int >= minValue, int >= Literal(Long.MinValue))
      ruleTest(int >= underflow, TrueLiteral)

      ruleTest(int < maxValue, int < Literal(Long.MaxValue))
      ruleTest(int < overflow, TrueLiteral)
      ruleTest(int < minValue, int < Literal(Long.MinValue))
      ruleTest(int < underflow, FalseLiteral)

      ruleTest(int <= maxValue, int <= Literal(Long.MaxValue))
      ruleTest(int <= overflow, TrueLiteral)
      ruleTest(int <= minValue, int <= Literal(Long.MinValue))
      ruleTest(int <= underflow, FalseLiteral)

      ruleTest(maxValue > int, Literal(Long.MaxValue) > int)
      ruleTest(overflow > int, TrueLiteral)
      ruleTest(minValue > int, Literal(Long.MinValue) > int)
      ruleTest(underflow > int, FalseLiteral)

      ruleTest(maxValue >= int, Literal(Long.MaxValue) >= int)
      ruleTest(overflow >= int, TrueLiteral)
      ruleTest(minValue >= int, Literal(Long.MinValue) >= int)
      ruleTest(underflow >= int, FalseLiteral)

      ruleTest(maxValue < int, Literal(Long.MaxValue) < int)
      ruleTest(overflow < int, FalseLiteral)
      ruleTest(minValue < int, Literal(Long.MinValue) < int)
      ruleTest(underflow < int, TrueLiteral)

      ruleTest(maxValue <= int, Literal(Long.MaxValue) <= int)
      ruleTest(overflow <= int, FalseLiteral)
      ruleTest(minValue <= int, Literal(Long.MinValue) <= int)
      ruleTest(underflow <= int, TrueLiteral)
    }
  }

  /** strength reduction for integer/decimal comparisons */
  def ruleTest(initial: Expression, transformed: Expression): Unit = {
    val testRelation = LocalRelation(AttributeReference("a", IntegerType)())
    comparePlans(
      DecimalPrecision(Project(Seq(Alias(initial, "a")()), testRelation)),
      Project(Seq(Alias(transformed, "a")()), testRelation))
  }
}
