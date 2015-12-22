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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Union, Project, LocalRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.{TableIdentifier, SimpleCatalystConf}

class DecimalPrecisionSuite extends SparkFunSuite with BeforeAndAfter {
  val conf = new SimpleCatalystConf(true)
  val catalog = new SimpleCatalog(conf)
  val analyzer = new Analyzer(catalog, EmptyFunctionRegistry, conf)

  val relation = LocalRelation(
    AttributeReference("i", IntegerType)(),
    AttributeReference("d1", DecimalType(2, 1))(),
    AttributeReference("d2", DecimalType(5, 2))(),
    AttributeReference("u", DecimalType.SYSTEM_DEFAULT)(),
    AttributeReference("f", FloatType)(),
    AttributeReference("b", DoubleType)()
  )

  val i: Expression = UnresolvedAttribute("i")
  val d1: Expression = UnresolvedAttribute("d1")
  val d2: Expression = UnresolvedAttribute("d2")
  val u: Expression = UnresolvedAttribute("u")
  val f: Expression = UnresolvedAttribute("f")
  val b: Expression = UnresolvedAttribute("b")

  before {
    catalog.registerTable(TableIdentifier("table"), relation)
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
      case Union(left, right) => (left.output.head, right.output.head)
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
}
