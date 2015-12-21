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

package org.apache.spark.sql.hive.orc

import org.apache.spark.sql.{Column, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}

/**
 * A test suite that tests ORC filter API based filter pushdown optimization.
 */
class OrcFilterSuite extends QueryTest with OrcTest {
  // Because `ExpressionTree` is not accessible at Hive 1.2.x, this should be checked
  // in string form in order to check filter creation including logical expressions
  // such as `and`, `or` or `not`. So, this test uses `SearchArgument.toString()`
  // to produce string expression and then compare it to given templates below.
  // This might have to be changed after Hive version is upgraded.
  val isNullTmpl = """leaf-0 = (IS_NULL _1)
                 |expr = leaf-0""".stripMargin.trim
  val isNotNullTmpl = """leaf-0 = (IS_NULL _1)
                    |expr = (not leaf-0)""".stripMargin.trim
  val equalsTmpl = """leaf-0 = (EQUALS _1 %s)
                     |expr = leaf-0""".stripMargin.trim
  val notEqualsTmpl = """leaf-0 = (EQUALS _1 %s)
                        |expr = (not leaf-0)""".stripMargin.trim
  val nullSafeEqualsTmpl = """leaf-0 = (NULL_SAFE_EQUALS _1 %s)
                            |expr = leaf-0""".stripMargin.trim
  val lessThenTmpl = """leaf-0 = (LESS_THAN _1 %s)
                       |expr = leaf-0""".stripMargin.trim
  val greaterThenTmpl = """leaf-0 = (LESS_THAN_EQUALS _1 %s)
                          |expr = (not leaf-0)""".stripMargin.trim
  val lessThenEqualsTmpl = """leaf-0 = (LESS_THAN_EQUALS _1 %s)
                             |expr = leaf-0""".stripMargin.trim
  val greaterThenEqualsTmpl = """leaf-0 = (LESS_THAN _1 %s)
                                |expr = (not leaf-0)""".stripMargin.trim
  val notLessThen = """leaf-0 = (LESS_THAN _1 %s)
                      |expr = (not leaf-0)""".stripMargin.trim
  val andLessThenGreaterThenTmpl = """leaf-0 = (LESS_THAN _1 %s)
                                     |leaf-1 = (LESS_THAN_EQUALS _1 %s)
                                     |expr = (or leaf-0 (not leaf-1))""".stripMargin.trim
  val orLessThenGreaterThenTmpl = """leaf-0 = (LESS_THAN _1 %s)
                                    |leaf-1 = (LESS_THAN_EQUALS _1 %s)
                                    |expr = (and leaf-0 (not leaf-1))""".stripMargin.trim

  private def checkFilterPredicate(
                                    df: DataFrame,
                                    predicate: Predicate,
                                    stringExpr: String): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct
    val query = df
      .select(output.map(e => Column(e)): _*)
      .where(Column(predicate))

    var maybeRelation: Option[OrcRelation] = None
    val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
      case PhysicalOperation(_, filters, LogicalRelation(orcRelation: OrcRelation, _)) =>
        maybeRelation = Some(orcRelation)
        filters
    }.flatten.reduceLeftOption(_ && _)
    assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

    val (_, selectedFilters) =
      DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
    assert(selectedFilters.nonEmpty, "No filter is pushed down")

    val maybeFilter = OrcFilters.createFilter(selectedFilters.toArray)
    assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $selectedFilters")
    maybeFilter.foreach { f =>
      assert(f.toString == stringExpr)
    }
  }

  private def checkFilterPredicate
  (predicate: Predicate, stringExpr: String)
  (implicit df: DataFrame): Unit = {
    checkFilterPredicate(df, predicate, stringExpr)
  }

  test("filter pushdown - boolean") {
    withOrcDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      checkFilterPredicate('_1.isNull, isNullTmpl)
      checkFilterPredicate('_1.isNotNull, isNotNullTmpl)
    }
  }

  test("filter pushdown - integer") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, isNullTmpl)
      checkFilterPredicate('_1.isNotNull, isNotNullTmpl)

      checkFilterPredicate('_1 === 1, equalsTmpl.format(1))
      checkFilterPredicate('_1 <=> 1, nullSafeEqualsTmpl.format(1))
      checkFilterPredicate('_1 !== 1, notEqualsTmpl.format(1))

      checkFilterPredicate('_1 < 2, lessThenTmpl.format(2))
      checkFilterPredicate('_1 > 3, greaterThenTmpl.format(3))
      checkFilterPredicate('_1 <= 1, lessThenEqualsTmpl.format(1))
      checkFilterPredicate('_1 >= 4, greaterThenEqualsTmpl.format(4))

      checkFilterPredicate(Literal(1) === '_1, equalsTmpl.format(1))
      checkFilterPredicate(Literal(1) <=> '_1, nullSafeEqualsTmpl.format(1))
      checkFilterPredicate(Literal(2) > '_1, lessThenTmpl.format(2))
      checkFilterPredicate(Literal(3) < '_1, greaterThenTmpl.format(3))
      checkFilterPredicate(Literal(1) >= '_1, lessThenEqualsTmpl.format(1))
      checkFilterPredicate(Literal(4) <= '_1, greaterThenEqualsTmpl.format(4))

      checkFilterPredicate(!('_1 < 4), notLessThen.format(4))
      checkFilterPredicate('_1 < 2 || '_1 > 3, andLessThenGreaterThenTmpl.format(2, 3))
      checkFilterPredicate('_1 < 2 && '_1 > 3, orLessThenGreaterThenTmpl.format(2, 3))
    }
  }

  test("filter pushdown - long") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      checkFilterPredicate('_1.isNull, isNullTmpl)
      checkFilterPredicate('_1.isNotNull, isNotNullTmpl)

      checkFilterPredicate('_1 === 1, equalsTmpl.format(1))
      checkFilterPredicate('_1 <=> 1, nullSafeEqualsTmpl.format(1))
      checkFilterPredicate('_1 !== 1, notEqualsTmpl.format(1))

      checkFilterPredicate('_1 < 2, lessThenTmpl.format(2))
      checkFilterPredicate('_1 > 3, greaterThenTmpl.format(3))
      checkFilterPredicate('_1 <= 1, lessThenEqualsTmpl.format(1))
      checkFilterPredicate('_1 >= 4, greaterThenEqualsTmpl.format(4))

      checkFilterPredicate(Literal(1) === '_1, equalsTmpl.format(1))
      checkFilterPredicate(Literal(1) <=> '_1, nullSafeEqualsTmpl.format(1))
      checkFilterPredicate(Literal(2) > '_1, lessThenTmpl.format(2))
      checkFilterPredicate(Literal(3) < '_1, greaterThenTmpl.format(3))
      checkFilterPredicate(Literal(1) >= '_1, lessThenEqualsTmpl.format(1))
      checkFilterPredicate(Literal(4) <= '_1, greaterThenEqualsTmpl.format(4))

      checkFilterPredicate(!('_1 < 4), notLessThen.format(4))
      checkFilterPredicate('_1 < 2 || '_1 > 3, andLessThenGreaterThenTmpl.format(2, 3))
      checkFilterPredicate('_1 < 2 && '_1 > 3, orLessThenGreaterThenTmpl.format(2, 3))
    }
  }

  test("filter pushdown - float") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      checkFilterPredicate('_1.isNull, isNullTmpl)
      checkFilterPredicate('_1.isNotNull, isNotNullTmpl)

      checkFilterPredicate('_1 === 1, equalsTmpl.format(1.0))
      checkFilterPredicate('_1 <=> 1, nullSafeEqualsTmpl.format(1.0))
      checkFilterPredicate('_1 !== 1, notEqualsTmpl.format(1.0))

      checkFilterPredicate('_1 < 2, lessThenTmpl.format(2.0))
      checkFilterPredicate('_1 > 3, greaterThenTmpl.format(3.0))
      checkFilterPredicate('_1 <= 1, lessThenEqualsTmpl.format(1.0))
      checkFilterPredicate('_1 >= 4, greaterThenEqualsTmpl.format(4.0))

      checkFilterPredicate(Literal(1) === '_1, equalsTmpl.format(1.0))
      checkFilterPredicate(Literal(1) <=> '_1, nullSafeEqualsTmpl.format(1.0))
      checkFilterPredicate(Literal(2) > '_1, lessThenTmpl.format(2.0))
      checkFilterPredicate(Literal(3) < '_1, greaterThenTmpl.format(3.0))
      checkFilterPredicate(Literal(1) >= '_1, lessThenEqualsTmpl.format(1.0))
      checkFilterPredicate(Literal(4) <= '_1, greaterThenEqualsTmpl.format(4.0))

      checkFilterPredicate(!('_1 < 4), notLessThen.format(4.0))
      checkFilterPredicate('_1 < 2 || '_1 > 3, andLessThenGreaterThenTmpl.format(2.0, 3.0))
      checkFilterPredicate('_1 < 2 && '_1 > 3, orLessThenGreaterThenTmpl.format(2.0, 3.0))
    }
  }

  test("filter pushdown - double") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      checkFilterPredicate('_1.isNull, isNullTmpl)
      checkFilterPredicate('_1.isNotNull, isNotNullTmpl)

      checkFilterPredicate('_1 === 1, equalsTmpl.format(1.0))
      checkFilterPredicate('_1 <=> 1, nullSafeEqualsTmpl.format(1.0))
      checkFilterPredicate('_1 !== 1, notEqualsTmpl.format(1.0))

      checkFilterPredicate('_1 < 2, lessThenTmpl.format(2.0))
      checkFilterPredicate('_1 > 3, greaterThenTmpl.format(3.0))
      checkFilterPredicate('_1 <= 1, lessThenEqualsTmpl.format(1.0))
      checkFilterPredicate('_1 >= 4, greaterThenEqualsTmpl.format(4.0))

      checkFilterPredicate(Literal(1) === '_1, equalsTmpl.format(1.0))
      checkFilterPredicate(Literal(1) <=> '_1, nullSafeEqualsTmpl.format(1.0))
      checkFilterPredicate(Literal(2) > '_1, lessThenTmpl.format(2.0))
      checkFilterPredicate(Literal(3) < '_1, greaterThenTmpl.format(3.0))
      checkFilterPredicate(Literal(1) >= '_1, lessThenEqualsTmpl.format(1.0))
      checkFilterPredicate(Literal(4) <= '_1, greaterThenEqualsTmpl.format(4.0))

      checkFilterPredicate(!('_1 < 4), notLessThen.format(4.0))
      checkFilterPredicate('_1 < 2 || '_1 > 3, andLessThenGreaterThenTmpl.format(2.0, 3.0))
      checkFilterPredicate('_1 < 2 && '_1 > 3, orLessThenGreaterThenTmpl.format(2.0, 3.0))
    }
  }

  test("filter pushdown - string") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(i.toString))) { implicit df =>
      checkFilterPredicate('_1.isNull, isNullTmpl)
      checkFilterPredicate('_1.isNotNull, isNotNullTmpl)

      checkFilterPredicate('_1 === "1", equalsTmpl.format("1"))
      checkFilterPredicate('_1 <=> "1", nullSafeEqualsTmpl.format("1"))
      checkFilterPredicate('_1 !== "1", notEqualsTmpl.format("1"))

      checkFilterPredicate('_1 < "2", lessThenTmpl.format("2"))
      checkFilterPredicate('_1 > "3", greaterThenTmpl.format("3"))
      checkFilterPredicate('_1 <= "1", lessThenEqualsTmpl.format("1"))
      checkFilterPredicate('_1 >= "4", greaterThenEqualsTmpl.format("4"))

      checkFilterPredicate(Literal("1") === '_1, equalsTmpl.format("1"))
      checkFilterPredicate(Literal("1") <=> '_1, nullSafeEqualsTmpl.format("1"))
      checkFilterPredicate(Literal("2") > '_1, lessThenTmpl.format("2"))
      checkFilterPredicate(Literal("3") < '_1, greaterThenTmpl.format("3"))
      checkFilterPredicate(Literal("1") >= '_1, lessThenEqualsTmpl.format("1"))
      checkFilterPredicate(Literal("4") <= '_1, greaterThenEqualsTmpl.format("4"))

      checkFilterPredicate(!('_1 < "4"), notLessThen.format("4"))
      checkFilterPredicate('_1 < "2" || '_1 > "3", andLessThenGreaterThenTmpl.format("2", "3"))
      checkFilterPredicate('_1 < "2" && '_1 > "3", orLessThenGreaterThenTmpl.format("2", "3"))
    }
  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes("UTF-8")
    }

    withOrcDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkFilterPredicate('_1.isNull, isNullTmpl)
      checkFilterPredicate('_1.isNotNull, isNotNullTmpl)
    }
  }
}
