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

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf

import org.apache.spark.sql.{Column, DataFrame, QueryTest, SQLConf}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}

/**
 * A test suite that tests ORC filter API based filter pushdown optimization.
 */
class OrcFilterSuite extends QueryTest with OrcTest {
  private def checkFilterPredicate(
                                    df: DataFrame,
                                    predicate: Predicate,
                                    filterOperator: PredicateLeaf.Operator): Unit = {
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
      val operator = f.getLeaves.asScala.head.getOperator
      assert(operator === filterOperator)
    }
  }

  private def checkFilterPredicate
  (predicate: Predicate, filterOperator: PredicateLeaf.Operator)
  (implicit df: DataFrame): Unit = {
    checkFilterPredicate(df, predicate, filterOperator)
  }

  test("filter pushdown - boolean") {
    withOrcDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)
    }
  }

  test("filter pushdown - integer") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(!('_1 < 4), PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - long") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(!('_1 < 4), PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - float") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(!('_1 < 4), PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - double") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === 1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> 1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < 2, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > 3, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= 1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= 4, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal(1) === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal(1) <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal(2) > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal(3) < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(1) >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal(4) <= '_1, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(!('_1 < 4), PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - string") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(i.toString))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)

      checkFilterPredicate('_1 === "1", PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate('_1 <=> "1", PredicateLeaf.Operator.NULL_SAFE_EQUALS)

      checkFilterPredicate('_1 < "2", PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate('_1 > "3", PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 <= "1", PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate('_1 >= "4", PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(Literal("1") === '_1, PredicateLeaf.Operator.EQUALS)
      checkFilterPredicate(Literal("1") <=> '_1, PredicateLeaf.Operator.NULL_SAFE_EQUALS)
      checkFilterPredicate(Literal("2") > '_1, PredicateLeaf.Operator.LESS_THAN)
      checkFilterPredicate(Literal("3") < '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal("1") >= '_1, PredicateLeaf.Operator.LESS_THAN_EQUALS)
      checkFilterPredicate(Literal("4") <= '_1, PredicateLeaf.Operator.LESS_THAN)

      checkFilterPredicate(!('_1 < "4"), PredicateLeaf.Operator.LESS_THAN)
    }
  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes("UTF-8")
    }

    withOrcDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkFilterPredicate('_1.isNull, PredicateLeaf.Operator.IS_NULL)
    }
  }
}
