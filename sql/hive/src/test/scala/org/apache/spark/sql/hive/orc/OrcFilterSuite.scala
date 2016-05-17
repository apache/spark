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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.io.sarg.{PredicateLeaf, SearchArgument}

import org.apache.spark.sql.{Column, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation, LogicalRelation}

/**
 * A test suite that tests ORC filter API based filter pushdown optimization.
 */
class OrcFilterSuite extends QueryTest with OrcTest {
  private def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      checker: (SearchArgument) => Unit): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct
    val query = df
      .select(output.map(e => Column(e)): _*)
      .where(Column(predicate))

    var maybeRelation: Option[HadoopFsRelation] = None
    val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
      case PhysicalOperation(_, filters, LogicalRelation(orcRelation: HadoopFsRelation, _, _)) =>
        maybeRelation = Some(orcRelation)
        filters
    }.flatten.reduceLeftOption(_ && _)
    assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

    val (_, selectedFilters) =
      DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
    assert(selectedFilters.nonEmpty, "No filter is pushed down")

    val maybeFilter = OrcFilters.createFilter(query.schema, selectedFilters.toArray)
    assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $selectedFilters")
    checker(maybeFilter.get)
  }

  private def checkFilterPredicate
      (predicate: Predicate, filterOperator: PredicateLeaf.Operator)
      (implicit df: DataFrame): Unit = {
    def checkComparisonOperator(filter: SearchArgument) = {
      val operator = filter.getLeaves.asScala
      assert(operator.map(_.getOperator).contains(filterOperator))
    }
    checkFilterPredicate(df, predicate, checkComparisonOperator)
  }

  private def checkFilterPredicate
      (predicate: Predicate, stringExpr: String)
      (implicit df: DataFrame): Unit = {
    def checkLogicalOperator(filter: SearchArgument) = {
      assert(filter.toString == stringExpr)
    }
    checkFilterPredicate(df, predicate, checkLogicalOperator)
  }

  private def checkNoFilterPredicate
      (predicate: Predicate)
      (implicit df: DataFrame): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct
    val query = df
      .select(output.map(e => Column(e)): _*)
      .where(Column(predicate))

    var maybeRelation: Option[HadoopFsRelation] = None
    val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
      case PhysicalOperation(_, filters, LogicalRelation(orcRelation: HadoopFsRelation, _, _)) =>
        maybeRelation = Some(orcRelation)
        filters
    }.flatten.reduceLeftOption(_ && _)
    assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

    val (_, selectedFilters) =
      DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
    assert(selectedFilters.nonEmpty, "No filter is pushed down")

    val maybeFilter = OrcFilters.createFilter(query.schema, selectedFilters.toArray)
    assert(maybeFilter.isEmpty, s"Could generate filter predicate for $selectedFilters")
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
    }
  }

  test("filter pushdown - combinations with logical operators") {
    withOrcDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      // Because `ExpressionTree` is not accessible at Hive 1.2.x, this should be checked
      // in string form in order to check filter creation including logical operators
      // such as `and`, `or` or `not`. So, this function uses `SearchArgument.toString()`
      // to produce string expression and then compare it to given string expression below.
      // This might have to be changed after Hive version is upgraded.
      checkFilterPredicate(
        '_1.isNotNull,
        """leaf-0 = (IS_NULL _1)
          |expr = (not leaf-0)""".stripMargin.trim
      )
      checkFilterPredicate(
        '_1 =!= 1,
        """leaf-0 = (IS_NULL _1)
          |leaf-1 = (EQUALS _1 1)
          |expr = (and (not leaf-0) (not leaf-1))""".stripMargin.trim
      )
      checkFilterPredicate(
        !('_1 < 4),
        """leaf-0 = (IS_NULL _1)
          |leaf-1 = (LESS_THAN _1 4)
          |expr = (and (not leaf-0) (not leaf-1))""".stripMargin.trim
      )
      checkFilterPredicate(
        '_1 < 2 || '_1 > 3,
        """leaf-0 = (LESS_THAN _1 2)
          |leaf-1 = (LESS_THAN_EQUALS _1 3)
          |expr = (or leaf-0 (not leaf-1))""".stripMargin.trim
      )
      checkFilterPredicate(
        '_1 < 2 && '_1 > 3,
        """leaf-0 = (IS_NULL _1)
          |leaf-1 = (LESS_THAN _1 2)
          |leaf-2 = (LESS_THAN_EQUALS _1 3)
          |expr = (and (not leaf-0) leaf-1 (not leaf-2))""".stripMargin.trim
      )
    }
  }

  test("no filter pushdown - non-supported types") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }
    // ArrayType
    withOrcDataFrame((1 to 4).map(i => Tuple1(Array(i)))) { implicit df =>
      checkNoFilterPredicate('_1.isNull)
    }
    // DecimalType
    withOrcDataFrame((1 to 4).map(i => Tuple1(BigDecimal.valueOf(i)))) { implicit df =>
      checkNoFilterPredicate('_1 <= BigDecimal.valueOf(4))
    }
    // BinaryType
    withOrcDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkNoFilterPredicate('_1 <=> 1.b)
    }
    // BooleanType
    withOrcDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      checkNoFilterPredicate('_1 === true)
    }
    // TimestampType
    val stringTimestamp = "2015-08-20 15:57:00"
    withOrcDataFrame(Seq(Tuple1(Timestamp.valueOf(stringTimestamp)))) { implicit df =>
      checkNoFilterPredicate('_1 <=> Timestamp.valueOf(stringTimestamp))
    }
    // DateType
    val stringDate = "2015-01-01"
    withOrcDataFrame(Seq(Tuple1(Date.valueOf(stringDate)))) { implicit df =>
      checkNoFilterPredicate('_1 === Date.valueOf(stringDate))
    }
    // MapType
    withOrcDataFrame((1 to 4).map(i => Tuple1(Map(i -> i)))) { implicit df =>
      checkNoFilterPredicate('_1.isNotNull)
    }
  }
}
