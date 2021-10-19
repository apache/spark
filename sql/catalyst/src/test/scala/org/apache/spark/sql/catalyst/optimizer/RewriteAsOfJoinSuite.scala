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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CreateStruct, GetStructField, If, OuterReference, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.MinBy
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LocalRelation}

class RewriteAsOfJoinSuite extends PlanTest {

  test("simple") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = OuterReference(left.output(0)) >= right.output(0)
    val rightStruct = CreateStruct(right.output)
    val orderExpression = OuterReference(left.output(0)) - right.output(0)
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("condition") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0),
      Some(left.output(1) === right.output(1)), Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = OuterReference(left.output(1)) === right.output(1) &&
      OuterReference(left.output(0)) >= right.output(0)
    val rightStruct = CreateStruct(right.output)
    val orderExpression = OuterReference(left.output(0)) - right.output(0)
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("left outer") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = OuterReference(left.output(0)) >= right.output(0)
    val rightStruct = CreateStruct(right.output)
    val orderExpression = OuterReference(left.output(0)) - right.output(0)
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = OuterReference(left.output(0)) >= right.output(0) &&
      right.output(0) >= OuterReference(left.output(0)) - 1
    val rightStruct = CreateStruct(right.output)
    val orderExpression = OuterReference(left.output(0)) - right.output(0)
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("allowExactMatches = false") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, LeftOuter,
      tolerance = None, allowExactMatches = false, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = OuterReference(left.output(0)) > right.output(0)
    val rightStruct = CreateStruct(right.output)
    val orderExpression = OuterReference(left.output(0)) - right.output(0)
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance & allowExactMatches = false") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = false, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = OuterReference(left.output(0)) > right.output(0) &&
      right.output(0) > OuterReference(left.output(0)) - 1
    val rightStruct = CreateStruct(right.output)
    val orderExpression = OuterReference(left.output(0)) - right.output(0)
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("direction = forward") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("forward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = OuterReference(left.output(0)) <= right.output(0)
    val rightStruct = CreateStruct(right.output)
    val orderExpression = right.output(0) - OuterReference(left.output(0))
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("direction = nearest") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("nearest"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = true
    val rightStruct = CreateStruct(right.output)
    val orderExpression = If(OuterReference(left.output(0)) > right.output(0),
      OuterReference(left.output(0)) - right.output(0),
      right.output(0) - OuterReference(left.output(0)))
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance & allowExactMatches = false & direction = nearest") {
    val left = LocalRelation('a.int, 'b.int, 'c.int)
    val right = LocalRelation('a.int, 'b.int, 'd.int)
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = false, direction = AsOfJoinDirection("nearest"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val filter = (!(OuterReference(left.output(0)) === right.output(0))) &&
      ((right.output(0) > OuterReference(left.output(0)) - 1) &&
        (right.output(0) < OuterReference(left.output(0)) + 1))
    val rightStruct = CreateStruct(right.output)
    val orderExpression = If(OuterReference(left.output(0)) > right.output(0),
      OuterReference(left.output(0)) - right.output(0),
      right.output(0) - OuterReference(left.output(0)))
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val correctAnswer = scalarSubquery
      .where(scalarSubquery.output.last.isNotNull)
      .select(left.output :+
        GetStructField(scalarSubquery.output.last, 0).as("a") :+
        GetStructField(scalarSubquery.output.last, 1).as("b") :+
        GetStructField(scalarSubquery.output.last, 2).as("d"): _*)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }
}
