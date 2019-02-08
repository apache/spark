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

import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{StringType, StructType}

class NestedColumnPruningSuite extends SchemaPruningTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Nested Column pruning", FixedPoint(100),
      ColumnPruning,
      RemoveNoopOperators) :: Nil
  }

  private val name = StructType.fromDDL("first string, middle string, last string")
  private val employer = StructType.fromDDL("id int, company struct<name:string, address:string>")
  private val contact = LocalRelation(
    'id.int,
    'name.struct(name),
    'address.string,
    'friends.array(name),
    'relatives.map(StringType, name),
    'employer.struct(employer))

  test("Pushing a single nested field projection - positive") {
    val ops = Array(
      (input: LogicalPlan) => input.limit(5),
      (input: LogicalPlan) => input.repartition(1),
      (input: LogicalPlan) => Sample(0.0, 0.6, false, 11L, input.subquery('x))
    )

    val queries = ops.map { op =>
      op(contact)
        .select(GetStructField('name, 1, Some("middle")))
        .analyze
    }

    val optimizedQueries = queries.map(Optimize.execute)

    val subQuery = contact
      .select(namedStruct(Literal("middle"), GetStructField('name, 1, Some("middle"))).as('name))
    val expectedQueries = ops
      .map(_(subQuery).select(GetStructField('name, 0, Some("middle"))).analyze)

    optimizedQueries.zip(expectedQueries).foreach { case (optimized, expected) =>
      comparePlans(optimized, expected)
    }
  }

  test("Pushing a single nested field projection - negative") {
    val ops = Array(
      (input: LogicalPlan) => input.distribute('name)(1),
      (input: LogicalPlan) => input.distribute($"name.middle")(1),
      (input: LogicalPlan) => input.orderBy('name.asc),
      (input: LogicalPlan) => input.orderBy($"name.middle".asc),
      (input: LogicalPlan) => input.sortBy('name.asc),
      (input: LogicalPlan) => input.sortBy($"name.middle".asc)
    )

    val queries = ops.map { op =>
      op(contact.select('name))
        .select(GetStructField('name, 1, Some("middle")))
        .analyze
    }

    val optimizedQueries = queries.map(Optimize.execute)
    val expectedQueries = queries

    optimizedQueries.zip(expectedQueries).foreach { case (optimized, expected) =>
      comparePlans(optimized, expected)
    }
  }

  test("Pushing a single nested field projection through filters - negative") {
    val ops = Array(
      (input: LogicalPlan) => input.where('name.isNotNull),
      (input: LogicalPlan) => input.where($"name.middle".isNotNull)
    )

    val queries = ops.map { op =>
      op(contact)
        .select(GetStructField('name, 1, Some("middle")))
        .analyze
    }

    val optimizedQueries = queries.map(Optimize.execute)
    val expectedQueries = queries

    optimizedQueries.zip(expectedQueries).foreach { case (optimized, expected) =>
      comparePlans(optimized, expected)
    }
  }

  test("Pushing multiple nested field projection - positive") {
    val first = GetStructField('name, 0, Some("first"))
    val last = GetStructField('name, 2, Some("last"))
    val revisedLast = GetStructField('name, 1, Some("last"))

    val query = contact
      .limit(5)
      .select('id, first, last)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select(
        'id,
        namedStruct(
          Literal("first"), first,
          Literal("last"), last).as("name"))
      .limit(5)
      .select('id, first, revisedLast)
      .analyze
    comparePlans(optimized, expected)
  }

  test("Pushing multiple nested field projection - negative") {
    val query = contact
      .limit(5)
      .select('id, GetStructField('name, 0, Some("first")), 'name)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select('id, 'name)
      .limit(5)
      .select('id, GetStructField('name, 0, Some("first")), 'name)
      .analyze
    comparePlans(optimized, expected)
  }

  test("function with nested field inputs") {
    val first = GetStructField('name, 0, Some("first"))
    val last = GetStructField('name, 2, Some("last"))
    val revisedLast = GetStructField('name, 1, Some("last"))

    val query = contact
      .limit(5)
      .select('id, ConcatWs(Seq(first, last)))
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select(
        'id,
        namedStruct(
          Literal("first"), first,
          Literal("last"), last).as("name"))
      .limit(5)
      .select('id, ConcatWs(Seq(first, revisedLast)))
      .analyze
    comparePlans(optimized, expected)
  }
}
