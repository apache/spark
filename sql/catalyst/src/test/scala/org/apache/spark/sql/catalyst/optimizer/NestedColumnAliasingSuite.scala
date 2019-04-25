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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{StringType, StructType}

class NestedColumnAliasingSuite extends SchemaPruningTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Nested column pruning", FixedPoint(100),
      ColumnPruning,
      CollapseProject,
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

  test("Pushing a single nested field projection") {
    def testSingleFieldPushDown(op: LogicalPlan => LogicalPlan): Unit = {
      val middle = GetStructField('name, 1, Some("middle"))
      val query = op(contact).select(middle).analyze
      val optimized = Optimize.execute(query)
      val expected = op(contact.select(middle)).analyze
      comparePlans(optimized, expected)
    }

    testSingleFieldPushDown((input: LogicalPlan) => input.limit(5))
    testSingleFieldPushDown((input: LogicalPlan) => input.repartition(1))
    testSingleFieldPushDown((input: LogicalPlan) => Sample(0.0, 0.6, false, 11L, input))
  }

  test("Pushing multiple nested field projection") {
    val first = GetStructField('name, 0, Some("first"))
    val last = GetStructField('name, 2, Some("last"))

    val query = contact
      .limit(5)
      .select('id, first, last)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select('id, first, last)
      .limit(5)
      .analyze

    comparePlans(optimized, expected)
  }

  test("function with nested field inputs") {
    val first = GetStructField('name, 0, Some("first"))
    val last = GetStructField('name, 2, Some("last"))

    val query = contact
      .limit(5)
      .select('id, ConcatWs(Seq(first, last)))
      .analyze

    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = contact
      .select('id, first.as(aliases(0)), last.as(aliases(1)))
      .limit(5)
      .select(
        'id,
        ConcatWs(Seq($"${aliases(0)}", $"${aliases(1)}")).as("concat_ws(name.first, name.last)"))
      .analyze
    comparePlans(optimized, expected)
  }

  test("multi-level nested field") {
    val field1 = GetStructField(GetStructField('employer, 1, Some("company")), 0, Some("name"))
    val field2 = GetStructField('employer, 0, Some("id"))

    val query = contact
      .limit(5)
      .select(field1, field2)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select(field1, field2)
      .limit(5)
      .analyze
    comparePlans(optimized, expected)
  }

  test("Push original case-sensitive names") {
    val first1 = GetStructField('name, 0, Some("first"))
    val first2 = GetStructField('name, 1, Some("FIRST"))

    val query = contact
      .limit(5)
      .select('id, first1, first2)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select('id, first1, first2)
      .limit(5)
      .analyze

    comparePlans(optimized, expected)
  }

  test("Pushing a single nested field projection - negative") {
    val ops = Seq(
      (input: LogicalPlan) => input.distribute('name)(1),
      (input: LogicalPlan) => input.distribute($"name.middle")(1),
      (input: LogicalPlan) => input.orderBy('name.asc),
      (input: LogicalPlan) => input.orderBy($"name.middle".asc),
      (input: LogicalPlan) => input.sortBy('name.asc),
      (input: LogicalPlan) => input.sortBy($"name.middle".asc),
      (input: LogicalPlan) => input.union(input)
    )

    val queries = ops.map { op =>
      op(contact.select('name))
        .select(GetStructField('name, 1, Some("middle")))
        .analyze
    }

    val optimizedQueries :+ optimizedUnion = queries.map(Optimize.execute)
    val expectedQueries = queries.init
    optimizedQueries.zip(expectedQueries).foreach { case (optimized, expected) =>
      comparePlans(optimized, expected)
    }
    val expectedUnion =
      contact.select('name).union(contact.select('name.as('name)))
        .select(GetStructField('name, 1, Some("middle"))).analyze
    comparePlans(optimizedUnion, expectedUnion)
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

  test("Do not optimize when parent field is used") {
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

  test("Some nested column means the whole structure") {
    val nestedRelation = LocalRelation('a.struct('b.struct('c.int, 'd.int, 'e.int)))

    val query = nestedRelation
      .limit(5)
      .select(GetStructField('a, 0, Some("b")))
      .analyze

    val optimized = Optimize.execute(query)

    val expected = nestedRelation
      .select(GetStructField('a, 0, Some("b")))
      .limit(5)
      .analyze

    comparePlans(optimized, expected)
  }

  private def collectGeneratedAliases(query: LogicalPlan): ArrayBuffer[String] = {
    val aliases = ArrayBuffer[String]()
    query.transformAllExpressions {
      case a @ Alias(_, name) if name.startsWith("_gen_alias_") =>
        aliases += name
        a
    }
    aliases
  }
}
