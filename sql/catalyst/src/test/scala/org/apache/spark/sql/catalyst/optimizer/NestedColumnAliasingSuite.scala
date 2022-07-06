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
import org.apache.spark.sql.catalyst.analysis.{SimpleAnalyzer, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Cross
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

class NestedColumnAliasingSuite extends SchemaPruningTest {

  import NestedColumnAliasingSuite._

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Nested column pruning", FixedPoint(100),
      ColumnPruning,
      CollapseProject,
      RemoveNoopOperators) :: Nil
  }

  private val name = StructType.fromDDL("first string, middle string, last string")
  private val employer = StructType.fromDDL("id int, company struct<name:string, address:string>")
  private val contact = LocalRelation(
    $"id".int,
    $"name".struct(name),
    $"address".string,
    $"friends".array(name),
    Symbol("relatives").map(StringType, name),
    $"employer".struct(employer))

  test("Pushing a single nested field projection") {
    def testSingleFieldPushDown(op: LogicalPlan => LogicalPlan): Unit = {
      val middle = GetStructField($"name", 1, Some("middle"))
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
    val first = GetStructField($"name", 0, Some("first"))
    val last = GetStructField($"name", 2, Some("last"))

    val query = contact
      .limit(5)
      .select($"id", first, last)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select($"id", first, last)
      .limit(5)
      .analyze

    comparePlans(optimized, expected)
  }

  test("function with nested field inputs") {
    val first = GetStructField($"name", 0, Some("first"))
    val last = GetStructField($"name", 2, Some("last"))

    val query = contact
      .limit(5)
      .select($"id", ConcatWs(Seq(first, last)))
      .analyze

    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = contact
      .select($"id", first.as(aliases(0)), last.as(aliases(1)))
      .limit(5)
      .select(
        $"id",
        ConcatWs(Seq($"${aliases(0)}", $"${aliases(1)}")).as("concat_ws(name.first, name.last)"))
      .analyze
    comparePlans(optimized, expected)
  }

  test("multi-level nested field") {
    val field1 = GetStructField(GetStructField($"employer", 1, Some("company")), 0, Some("name"))
    val field2 = GetStructField($"employer", 0, Some("id"))

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
    val first1 = GetStructField($"name", 0, Some("first"))
    val first2 = GetStructField($"name", 1, Some("FIRST"))

    val query = contact
      .limit(5)
      .select($"id", first1, first2)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select($"id", first1, first2)
      .limit(5)
      .analyze

    comparePlans(optimized, expected)
  }

  test("Pushing a single nested field projection - negative") {
    val ops = Seq(
      (input: LogicalPlan) => input.distribute($"name")(1),
      (input: LogicalPlan) => input.orderBy($"name".asc),
      (input: LogicalPlan) => input.sortBy($"name".asc),
      (input: LogicalPlan) => input.union(input)
    )

    val queries = ops.map { op =>
      op(contact.select($"name"))
        .select(GetStructField($"name", 1, Some("middle")))
        .analyze
    }

    val optimizedQueries :+ optimizedUnion = queries.map(Optimize.execute)
    val expectedQueries = queries.init
    optimizedQueries.zip(expectedQueries).foreach { case (optimized, expected) =>
      comparePlans(optimized, expected)
    }
    val expectedUnion =
      contact.select($"name").union(contact.select($"name"))
        .select(GetStructField($"name", 1, Some("middle"))).analyze
    comparePlans(optimizedUnion, expectedUnion)
  }

  test("Pushing a single nested field projection through filters - negative") {
    val ops = Array(
      (input: LogicalPlan) => input.where($"name".isNotNull),
      (input: LogicalPlan) => input.where($"name.middle".isNotNull)
    )

    val queries = ops.map { op =>
      op(contact)
        .select(GetStructField($"name", 1, Some("middle")))
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
      .select($"id", GetStructField($"name", 0, Some("first")), $"name")
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select($"id", $"name")
      .limit(5)
      .select($"id", GetStructField($"name", 0, Some("first")), $"name")
      .analyze
    comparePlans(optimized, expected)
  }

  test("Some nested column means the whole structure") {
    val nestedRelation = LocalRelation($"a".struct($"b".struct($"c".int, $"d".int, $"e".int)))

    val query = nestedRelation
      .limit(5)
      .select(GetStructField($"a", 0, Some("b")))
      .analyze

    val optimized = Optimize.execute(query)

    comparePlans(optimized, query)
  }

  test("nested field pruning for getting struct field in array of struct") {
    val field1 = GetArrayStructFields(child = $"friends",
      field = StructField("first", StringType),
      ordinal = 0,
      numFields = 3,
      containsNull = true)
    val field2 = GetStructField($"employer", 0, Some("id"))

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

  test("nested field pruning for getting struct field in map") {
    val field1 = GetStructField(GetMapValue($"relatives", Literal("key")), 0, Some("first"))
    val field2 = GetArrayStructFields(child = MapValues($"relatives"),
      field = StructField("middle", StringType),
      ordinal = 1,
      numFields = 3,
      containsNull = true)

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

  test("SPARK-27633: Do not generate redundant aliases if parent nested field is aliased too") {
    val nestedRelation = LocalRelation($"a".struct($"b".struct($"c".int,
      $"d".struct($"f".int, $"g".int)), $"e".int))

    // `a.b`
    val first = $"a".getField("b")
    // `a.b.c` + 1
    val second = $"a".getField("b").getField("c") + Literal(1)
    // `a.b.d.f`
    val last = $"a".getField("b").getField("d").getField("f")

    val query = nestedRelation
      .limit(5)
      .select(first, second, last)
      .analyze

    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = nestedRelation
      .select(first.as(aliases(0)))
      .limit(5)
      .select($"${aliases(0)}".as("a.b"),
        ($"${aliases(0)}".getField("c") + Literal(1)).as("(a.b.c + 1)"),
        $"${aliases(0)}".getField("d").getField("f").as("a.b.d.f"))
      .analyze

    comparePlans(optimized, expected)
  }

  test("Nested field pruning for Project and Generate") {
    val query = contact
      .generate(Explode($"friends".getField("first")), outputNames = Seq("explode"))
      .select($"explode", $"friends".getField("middle"))
      .analyze
    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = contact
      .select(
        $"friends".getField("middle").as(aliases(0)),
        $"friends".getField("first").as(aliases(1)))
      .generate(Explode($"${aliases(1)}"),
        unrequiredChildIndex = Seq(1),
        outputNames = Seq("explode"))
      .select($"explode", $"${aliases(0)}".as("friends.middle"))
      .analyze
    comparePlans(optimized, expected)
  }

  test("Nested field pruning for Generate") {
    val query = contact
      .generate(Explode($"friends".getField("first")), outputNames = Seq("explode"))
      .select($"explode")
      .analyze
    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = contact
      .select($"friends".getField("first").as(aliases(0)))
      .generate(Explode($"${aliases(0)}"),
        unrequiredChildIndex = Seq(0),
        outputNames = Seq("explode"))
      .analyze
    comparePlans(optimized, expected)
  }

  test("Nested field pruning for Project and Generate: multiple-field case is not supported") {
    val companies = LocalRelation(
      $"id".int,
      $"employers".array(employer))

    val query = companies
      .generate(Explode($"employers".getField("company")), outputNames = Seq("company"))
      .select($"company".getField("name"), $"company".getField("address"))
      .analyze
    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = companies
      .select($"employers".getField("company").as(aliases(0)))
      .generate(Explode($"${aliases(0)}"),
        unrequiredChildIndex = Seq(0),
        outputNames = Seq("company"))
      .select($"company".getField("name").as("company.name"),
        $"company".getField("address").as("company.address"))
      .analyze
    comparePlans(optimized, expected)
  }

  test("Nested field pruning for Generate: not prune on required child output") {
    val query = contact
      .generate(
        Explode($"friends".getField("first")),
        outputNames = Seq("explode"))
      .select($"explode", $"friends")
      .analyze
    val optimized = Optimize.execute(query)

    val expected = contact
      .select($"friends")
      .generate(Explode($"friends".getField("first")),
        outputNames = Seq("explode"))
      .select($"explode", $"friends")
      .analyze
    comparePlans(optimized, expected)
  }

  test("Nested field pruning through RepartitionByExpression") {
    val query1 = contact
      .distribute($"id")(1)
      .select($"name.middle")
      .analyze
    val optimized1 = Optimize.execute(query1)

    val aliases1 = collectGeneratedAliases(optimized1)

    val expected1 = contact
      .select($"id", $"name".getField("middle").as(aliases1(0)))
      .distribute($"id")(1)
      .select($"${aliases1(0)}".as("middle"))
      .analyze
    comparePlans(optimized1, expected1)

    val query2 = contact
      .distribute($"name.middle")(1)
      .select($"name.middle")
      .analyze
    val optimized2 = Optimize.execute(query2)

    val aliases2 = collectGeneratedAliases(optimized2)

    val expected2 = contact
      .select($"name".getField("middle").as(aliases2(0)))
      .distribute($"${aliases2(0)}")(1)
      .select($"${aliases2(0)}".as("middle"))
      .analyze
    comparePlans(optimized2, expected2)

    val query3 = contact
      .select($"name")
      .distribute($"name")(1)
      .select($"name.middle")
      .analyze
    val optimized3 = Optimize.execute(query3)

    comparePlans(optimized3, query3)
  }

  test("Nested field pruning through Join") {
    val department = LocalRelation(
      $"depID".int,
      $"personID".string)

    val query1 = contact.join(department, condition = Some($"id" === $"depID"))
      .select($"name.middle")
      .analyze
    val optimized1 = Optimize.execute(query1)

    val aliases1 = collectGeneratedAliases(optimized1)

    val expected1 = contact.select($"id", $"name".getField("middle").as(aliases1(0)))
      .join(department.select($"depID"), condition = Some($"id" === $"depID"))
      .select($"${aliases1(0)}".as("middle"))
      .analyze
    comparePlans(optimized1, expected1)

    val query2 = contact.join(department, condition = Some($"name.middle" === $"personID"))
      .select($"name.first")
      .analyze
    val optimized2 = Optimize.execute(query2)

    val aliases2 = collectGeneratedAliases(optimized2)

    val expected2 = contact.select(
      $"name".getField("first").as(aliases2(0)),
      $"name".getField("middle").as(aliases2(1)))
      .join(department.select($"personID"), condition = Some($"${aliases2(1)}" === $"personID"))
      .select($"${aliases2(0)}".as("first"))
      .analyze
    comparePlans(optimized2, expected2)

    val contact2 = LocalRelation($"name2".struct(name))
    val query3 = contact.select($"name")
      .join(contact2, condition = Some($"name" === $"name2"))
      .select($"name.first")
      .analyze
    val optimized3 = Optimize.execute(query3)
    comparePlans(optimized3, query3)
  }

  test("Nested field pruning for Aggregate") {
    def runTest(basePlan: LogicalPlan => LogicalPlan): Unit = {
      val query1 = basePlan(contact).groupBy($"id")(first($"name.first").as("first")).analyze
      val optimized1 = Optimize.execute(query1)
      val aliases1 = collectGeneratedAliases(optimized1)

      val expected1 = basePlan(
        contact
        .select($"id", $"name".getField("first").as(aliases1(0)))
      ).groupBy($"id")(first($"${aliases1(0)}").as("first")).analyze
      comparePlans(optimized1, expected1)

      val query2 = basePlan(contact).groupBy($"name.last")(first($"name.first").as("first")).analyze
      val optimized2 = Optimize.execute(query2)
      val aliases2 = collectGeneratedAliases(optimized2)

      val expected2 = basePlan(
        contact
        .select($"name".getField("last").as(aliases2(0)), $"name".getField("first").as(aliases2(1)))
      ).groupBy($"${aliases2(0)}")(first($"${aliases2(1)}").as("first")).analyze
      comparePlans(optimized2, expected2)
    }

    Seq(
      (plan: LogicalPlan) => plan,
      (plan: LogicalPlan) => plan.limit(100),
      (plan: LogicalPlan) => plan.repartition(100),
      (plan: LogicalPlan) => Sample(0.0, 0.6, false, 11L, plan)).foreach {  base =>
      runTest(base)
    }

    val query3 = contact.groupBy($"id")(first($"name"), first($"name.first").as("first")).analyze
    val optimized3 = Optimize.execute(query3)
    val expected3 = contact.select($"id", $"name")
      .groupBy($"id")(first($"name"), first($"name.first").as("first")).analyze
    comparePlans(optimized3, expected3)
  }

  test("Nested field pruning for Window") {
    val spec = windowSpec($"address" :: Nil, $"id".asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(RowNumber(), spec)
    val query = contact
      .select($"name.first", winExpr.as("window"))
      .orderBy($"name.last".asc)
      .analyze
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    val expected = contact
      .select($"name.first", $"address", $"id", $"name.last".as(aliases(1)))
      .window(Seq(winExpr.as("window")), Seq($"address"), Seq($"id".asc))
      .select($"first", $"window", $"${aliases(1)}".as(aliases(0)))
      .orderBy($"${aliases(0)}".asc)
      .select($"first", $"window")
      .analyze
    comparePlans(optimized, expected)
  }

  test("Nested field pruning for Filter with other supported operators") {
    val spec = windowSpec($"address" :: Nil, $"id".asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(RowNumber(), spec)
    val query1 = contact.select($"name.first", winExpr.as("window"))
      .where($"window" === 1 && $"name.first" === "a")
      .analyze
    val optimized1 = Optimize.execute(query1)
    val aliases1 = collectGeneratedAliases(optimized1)
    val expected1 = contact
      .select($"name.first", $"address", $"id", $"name.first".as(aliases1(1)))
      .window(Seq(winExpr.as("window")), Seq($"address"), Seq($"id".asc))
      .select($"first", $"${aliases1(1)}".as(aliases1(0)), $"window")
      .where($"window" === 1 && $"${aliases1(0)}" === "a")
      .select($"first", $"window")
      .analyze
    comparePlans(optimized1, expected1)

    val query2 = contact.sortBy($"name.first".asc)
      .where($"name.first" === "a")
      .select($"name.first")
      .analyze
    val optimized2 = Optimize.execute(query2)
    val aliases2 = collectGeneratedAliases(optimized2)
    val expected2 = contact
      .select($"name.first".as(aliases2(1)))
      .sortBy($"${aliases2(1)}".asc)
      .select($"${aliases2(1)}".as(aliases2(0)))
      .where($"${aliases2(0)}" === "a")
      .select($"${aliases2(0)}".as("first"))
      .analyze
    comparePlans(optimized2, expected2)

    val query3 = contact.distribute($"name.first")(100)
      .where($"name.first" === "a")
      .select($"name.first")
      .analyze
    val optimized3 = Optimize.execute(query3)
    val aliases3 = collectGeneratedAliases(optimized3)
    val expected3 = contact
      .select($"name.first".as(aliases3(1)))
      .distribute($"${aliases3(1)}")(100)
      .select($"${aliases3(1)}".as(aliases3(0)))
      .where($"${aliases3(0)}" === "a")
      .select($"${aliases3(0)}".as("first"))
      .analyze
    comparePlans(optimized3, expected3)

    val department = LocalRelation(
      $"depID".int,
      $"personID".string)
    val query4 = contact.join(department, condition = Some($"id" === $"depID"))
      .where($"name.first" === "a")
      .select($"name.first")
      .analyze
    val optimized4 = Optimize.execute(query4)
    val aliases4 = collectGeneratedAliases(optimized4)
    val expected4 = contact
      .select($"id", $"name.first".as(aliases4(1)))
      .join(department.select($"depID"), condition = Some($"id" === $"depID"))
      .select($"${aliases4(1)}".as(aliases4(0)))
      .where($"${aliases4(0)}" === "a")
      .select($"${aliases4(0)}".as("first"))
      .analyze
    comparePlans(optimized4, expected4)

    def runTest(basePlan: LogicalPlan => LogicalPlan): Unit = {
      val query = basePlan(contact)
        .where($"name.first" === "a")
        .select($"name.first")
        .analyze
      val optimized = Optimize.execute(query)
      val aliases = collectGeneratedAliases(optimized)
      val expected = basePlan(contact
        .select($"name.first".as(aliases(0))))
        .where($"${aliases(0)}" === "a")
        .select($"${aliases(0)}".as("first"))
        .analyze
      comparePlans(optimized, expected)
    }
    Seq(
      (plan: LogicalPlan) => plan.limit(100),
      (plan: LogicalPlan) => plan.repartition(100),
      (plan: LogicalPlan) => Sample(0.0, 0.6, false, 11L, plan)).foreach {  base =>
        runTest(base)
      }
  }

  test("Nested field pruning for Sort") {
    val query1 = contact.select($"name.first", $"name.last")
      .sortBy($"name.first".asc, $"name.last".asc)
      .analyze
    val optimized1 = Optimize.execute(query1)
    val aliases1 = collectGeneratedAliases(optimized1)
    val expected1 = contact
      .select($"name.first",
        $"name.last",
        $"name.first".as(aliases1(0)),
        $"name.last".as(aliases1(1)))
      .sortBy($"${aliases1(0)}".asc, $"${aliases1(1)}".asc)
      .select($"first", $"last")
      .analyze
    comparePlans(optimized1, expected1)

    val query2 = contact.select($"name.first", $"name.last")
      .orderBy($"name.first".asc, $"name.last".asc)
      .analyze
    val optimized2 = Optimize.execute(query2)
    val aliases2 = collectGeneratedAliases(optimized2)
    val expected2 = contact
      .select($"name.first",
        $"name.last",
        $"name.first".as(aliases2(0)),
        $"name.last".as(aliases2(1)))
      .orderBy($"${aliases2(0)}".asc, $"${aliases2(1)}".asc)
      .select($"first", $"last")
      .analyze
    comparePlans(optimized2, expected2)
  }

  test("Nested field pruning for Expand") {
    def runTest(basePlan: LogicalPlan => LogicalPlan): Unit = {
      val query1 = Expand(
        Seq(
          Seq($"name.first", $"name.middle"),
          Seq(ConcatWs(Seq($"name.first", $"name.middle")),
            ConcatWs(Seq($"name.middle", $"name.first")))
        ),
        Seq($"a".string, $"b".string),
        basePlan(contact)
      ).analyze
      val optimized1 = Optimize.execute(query1)
      val aliases1 = collectGeneratedAliases(optimized1)

      val expected1 = Expand(
        Seq(
          Seq($"${aliases1(0)}", $"${aliases1(1)}"),
          Seq(ConcatWs(Seq($"${aliases1(0)}", $"${aliases1(1)}")),
            ConcatWs(Seq($"${aliases1(1)}", $"${aliases1(0)}")))
        ),
        Seq($"a".string, $"b".string),
        basePlan(contact.select(
          $"name".getField("first").as(aliases1(0)),
          $"name".getField("middle").as(aliases1(1))))
      ).analyze
      comparePlans(optimized1, expected1)
    }

    Seq(
      (plan: LogicalPlan) => plan,
      (plan: LogicalPlan) => plan.limit(100),
      (plan: LogicalPlan) => plan.repartition(100),
      (plan: LogicalPlan) => Sample(0.0, 0.6, false, 11L, plan)).foreach {  base =>
      runTest(base)
    }

    val query2 = Expand(
      Seq(
        Seq($"name", $"name.middle"),
        Seq($"name", ConcatWs(Seq($"name.middle", $"name.first")))
      ),
      Seq($"a".string, $"b".string),
      contact
    ).analyze
    val optimized2 = Optimize.execute(query2)
    val expected2 = Expand(
      Seq(
        Seq($"name", $"name.middle"),
        Seq($"name", ConcatWs(Seq($"name.middle", $"name.first")))
      ),
      Seq($"a".string, $"b".string),
      contact.select($"name")
    ).analyze
    comparePlans(optimized2, expected2)
  }

  test("SPARK-34638: nested column prune on generator output for one field") {
    val companies = LocalRelation(
      $"id".int,
      $"employers".array(employer))

    val query = companies
      .generate(Explode($"employers".getField("company")), outputNames = Seq("company"))
      .select($"company".getField("name"))
      .analyze
    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = companies
      .select($"employers".getField("company").getField("name").as(aliases(0)))
      .generate(Explode($"${aliases(0)}"),
        unrequiredChildIndex = Seq(0),
        outputNames = Seq("company"))
      .select($"company".as("company.name"))
      .analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-35636: do not push lambda key out of lambda function") {
    val rel = LocalRelation(
      Symbol("kvs").map(StringType, new StructType().add("v1", IntegerType)),
      $"keys".array(StringType))
    val key = UnresolvedNamedLambdaVariable("key" :: Nil)
    val lambda = LambdaFunction($"kvs".getItem(key).getField("v1"), key :: Nil)
    val query = rel
      .limit(5)
      .select($"keys", $"kvs")
      .limit(5)
      .select(ArrayTransform($"keys", lambda).as("a"))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-35636: do not push down extract value in higher order " +
    "function that references both sides of a join") {
    val left = LocalRelation(Symbol("kvs").map(StringType, new StructType().add("v1", IntegerType)))
    val right = LocalRelation($"keys".array(StringType))
    val key = UnresolvedNamedLambdaVariable("key" :: Nil)
    val lambda = LambdaFunction($"kvs".getItem(key).getField("v1"), key :: Nil)
    val query = left
      .join(right, Cross, None)
      .select(ArrayTransform($"keys", lambda).as("a"))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-35972: NestedColumnAliasing should consider semantic equality") {
    val dataType = new StructType()
      .add(StructField("itemid", StringType))
      .add(StructField("search_params", StructType(Seq(
        StructField("col1", StringType),
        StructField("col2", StringType)
      ))))
    val relation = LocalRelation($"struct_data".struct(dataType))
    val plan = relation
      .repartition(100)
      .select(
        GetStructField($"struct_data", 1, None).as("value"),
        $"struct_data.search_params.col1".as("col1"),
        $"struct_data.search_params.col2".as("col2")).analyze
    val query = Optimize.execute(plan)
    val optimized = relation
      .select(GetStructField($"struct_data", 1, None).as("_extract_search_params"))
      .repartition(100)
      .select(
        $"_extract_search_params".as("value"),
        $"_extract_search_params.col1".as("col1"),
        $"_extract_search_params.col2".as("col2")).analyze
    comparePlans(optimized, query)
  }

  test("SPARK-36677: NestedColumnAliasing should not push down aggregate functions into " +
    "projections") {
    val nestedRelation = LocalRelation(
      $"a".struct(
        $"c".struct(
          $"e".string),
        $"d".string),
      $"b".string)

    val plan = nestedRelation
      .select($"a", $"b")
      .groupBy($"b")(max($"a").getField("c").getField("e"))
      .analyze

    val optimized = Optimize.execute(plan)

    // The plan should not contain aggregation functions inside the projection
    SimpleAnalyzer.checkAnalysis(optimized)

    val expected = nestedRelation
      .groupBy($"b")(max($"a").getField("c").getField("e"))
      .analyze

    comparePlans(optimized, expected)
  }

  test("SPARK-37904: Improve rebalance in NestedColumnAliasing") {
    // alias nested columns through rebalance
    val plan1 = contact.rebalance($"id").select($"name.first").analyze
    val optimized1 = Optimize.execute(plan1)
    val expected1 = contact.select($"id", $"name.first".as("_extract_first"))
      .rebalance($"id").select($"_extract_first".as("first")).analyze
    comparePlans(optimized1, expected1)

    // also alias rebalance nested columns
    val plan2 = contact.rebalance($"name.first").select($"name.first").analyze
    val optimized2 = Optimize.execute(plan2)
    val expected2 = contact.select($"name.first".as("_extract_first"))
      .rebalance($"_extract_first".as("first")).select($"_extract_first".as("first")).analyze
    comparePlans(optimized2, expected2)

    // do not alias nested columns if its child contains root reference
    val plan3 = contact.rebalance($"name").select($"name.first").analyze
    val optimized3 = Optimize.execute(plan3)
    val expected3 = contact.select($"name").rebalance($"name").select($"name.first").analyze
    comparePlans(optimized3, expected3)
  }

  test("SPARK-38530: Do not push down nested ExtractValues with other expressions") {
    val inputType = StructType.fromDDL(
      "a int, b struct<c: array<int>, c2: int>")
    val simpleStruct = StructType.fromDDL(
      "b struct<c: struct<d: int, e: int>, c2 int>"
    )
    val input = LocalRelation(
      'id.int,
      'col1.array(ArrayType(inputType)))

    val query = input
      .generate(Explode('col1))
      .select(
        UnresolvedExtractValue(
          UnresolvedExtractValue(
            CaseWhen(Seq(('col.getField("a") === 1,
              Literal.default(simpleStruct)))),
            Literal("b")),
          Literal("c")).as("result"))
      .analyze
    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    // Only the inner-most col.a should be pushed down.
    val expected = input
      .select('col1.getField("a").as(aliases(0)))
      .generate(Explode($"${aliases(0)}"), unrequiredChildIndex = Seq(0))
      .select(UnresolvedExtractValue(UnresolvedExtractValue(
        CaseWhen(Seq(('col === 1,
          Literal.default(simpleStruct)))), Literal("b")), Literal("c")).as("result"))
      .analyze

    comparePlans(optimized, expected)
  }

  test("SPARK-38529: GeneratorNestedColumnAliasing does not pushdown for non-Explode") {
    val employer = StructType.fromDDL("id int, company struct<name:string, address:string>")
    val input = LocalRelation(
      'col1.int,
      'col2.array(ArrayType(StructType.fromDDL("field1 struct<col1: int, col2: int>, field2 int")))
    )
    val plan = input.generate(Inline('col2)).select('field1.getField("col1")).analyze
    val optimized = GeneratorNestedColumnAliasing.unapply(plan)
    // The plan is expected to be unchanged.
    comparePlans(plan, RemoveNoopOperators.apply(optimized.get))
  }
}

object NestedColumnAliasingSuite {
  def collectGeneratedAliases(query: LogicalPlan): ArrayBuffer[String] = {
    val aliases = ArrayBuffer[String]()
    query.transformAllExpressions {
      case a @ Alias(_, name) if name.startsWith("_extract_") =>
        aliases += name
        a
    }
    aliases
  }
}
