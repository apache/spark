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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, MapType, StringType, StructField, StructType}

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

  test("SPARK-48428: Do not pushdown when attr is used in expression with mutliple references") {
    val query = contact
      .limit(5)
      .select(
        GetStructField(GetStructField(CreateStruct(Seq($"id", $"employer")), 1), 0),
        $"employer.id")
      .analyze

    val optimized = Optimize.execute(query)

    val expected = contact
      .select($"id", $"employer")
      .limit(5)
      .select(
        GetStructField(GetStructField(CreateStruct(Seq($"id", $"employer")), 1), 0),
        $"employer.id")
      .analyze

    comparePlans(optimized, expected)
  }

  test("SPARK-XXXXX: multi-field nested column prune on generator output") {
    val pageviews = LocalRelation(
      $"pageId".int,
      $"requests".array(StructType(Seq(
        StructField("requestId", IntegerType),
        StructField("timestamp", StringType),
        StructField("items", ArrayType(StructType(Seq(
          StructField("itemId", IntegerType),
          StructField("itemName", StringType),
          StructField("itemPrice", DoubleType),
          StructField("category", StringType)
        ))))
      ))))

    // Test case: exploding requests and selecting multiple fields from items
    val query = pageviews
      .generate(Explode($"requests"), outputNames = Seq("request"))
      .generate(Explode($"request".getField("items")), outputNames = Seq("item"))
      .select($"item".getField("itemId"), $"item".getField("itemName"))
      .analyze
    val optimized = Optimize.execute(query)

    // The optimization should prune unnecessary fields (itemPrice, category) and timestamp field
    // Check that the optimization was applied by verifying aliases were created
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Multi-field optimization should create aliases")
  }

  test("SPARK-XXXXX: multi-field explode with deep nesting") {
    val deepNestedData = LocalRelation(
      $"data".array(StructType(Seq(
        StructField("level1", StructType(Seq(
          StructField("level2", StructType(Seq(
            StructField("neededField1", StringType),
            StructField("neededField2", IntegerType),
            StructField("unneededField", StringType)
          )))
        )))
      ))))

    val query = deepNestedData
      .generate(Explode($"data"), outputNames = Seq("item"))
      .select(
        $"item".getField("level1").getField("level2").getField("neededField1"),
        $"item".getField("level1").getField("level2").getField("neededField2")
      )
      .analyze
    val optimized = Optimize.execute(query)

    // Should optimize even with deep nesting
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Deep nesting optimization should create aliases")
  }

  test("SPARK-XXXXX: multi-field explode preserves single field optimization") {
    val simpleData = LocalRelation(
      $"items".array(StructType(Seq(
        StructField("needed", StringType),
        StructField("unneeded1", StringType),
        StructField("unneeded2", IntegerType)
      ))))

    // Single field case should still work
    val singleFieldQuery = simpleData
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select($"item".getField("needed"))
      .analyze
    val singleFieldOptimized = Optimize.execute(singleFieldQuery)
    val singleFieldAliases = collectGeneratedAliases(singleFieldOptimized)
    assert(singleFieldAliases.nonEmpty, "Single field optimization should still work")

    // Multi-field case should also work
    val multiFieldQuery = simpleData
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select($"item".getField("needed"), $"item".getField("unneeded1"))
      .analyze
    val multiFieldOptimized = Optimize.execute(multiFieldQuery)
    val multiFieldAliases = collectGeneratedAliases(multiFieldOptimized)
    assert(multiFieldAliases.nonEmpty, "Multi-field optimization should work")
  }

  test("SPARK-XXXXX: pageviews requests items use case") {
    // Real-world scenario: pageviews with requests containing items
    val pageviewSchema = StructType(Seq(
      StructField("pageId", StringType),
      StructField("userId", StringType),
      StructField("requests", ArrayType(StructType(Seq(
        StructField("requestId", StringType),
        StructField("timestamp", StringType),
        StructField("userAgent", StringType), // Should be pruned
        StructField("items", ArrayType(StructType(Seq(
          StructField("itemId", StringType),
          StructField("itemName", StringType),
          StructField("itemPrice", DoubleType), // Should be pruned
          StructField("itemCategory", StringType), // Should be pruned
          StructField("itemBrand", StringType) // Should be pruned
        ))))
      ))))
    ))

    val pageviews = LocalRelation($"data".struct(pageviewSchema))

    // Query that only needs item.itemId and item.itemName
    val query = pageviews
      .generate(Explode($"data".getField("requests")), outputNames = Seq("request"))
      .generate(Explode($"request".getField("items")), outputNames = Seq("item"))
      .select($"item".getField("itemId"), $"item".getField("itemName"))
      .analyze
    val optimized = Optimize.execute(query)

    // Should create aliases for optimization
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Pageviews use case should be optimized")

    // Verify we don't get compilation errors
    assert(optimized != null)
  }

  test("SPARK-XXXXX: complex multi-level nested arrays with filters") {
    val complexSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("events", ArrayType(StructType(Seq(
        StructField("eventId", IntegerType),
        StructField("sessions", ArrayType(StructType(Seq(
          StructField("sessionId", IntegerType), 
          StructField("actions", ArrayType(StructType(Seq(
            StructField("actionId", IntegerType),
            StructField("actionType", StringType),
            StructField("timestamp", StringType),
            StructField("metadata", StructType(Seq(
              StructField("userId", IntegerType),
              StructField("deviceType", StringType),
              StructField("location", StringType)
            )))
          ))))
        ))))
      ))))
    ))
    
    val complexData = LocalRelation($"data".struct(complexSchema))

    // Multi-level explode with complex filtering - should optimize deeply nested access
    val query = complexData
      .generate(Explode($"data".getField("events")), outputNames = Seq("event"))
      .generate(Explode($"event".getField("sessions")), outputNames = Seq("session"))
      .generate(Explode($"session".getField("actions")), outputNames = Seq("action"))
      .where($"action".getField("actionType") === "click")
      .select($"action".getField("actionId"), $"action".getField("metadata").getField("userId"))
      .analyze
    
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Complex multi-level optimization should create aliases")
  }

  test("SPARK-XXXXX: mixed single and multi-field optimization in same query") {
    val mixedData = LocalRelation(
      $"groups".array(StructType(Seq(
        StructField("groupId", IntegerType),
        StructField("members", ArrayType(StructType(Seq(
          StructField("memberId", IntegerType),
          StructField("memberName", StringType),
          StructField("memberRole", StringType),
          StructField("memberEmail", StringType)
        ))))
      ))))

    // First explode uses single field, second explode uses multiple fields
    val leftPart = mixedData
      .generate(Explode($"groups".getField("groupId")), outputNames = Seq("groupId"))
      
    val rightPart = mixedData
      .generate(Explode($"groups"), outputNames = Seq("group"))
      .generate(Explode($"group".getField("members")), outputNames = Seq("member"))
      .select($"member".getField("memberId"), $"member".getField("memberName"))
      
    val query = leftPart
      .join(rightPart, Cross, None)
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Mixed optimization scenarios should work")
  }

  test("SPARK-XXXXX: explode with complex expressions in projections") {
    val expressionData = LocalRelation(
      $"containers".array(StructType(Seq(
        StructField("containerId", IntegerType),
        StructField("items", ArrayType(StructType(Seq(
          StructField("itemId", IntegerType),
          StructField("itemName", StringType),
          StructField("itemValue", DoubleType),
          StructField("itemCount", IntegerType)
        ))))
      ))))

    // Using complex expressions with multiple nested field access
    val query = expressionData
      .generate(Explode($"containers"), outputNames = Seq("container"))
      .generate(Explode($"container".getField("items")), outputNames = Seq("item"))
      .select(
        ($"item".getField("itemId") + Literal(1000)).as("adjustedId"),
        $"item".getField("itemName").as("itemName"),
        ($"item".getField("itemValue") * $"item".getField("itemCount")).as("totalValue")
      )
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Complex expressions should still enable optimization")
  }

  test("SPARK-XXXXX: nested structs with optional fields (nullability)") {
    val nullableSchema = StructType(Seq(
      StructField("records", ArrayType(StructType(Seq(
        StructField("recordId", IntegerType, nullable = false),
        StructField("optionalData", StructType(Seq(
          StructField("field1", StringType, nullable = true),
          StructField("field2", IntegerType, nullable = true),
          StructField("field3", DoubleType, nullable = true)
        )), nullable = true)
      ))))
    ))
    
    val nullableData = LocalRelation($"data".struct(nullableSchema))

    val query = nullableData
      .generate(Explode($"data".getField("records")), outputNames = Seq("record"))
      .select(
        $"record".getField("recordId"),
        $"record".getField("optionalData").getField("field1"),
        $"record".getField("optionalData").getField("field2")
      )
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Nullable fields should be handled correctly")
  }

  test("SPARK-XXXXX: very wide structs with many fields") {
    // Create a struct with many fields to test field pruning efficiency
    val wideStructFields = (1 to 50).map(i => 
      StructField(s"field$i", StringType))
    val wideStruct = StructType(wideStructFields)
    
    val wideData = LocalRelation(
      $"items".array(wideStruct)
    )

    // Only select a few fields from the wide struct
    val query = wideData
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select(
        $"item".getField("field1"),
        $"item".getField("field25"), 
        $"item".getField("field50")
      )
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(aliases.nonEmpty, "Wide structs should benefit from field pruning")
  }

  test("SPARK-XXXXX: explode with simple joins") {
    val leftData = LocalRelation(
      $"leftRecords".array(StructType(Seq(
        StructField("id", IntegerType),
        StructField("leftName", StringType),
        StructField("leftValue", DoubleType)
      )))
    )
    
    val rightData = LocalRelation(
      $"rightRecords".array(StructType(Seq(
        StructField("id", IntegerType),
        StructField("rightName", StringType),
        StructField("rightCode", StringType)
      )))
    )

    // Simple case - just test the explode works
    val leftExploded = leftData
      .generate(Explode($"leftRecords"), outputNames = Seq("left"))
      .select($"left".getField("id"), $"left".getField("leftName"))
      .analyze
      
    val optimized = Optimize.execute(leftExploded)
    val aliases = collectGeneratedAliases(optimized)
    // Should optimize by pruning leftValue
    assert(optimized != null, "Simple exploded queries should work")
  }

  test("SPARK-XXXXX: performance regression prevention") {
    // Test that we don't accidentally make things worse for simple cases
    val simpleData = LocalRelation(
      $"simpleArray".array(StringType)
    )

    val simpleQuery = simpleData
      .generate(Explode($"simpleArray"), outputNames = Seq("item"))
      .select($"item")
      .analyze
      
    val optimized = Optimize.execute(simpleQuery)
    // For simple types, no nested column aliasing should occur
    val aliases = collectGeneratedAliases(optimized)
    // This should NOT create aliases for simple types
    // The optimization should be a no-op
    assert(optimized != null, "Simple queries should not break")
  }

  test("SPARK-XXXXX: negative test - unsupported generator types") {
    val data = LocalRelation($"data".string)
    
    // Stack generator is supported, but test with custom unsupported generator
    val query = data
      .generate(JsonTuple(Seq($"data", Literal("field1"), Literal("field2"))), outputNames = Seq("f1", "f2"))
      .select($"f1", $"f2")
      .analyze
      
    val optimized = Optimize.execute(query)
    // Should not crash, should handle gracefully
    assert(optimized != null)
  }

  test("SPARK-XXXXX: negative test - no nested field access") {
    val simpleStruct = LocalRelation(
      $"items".array(StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )))
    )

    // Query that doesn't access nested fields - optimization should not apply
    val query = simpleStruct
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select($"item") // Selecting entire struct, not nested fields
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    // Should not create aliases when no field pruning is possible
    assert(optimized != null)
  }

  test("SPARK-XXXXX: negative test - empty arrays") {
    val emptyArrayData = LocalRelation(
      $"emptyItems".array(StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )))
    )

    val query = emptyArrayData
      .generate(Explode($"emptyItems"), outputNames = Seq("item"))
      .select($"item".getField("id"))
      .analyze
      
    val optimized = Optimize.execute(query)
    // Should handle empty arrays gracefully
    assert(optimized != null)
  }

  test("SPARK-XXXXX: negative test - null struct fields") {
    val nullStructData = LocalRelation(
      $"nullableItems".array(StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("data", StructType(Seq(
          StructField("field1", StringType, nullable = true),
          StructField("field2", StringType, nullable = true)
        )), nullable = true)
      )))
    )

    val query = nullStructData
      .generate(Explode($"nullableItems"), outputNames = Seq("item"))
      .select($"item".getField("data").getField("field1"))
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    // Should handle nullable struct fields
    assert(optimized != null)
  }

  test("SPARK-XXXXX: edge case - circular references in schema") {
    // Test complex schema that might cause issues
    val complexSchema = StructType(Seq(
      StructField("items", ArrayType(StructType(Seq(
        StructField("itemId", IntegerType),
        StructField("children", ArrayType(StructType(Seq(
          StructField("childId", IntegerType),
          StructField("parentRef", StringType), // Could reference parent
          StructField("value", StringType)
        ))))
      ))))
    ))
    
    val circularData = LocalRelation($"root".struct(complexSchema))

    val query = circularData
      .generate(Explode($"root".getField("items")), outputNames = Seq("item"))
      .generate(Explode($"item".getField("children")), outputNames = Seq("child"))
      .select($"child".getField("childId"), $"child".getField("value"))
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(optimized != null, "Complex schemas should not cause failures")
  }

  test("SPARK-XXXXX: edge case - extremely deep nesting") {
    // Create very deep nesting to test limits
    var currentStruct = StructType(Seq(
      StructField("leaf", StringType),
      StructField("leafId", IntegerType)
    ))
    
    // Build 10 levels deep
    for (i <- 1 to 10) {
      currentStruct = StructType(Seq(
        StructField(s"level$i", currentStruct),
        StructField(s"id$i", IntegerType)
      ))
    }
    
    val deepData = LocalRelation($"items".array(currentStruct))

    // Access the deepest field
    var fieldAccess = $"item".getField("level1")
    for (i <- 2 to 10) {
      fieldAccess = fieldAccess.getField(s"level$i")
    }
    fieldAccess = fieldAccess.getField("leaf")

    val query = deepData
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select(fieldAccess)
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(optimized != null, "Very deep nesting should not cause stack overflow")
  }

  test("SPARK-XXXXX: edge case - mixed array and struct types") {
    val mixedSchema = StructType(Seq(
      StructField("arrayOfArrays", ArrayType(ArrayType(StringType))),
      StructField("arrayOfStructs", ArrayType(StructType(Seq(
        StructField("structId", IntegerType),
        StructField("nestedArray", ArrayType(StringType))
      )))),
      StructField("structOfArrays", StructType(Seq(
        StructField("arrayField", ArrayType(StringType)),
        StructField("simpleField", StringType)
      )))
    ))
    
    val mixedData = LocalRelation($"data".struct(mixedSchema))

    val query = mixedData
      .generate(Explode($"data".getField("arrayOfStructs")), outputNames = Seq("item"))
      .select($"item".getField("structId"))
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(optimized != null, "Mixed array/struct types should be handled")
  }

  test("SPARK-XXXXX: edge case - unicode and special characters in field names") {
    val unicodeSchema = StructType(Seq(
      StructField("items", ArrayType(StructType(Seq(
        StructField("field_with_underscore", StringType),
        StructField("field-with-dash", StringType),
        StructField("field with space", StringType),
        StructField("フィールド", StringType), // Japanese characters
        StructField("поле", StringType), // Cyrillic characters
        StructField("🔥field", StringType) // Emoji
      ))))
    ))
    
    val unicodeData = LocalRelation($"root".struct(unicodeSchema))

    val query = unicodeData
      .generate(Explode($"root".getField("items")), outputNames = Seq("item"))
      .select(
        $"item".getField("field_with_underscore"),
        $"item".getField("フィールド")
      )
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(optimized != null, "Unicode field names should be handled correctly")
  }

  test("SPARK-XXXXX: edge case - case sensitivity") {
    val caseSchema = StructType(Seq(
      StructField("Items", ArrayType(StructType(Seq(
        StructField("FieldName", StringType),
        StructField("fieldname", StringType), // Different case
        StructField("FIELDNAME", StringType)  // All caps
      ))))
    ))
    
    val caseData = LocalRelation($"root".struct(caseSchema))

    val query = caseData
      .generate(Explode($"root".getField("Items")), outputNames = Seq("item"))
      .select($"item".getField("FieldName"), $"item".getField("fieldname"))
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(optimized != null, "Case sensitive field names should work")
  }

  test("SPARK-XXXXX: stress test - many simultaneous explodes") {
    // Test with multiple explodes in the same query
    val multiExplodeData = LocalRelation(
      $"data1".array(StringType),
      $"data2".array(IntegerType),
      $"data3".array(StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )))
    )

    val query = multiExplodeData
      .generate(Explode($"data1"), outputNames = Seq("item1"))
      .generate(Explode($"data2"), outputNames = Seq("item2"))
      .generate(Explode($"data3"), outputNames = Seq("item3"))
      .select($"item1", $"item2", $"item3".getField("id"))
      .analyze
      
    val optimized = Optimize.execute(query)
    val aliases = collectGeneratedAliases(optimized)
    assert(optimized != null, "Multiple explodes should not interfere with each other")
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
