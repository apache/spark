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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class OptimizeJsonExprsSuite extends PlanTest with ExpressionEvalHelper {

  private var jsonExpressionOptimizeEnabled: Boolean = _
  private var getJsonObjectSharedParsingEnabled: Boolean = _
  protected override def beforeAll(): Unit = {
    jsonExpressionOptimizeEnabled = SQLConf.get.jsonExpressionOptimization
    getJsonObjectSharedParsingEnabled = SQLConf.get.getJsonObjectSharedParsingEnabled
    SQLConf.get.setConf(SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED, true)
  }

  protected override def afterAll(): Unit = {
    SQLConf.get.setConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION, jsonExpressionOptimizeEnabled)
    SQLConf.get.setConf(
      SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED, getJsonObjectSharedParsingEnabled)
  }

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Json optimization", FixedPoint(10), OptimizeCsvJsonExprs) :: Nil
  }

  object OptimizerWithCollapseProject extends RuleExecutor[LogicalPlan] {
    val batches = Batch(
      "Json optimization with project collapse",
      FixedPoint(10),
      CollapseProject,
      OptimizeCsvJsonExprs) :: Nil
  }

  object OptimizerWithColumnPruning extends RuleExecutor[LogicalPlan] {
    val batches = Batch(
      "Json optimization with column pruning",
      FixedPoint(10),
      ColumnPruning,
      CollapseProject,
      OptimizeCsvJsonExprs) :: Nil
  }

  val schema = StructType.fromDDL("a int, b int")

  private val structAtt = $"struct".struct(schema).notNull
  private val jsonAttr = $"json".string

  private val testRelation = LocalRelation(structAtt)
  private val testRelation2 = LocalRelation(jsonAttr)

  test("SPARK-32948: optimize from_json + to_json") {
    val options = Map.empty[String, String]

    val query1 = testRelation
      .select(JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct"))
    val optimized1 = Optimizer.execute(query1.analyze)

    val expected = testRelation.select($"struct".as("struct")).analyze
    comparePlans(optimized1, expected)

    val query2 = testRelation
      .select(
        JsonToStructs(schema, options,
          StructsToJson(options,
            JsonToStructs(schema, options,
              StructsToJson(options, $"struct")))).as("struct"))
    val optimized2 = Optimizer.execute(query2.analyze)

    comparePlans(optimized2, expected)
  }

  test("SPARK-32948: not optimize from_json + to_json if schema is different") {
    val options = Map.empty[String, String]
    val schema = StructType.fromDDL("a int")

    val query = testRelation
      .select(JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelation.select(
      JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-32948: if user gives schema with different letter case under case-insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = Map.empty[String, String]
      val schema = StructType.fromDDL("a int, B int")

      val query = testRelation
        .select(JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct"))
      val optimized = Optimizer.execute(query.analyze)

      val expected = testRelation.select(
        JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct")).analyze
      comparePlans(optimized, expected)
    }
  }

  test("SPARK-32948: not optimize from_json + to_json if nullability is different") {
    val options = Map.empty[String, String]
    val nonNullSchema = StructType(
      StructField("a", IntegerType, false) :: StructField("b", IntegerType, false) :: Nil)

    val structAtt = $"struct".struct(nonNullSchema).notNull
    val testRelationWithNonNullAttr = LocalRelation(structAtt)

    val schema = StructType.fromDDL("a int, b int")

    val query = testRelationWithNonNullAttr
      .select(JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelationWithNonNullAttr.select(
      JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-32948: not optimize from_json + to_json if option is not empty") {
    val options = Map("testOption" -> "test")

    val query = testRelation
      .select(JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelation.select(
      JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-32948: not optimize from_json + to_json if timezone is different") {
    val options = Map.empty[String, String]
    val UTC_OPT = Option("UTC")
    val PST = getZoneId("-08:00")

    val query1 = testRelation
      .select(JsonToStructs(schema, options,
        StructsToJson(options, $"struct", Option(PST.getId)), UTC_OPT).as("struct"))
    val optimized1 = Optimizer.execute(query1.analyze)

    val expected1 = testRelation.select(
      JsonToStructs(schema, options,
        StructsToJson(options, $"struct", Option(PST.getId)), UTC_OPT).as("struct")).analyze
    comparePlans(optimized1, expected1)

    val query2 = testRelation
      .select(JsonToStructs(schema, options,
        StructsToJson(options, $"struct", UTC_OPT), UTC_OPT).as("struct"))
    val optimized2 = Optimizer.execute(query2.analyze)
    val expected2 = testRelation.select($"struct".as("struct")).analyze
    comparePlans(optimized2, expected2)
  }

  test("SPARK-32958: prune unnecessary columns from GetStructField + from_json") {
    val options = Map.empty[String, String]

    val query1 = testRelation2
      .select(GetStructField(JsonToStructs(schema, options, $"json"), 0))
    val optimized1 = Optimizer.execute(query1.analyze)

    val prunedSchema1 = StructType.fromDDL("a int")
    val expected1 = testRelation2
      .select(GetStructField(JsonToStructs(prunedSchema1, options, $"json"), 0)).analyze
    comparePlans(optimized1, expected1)

    val query2 = testRelation2
      .select(GetStructField(JsonToStructs(schema, options, $"json"), 1))
    val optimized2 = Optimizer.execute(query2.analyze)

    val prunedSchema2 = StructType.fromDDL("b int")
    val expected2 = testRelation2
      .select(GetStructField(JsonToStructs(prunedSchema2, options, $"json"), 0)).analyze
    comparePlans(optimized2, expected2)
  }

  test("SPARK-47670: share simple top-level get_json_object paths") {
    val query = testRelation2.select(
      Cast(GetJsonObject($"json", Literal("$.b")), LongType).as("b"),
      GetJsonObject($"json", Literal("$['a']")).as("a"))
    val optimized = Optimizer.execute(query.analyze)

    optimized match {
      case Project(projectList, Project(innerProjectList, _: LocalRelation)) =>
        val sharedAlias = innerProjectList.collectFirst {
          case alias @ Alias(_: MultiGetJsonObject, "_shared_json_paths") => alias
        }.getOrElse(fail(s"Missing shared JSON paths in plan:\n$optimized"))
        val shared = sharedAlias.child.asInstanceOf[MultiGetJsonObject]
        assert(shared.fieldNames == Seq("b", "a"))
        assert(shared.fallbackPaths == Seq("$.b", "$['a']"))

        val sharedAttr = sharedAlias.toAttribute
        val extractedFields = projectList.flatMap(_.collect {
          case getStructField: GetStructField
              if getStructField.child.semanticEquals(sharedAttr) => getStructField
        })
        assert(extractedFields.map(_.ordinal) == Seq(0, 1))

      case _ =>
        fail(s"Expected shared JSON paths below the project, but found:\n$optimized")
    }
  }

  test("SPARK-47670: shared get_json_object parsing is disabled by default") {
    assert(!new SQLConf().getJsonObjectSharedParsingEnabled)
    val query = testRelation2.select(
      GetJsonObject($"json", Literal("$.a")).as("a"),
      GetJsonObject($"json", Literal("$.b")).as("b"))

    withSQLConf(SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> "false") {
      comparePlans(Optimizer.execute(query.analyze), query.analyze)
    }
  }

  test("SPARK-47670: do not fail optimization for unparseable get_json_object paths") {
    val oversizedIndex = "$[" + "9" * 100 + "]"
    val query = testRelation2.select(
      GetJsonObject($"json", Literal(oversizedIndex)).as("value"))

    comparePlans(Optimizer.execute(query.analyze), query.analyze)
  }

  test("SPARK-57626: share simple nested get_json_object paths") {
    val query = testRelation2.select(
      GetJsonObject($"json", Literal("$.a.b")).as("b"),
      GetJsonObject($"json", Literal("$['a']['c.d']")).as("c"),
      GetJsonObject($"json", Literal("$.e")).as("e"),
      GetJsonObject($"json", Literal("$.f.b")).as("other_b"))
    val optimized = Optimizer.execute(query.analyze)

    optimized match {
      case Project(projectList, Project(innerProjectList, _: LocalRelation)) =>
        val sharedAlias = innerProjectList.collectFirst {
          case alias @ Alias(_: MultiGetJsonObject, "_shared_json_paths") => alias
        }.getOrElse(fail(s"Missing shared JSON paths in plan:\n$optimized"))
        val shared = sharedAlias.child.asInstanceOf[MultiGetJsonObject]
        assert(shared.fieldNames == Seq("b", "c.d", "e", "b"))
        assert(shared.fallbackPaths == Seq("$.a.b", "$['a']['c.d']", "$.e", "$.f.b"))

        val sharedAttr = sharedAlias.toAttribute
        val extractedFields = projectList.flatMap(_.collect {
          case getStructField: GetStructField
              if getStructField.child.semanticEquals(sharedAttr) => getStructField
        })
        assert(extractedFields.map(_.ordinal) == Seq(0, 1, 2, 3))

      case _ =>
        fail(s"Expected shared nested JSON paths below the project, but found:\n$optimized")
    }
  }

  test("SPARK-57626: leave prefix-conflicting and unsupported paths independent") {
    val deepPath = (1 to 65).map(index => s"field$index").mkString("$.", ".", "")
    val legacyPaths = Seq("$.a.b", "$.items[0].id", "$.a.*", deepPath)
    val query = testRelation2.select(
      GetJsonObject($"json", Literal("$.a")).as("a"),
      GetJsonObject($"json", Literal(legacyPaths(0))).as("nested"),
      GetJsonObject($"json", Literal("$.c.d")).as("d"),
      GetJsonObject($"json", Literal(legacyPaths(1))).as("array"),
      GetJsonObject($"json", Literal(legacyPaths(2))).as("wildcard"),
      GetJsonObject($"json", Literal(legacyPaths(3))).as("deep"),
      GetJsonObject($"json", Literal("$.e")).as("e"))
    val optimized = Optimizer.execute(query.analyze)

    val shared = optimized.collect {
      case Project(projectList, _) => projectList.collectFirst {
        case alias @ Alias(_: MultiGetJsonObject, "_shared_json_paths") => alias
      }
    }.flatten.headOption.getOrElse(fail(s"Missing shared JSON paths in plan:\n$optimized"))
      .child.asInstanceOf[MultiGetJsonObject]
    assert(shared.fallbackPaths == Seq("$.a", "$.c.d", "$.e"))

    val remainingPaths = optimized.expressions.flatMap(_.collect {
      case GetJsonObject(_, Literal(path: UTF8String, StringType)) => path.toString
    })
    assert(legacyPaths.forall(remainingPaths.contains))
  }

  test("SPARK-57626: keep a later prefix-conflicting path independent") {
    val query = testRelation2.select(
      GetJsonObject($"json", Literal("$.a.b")).as("b"),
      GetJsonObject($"json", Literal("$.a")).as("a"),
      GetJsonObject($"json", Literal("$.a.c")).as("c"),
      GetJsonObject($"json", Literal("$.d")).as("d"))
    val optimized = Optimizer.execute(query.analyze)

    val shared = optimized.collect {
      case Project(projectList, _) => projectList.collectFirst {
        case alias @ Alias(_: MultiGetJsonObject, "_shared_json_paths") => alias
      }
    }.flatten.headOption.getOrElse(fail(s"Missing shared JSON paths in plan:\n$optimized"))
      .child.asInstanceOf[MultiGetJsonObject]
    assert(shared.fallbackPaths == Seq("$.a.b", "$.a.c", "$.d"))
    assert(optimized.expressions.exists(_.exists {
      case GetJsonObject(_, Literal(path: UTF8String, StringType)) => path.toString == "$.a"
      case _ => false
    }))
  }

  test("SPARK-57626: share parallel prefix chains in one optimizer invocation") {
    def prefixChain(root: String): Seq[String] = (1 to 9).map { depth =>
      (1 to depth).map(index => s"$root$index").mkString("$.", ".", "")
    }

    val leftPaths = prefixChain("a")
    val rightPaths = prefixChain("x")
    val query = testRelation2.select((leftPaths ++ rightPaths).zipWithIndex.map {
      case (path, index) => GetJsonObject($"json", Literal(path)).as(s"field_$index")
    }: _*)

    val expectedSharedPaths = leftPaths.zip(rightPaths).map {
      case (left, right) => Seq(left, right)
    }
    def assertSharedPaths(optimized: LogicalPlan): Unit = {
      val sharedPaths = optimized.collect {
        case Project(projectList, _) => projectList.collect {
          case Alias(shared: MultiGetJsonObject, "_shared_json_paths") => shared.fallbackPaths
        }
      }.flatten
      assert(sharedPaths == expectedSharedPaths)
    }

    assertSharedPaths(OptimizeCsvJsonExprs(query.analyze))
    assertSharedPaths(Optimizer.execute(query.analyze))
    assertSharedPaths(OptimizerWithCollapseProject.execute(query.analyze))
  }

  test("SPARK-57626: share a wide set of simple paths") {
    val pathCount = 2000
    val query = testRelation2.select((0 until pathCount).map { index =>
      GetJsonObject($"json", Literal(s"$$.field_$index")).as(s"field_$index")
    }: _*)
    val optimized = Optimizer.execute(query.analyze)

    val shared = optimized.collect {
      case Project(projectList, _) => projectList.collectFirst {
        case alias @ Alias(_: MultiGetJsonObject, "_shared_json_paths") => alias
      }
    }.flatten.headOption.getOrElse(fail(s"Missing shared JSON paths in plan:\n$optimized"))
      .child.asInstanceOf[MultiGetJsonObject]
    assert(shared.fallbackPaths.length == pathCount)
    assert(shared.fallbackPaths.head == "$.field_0")
    assert(shared.fallbackPaths.last == s"$$.field_${pathCount - 1}")
  }

  test("SPARK-47670: shared get_json_object paths survive project collapsing") {
    val query = testRelation2.select(
      GetJsonObject($"json", Literal("$.a")).as("a"),
      GetJsonObject($"json", Literal("$.b")).as("b"))

    val optimized = OptimizerWithCollapseProject.execute(query.analyze)

    assert(optimized.exists { plan =>
      plan.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject]))
    })
    assert(optimized.collect { case _: Project => true }.length == 2)
  }

  test("SPARK-47670: do not share get_json_object paths that are guarded or pruned") {
    val guardedQuery = testRelation2.select(
      If(
        IsNull($"json"),
        Literal(null, StringType),
        GetJsonObject($"json", Literal("$.a"))).as("a"),
      GetJsonObject($"json", Literal("$.b")).as("b"))
    assert(!Optimizer.execute(guardedQuery.analyze).exists { plan =>
      plan.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject]))
    })

    val lowerProject = testRelation2.select(
      GetJsonObject($"json", Literal("$.a")).as("a"),
      GetJsonObject($"json", Literal("$.b")).as("b"))
    val prunedQuery = lowerProject.select(lowerProject.output.head)
    assert(!OptimizerWithColumnPruning.execute(prunedQuery.analyze).exists { plan =>
      plan.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject]))
    })
  }

  test("SPARK-47670: do not share separately projected from_json fields") {
    val schema = StructType.fromDDL("a int, b struct<x: int>")
    val fromJson = JsonToStructs(schema, Map.empty, $"json")
    val query = testRelation2.select(
      GetStructField(fromJson, 0).as("a"),
      GetStructField(fromJson, 1).as("b"))

    val optimized = Optimizer.execute(query.analyze)
    val parsedSchemas = optimized.expressions.flatMap(_.collect {
      case jsonToStructs: JsonToStructs => jsonToStructs.schema
    })
    assert(parsedSchemas == Seq(
      StructType.fromDDL("a int"),
      StructType.fromDDL("b struct<x: int>")))
  }

  test("SPARK-47670: do not share get_json_object below right-first arithmetic") {
    val query = testRelation2.select(
      Pmod(Cast(GetJsonObject($"json", Literal("$.a")), IntegerType), Literal(0)).as("a"),
      GetJsonObject($"json", Literal("$.b")).as("b"))
    assert(!Optimizer.execute(query.analyze).exists { plan =>
      plan.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject]))
    })
  }

  test("SPARK-32958: prune unnecessary columns from GetArrayStructFields + from_json") {
    val options = Map.empty[String, String]
    val schema1 = ArrayType(StructType.fromDDL("a int, b int"), containsNull = true)
    val field1 = schema1.elementType.asInstanceOf[StructType](0)

    val query1 = testRelation2
      .select(GetArrayStructFields(
        JsonToStructs(schema1, options, $"json"), field1, 0, 2, true).as("a"))
    val optimized1 = Optimizer.execute(query1.analyze)

    val prunedSchema1 = ArrayType(StructType.fromDDL("a int"), containsNull = true)
    val expected1 = testRelation2
      .select(GetArrayStructFields(
        JsonToStructs(prunedSchema1, options, $"json"), field1, 0, 1, true).as("a")).analyze
    comparePlans(optimized1, expected1)

    val schema2 = ArrayType(
      StructType(
        StructField("a", IntegerType, false) ::
          StructField("b", IntegerType, false) :: Nil), containsNull = false)
    val field2 = schema2.elementType.asInstanceOf[StructType](1)
    val query2 = testRelation2
      .select(GetArrayStructFields(
        JsonToStructs(schema2, options, $"json"), field2, 1, 2, false).as("b"))
    val optimized2 = Optimizer.execute(query2.analyze)

    val prunedSchema2 = ArrayType(
      StructType(StructField("b", IntegerType, false) :: Nil), containsNull = false)
    val expected2 = testRelation2
      .select(GetArrayStructFields(
        JsonToStructs(prunedSchema2, options, $"json"), field2, 0, 1, false).as("b")).analyze
    comparePlans(optimized2, expected2)
  }

  test("SPARK-33907: do not prune unnecessary columns if options is not empty") {
    val options = Map("mode" -> "failfast")

    val query1 = testRelation2
      .select(GetStructField(JsonToStructs(schema, options, $"json"), 0))
    val optimized1 = Optimizer.execute(query1.analyze)

    comparePlans(optimized1, query1.analyze)

    val schema1 = ArrayType(StructType.fromDDL("a int, b int"), containsNull = true)
    val field1 = schema1.elementType.asInstanceOf[StructType](0)

    val query2 = testRelation2
      .select(GetArrayStructFields(
        JsonToStructs(schema1, options, $"json"), field1, 0, 2, true).as("a"))
    val optimized2 = Optimizer.execute(query2.analyze)

    comparePlans(optimized2, query2.analyze)
  }

  test("SPARK-33007: simplify named_struct + from_json") {
    val options = Map.empty[String, String]
    val schema = StructType.fromDDL("a int, b int, c long, d string")

    val prunedSchema1 = StructType.fromDDL("a int, b int")
    val nullStruct = namedStruct("a", Literal(null, IntegerType), "b", Literal(null, IntegerType))

    val UTC_OPT = Option("UTC")
    val json: BoundReference = $"json".string.canBeNull.at(0)

    assertEquivalent(
      testRelation2,
      namedStruct(
        "a", GetStructField(JsonToStructs(schema, options, json, UTC_OPT), 0),
        "b", GetStructField(JsonToStructs(schema, options, json, UTC_OPT), 1)).as("struct"),
      If(IsNull(json),
        nullStruct,
        KnownNotNull(JsonToStructs(prunedSchema1, options, json, UTC_OPT))).as("struct"))

    val field1 = StructType.fromDDL("a int")
    val field2 = StructType.fromDDL("b int")

    // Skip optimization if `namedStruct` aliases field name.
    assertEquivalent(
      testRelation2,
      namedStruct(
        "a1", GetStructField(JsonToStructs(schema, options, json, UTC_OPT), 0),
        "b", GetStructField(JsonToStructs(schema, options, json, UTC_OPT), 1)).as("struct"),
      namedStruct(
        "a1", GetStructField(JsonToStructs(field1, options, json, UTC_OPT), 0),
        "b", GetStructField(JsonToStructs(field2, options, json, UTC_OPT), 0)).as("struct"))

    assertEquivalent(
      testRelation2,
      namedStruct(
        "a", GetStructField(JsonToStructs(schema, options, json, UTC_OPT), 0),
        "a", GetStructField(JsonToStructs(schema, options, json, UTC_OPT), 0)).as("struct"),
      namedStruct(
        "a", GetStructField(JsonToStructs(field1, options, json, UTC_OPT), 0),
        "a", GetStructField(JsonToStructs(field1, options, json, UTC_OPT), 0)).as("struct"))

    val PST = getZoneId("-08:00")
    // Skip optimization if `JsonToStructs`s are not the same.
    assertEquivalent(
      testRelation2,
      namedStruct(
        "a", GetStructField(JsonToStructs(schema, options, json, UTC_OPT), 0),
        "b", GetStructField(JsonToStructs(schema, options, json, Option(PST.getId)), 1))
        .as("struct"),
      namedStruct(
        "a", GetStructField(JsonToStructs(field1, options, json, UTC_OPT), 0),
        "b", GetStructField(JsonToStructs(field2, options, json, Option(PST.getId)), 0))
        .as("struct"))
  }

  private def assertEquivalent(relation: LocalRelation, e1: Expression, e2: Expression): Unit = {
    val plan = relation.select(e1).analyze
    val actual = Optimizer.execute(plan)
    val expected = relation.select(e2).analyze
    comparePlans(actual, expected)

    Seq("""{"a":1, "b":2, "c": 123, "d": "test"}""", null).foreach(v => {
      val row = create_row(v)
      checkEvaluation(e1, e2.eval(row), row)
    })
  }

  test("SPARK-33078: disable json optimization") {
    withSQLConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "false") {
      val options = Map.empty[String, String]

      val query = testRelation
        .select(JsonToStructs(schema, options, StructsToJson(options, $"struct")).as("struct"))
      val optimized = Optimizer.execute(query.analyze)

      comparePlans(optimized, query.analyze)
    }
  }

  test("SPARK-49743: prune unnecessary columns from GetArrayStructFields does not change schema") {
    val options = Map.empty[String, String]
    val schema = ArrayType(StructType.fromDDL("a int, b int"), containsNull = true)

    val field = StructField("A", IntegerType) // Instead of "a", use "A" to test case sensitivity.
    val query = testRelation2
      .select(GetArrayStructFields(
        JsonToStructs(schema, options, $"json"), field, 0, 2, true).as("a"))
    val optimized = Optimizer.execute(query.analyze)

    val prunedSchema = ArrayType(StructType.fromDDL("a int"), containsNull = true)
    val expected = testRelation2
      .select(GetArrayStructFields(
        JsonToStructs(prunedSchema, options, $"json"), field, 0, 1, true).as("a")).analyze
    comparePlans(optimized, expected)
  }
}
