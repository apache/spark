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
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class OptimizeJsonExprsSuite extends PlanTest with ExpressionEvalHelper {

  private var jsonExpressionOptimizeEnabled: Boolean = _
  protected override def beforeAll(): Unit = {
    jsonExpressionOptimizeEnabled = SQLConf.get.jsonExpressionOptimization
  }

  protected override def afterAll(): Unit = {
    SQLConf.get.setConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION, jsonExpressionOptimizeEnabled)
  }

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Json optimization", FixedPoint(10), OptimizeCsvJsonExprs) :: Nil
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
}
