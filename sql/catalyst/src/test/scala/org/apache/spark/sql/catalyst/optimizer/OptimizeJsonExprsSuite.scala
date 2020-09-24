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

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Json optimization", FixedPoint(10), OptimizeJsonExprs) :: Nil
  }

  val schema = StructType.fromDDL("a int, b int")

  private val structAtt = 'struct.struct(schema).notNull

  private val testRelation = LocalRelation(structAtt)

  test("SPARK-32948: optimize from_json + to_json") {
    val options = Map.empty[String, String]

    val query1 = testRelation
      .select(JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct"))
    val optimized1 = Optimizer.execute(query1.analyze)

    val expected = testRelation.select('struct.as("struct")).analyze
    comparePlans(optimized1, expected)

    val query2 = testRelation
      .select(
        JsonToStructs(schema, options,
          StructsToJson(options,
            JsonToStructs(schema, options,
              StructsToJson(options, 'struct)))).as("struct"))
    val optimized2 = Optimizer.execute(query2.analyze)

    comparePlans(optimized2, expected)
  }

  test("SPARK-32948: not optimize from_json + to_json if schema is different") {
    val options = Map.empty[String, String]
    val schema = StructType.fromDDL("a int")

    val query = testRelation
      .select(JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelation.select(
      JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-32948: if user gives schema with different letter case under case-insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = Map.empty[String, String]
      val schema = StructType.fromDDL("a int, B int")

      val query = testRelation
        .select(JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct"))
      val optimized = Optimizer.execute(query.analyze)

      val expected = testRelation.select(
        JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct")).analyze
      comparePlans(optimized, expected)
    }
  }

  test("SPARK-32948: not optimize from_json + to_json if nullability is different") {
    val options = Map.empty[String, String]
    val nonNullSchema = StructType(
      StructField("a", IntegerType, false) :: StructField("b", IntegerType, false) :: Nil)

    val structAtt = 'struct.struct(nonNullSchema).notNull
    val testRelationWithNonNullAttr = LocalRelation(structAtt)

    val schema = StructType.fromDDL("a int, b int")

    val query = testRelationWithNonNullAttr
      .select(JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelationWithNonNullAttr.select(
      JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-32948: not optimize from_json + to_json if option is not empty") {
    val options = Map("testOption" -> "test")

    val query = testRelation
      .select(JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelation.select(
      JsonToStructs(schema, options, StructsToJson(options, 'struct)).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-32948: not optimize from_json + to_json if timezone is different") {
    val options = Map.empty[String, String]
    val UTC_OPT = Option("UTC")
    val PST = getZoneId("-08:00")

    val query1 = testRelation
      .select(JsonToStructs(schema, options,
        StructsToJson(options, 'struct, Option(PST.getId)), UTC_OPT).as("struct"))
    val optimized1 = Optimizer.execute(query1.analyze)

    val expected1 = testRelation.select(
      JsonToStructs(schema, options,
        StructsToJson(options, 'struct, Option(PST.getId)), UTC_OPT).as("struct")).analyze
    comparePlans(optimized1, expected1)

    val query2 = testRelation
      .select(JsonToStructs(schema, options,
        StructsToJson(options, 'struct, UTC_OPT), UTC_OPT).as("struct"))
    val optimized2 = Optimizer.execute(query2.analyze)
    val expected2 = testRelation.select('struct.as("struct")).analyze
    comparePlans(optimized2, expected2)
  }
}
