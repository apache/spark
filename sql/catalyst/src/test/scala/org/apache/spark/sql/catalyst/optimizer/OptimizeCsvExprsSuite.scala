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

class OptimizeCsvExprsSuite extends PlanTest with ExpressionEvalHelper {

  private var csvExpressionOptimizeEnabled: Boolean = _
  protected override def beforeAll(): Unit = {
    csvExpressionOptimizeEnabled = SQLConf.get.csvExpressionOptimization
  }

  protected override def afterAll(): Unit = {
    SQLConf.get.setConf(SQLConf.CSV_EXPRESSION_OPTIMIZATION, csvExpressionOptimizeEnabled)
  }

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Csv optimization", FixedPoint(10), OptimizeCsvJsonExprs) :: Nil
  }

  val schema = StructType.fromDDL("a int, b int")

  private val structAtt = $"struct".struct(schema).notNull
  private val csvAttr = $"csv".string

  private val testRelation = LocalRelation(csvAttr)
  private val testRelation2 = LocalRelation(structAtt)

  test("SPARK-32968: prune unnecessary columns from GetStructField + from_csv") {
    val options = Map.empty[String, String]

    val query1 = testRelation
      .select(GetStructField(CsvToStructs(schema, options, $"csv"), 0))
    val optimized1 = Optimizer.execute(query1.analyze)

    val prunedSchema1 = StructType.fromDDL("a int")
    val expected1 = testRelation
      .select(GetStructField(CsvToStructs(schema, options, $"csv", None, Some(prunedSchema1)), 0))
      .analyze
    comparePlans(optimized1, expected1)

    val query2 = testRelation
      .select(GetStructField(CsvToStructs(schema, options, $"csv"), 1))
    val optimized2 = Optimizer.execute(query2.analyze)

    val prunedSchema2 = StructType.fromDDL("b int")
    val expected2 = testRelation
      .select(GetStructField(CsvToStructs(schema, options, $"csv", None, Some(prunedSchema2)), 0))
      .analyze
    comparePlans(optimized2, expected2)
  }

  test("SPARK-32968: don't prune columns if options is not empty") {
    val options = Map("mode" -> "failfast")

    val query = testRelation
      .select(GetStructField(CsvToStructs(schema, options, $"csv"), 0))
    val optimized = Optimizer.execute(query.analyze)

    val expected = query.analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-xxxxxx: optimize from_csv + to_csv") {
    val options = Map.empty[String, String]

    val query1 = testRelation2.select(
      CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct"))
    val optimized1 = Optimizer.execute(query1.analyze)

    val expected = testRelation2.select($"struct".as("struct")).analyze
    comparePlans(optimized1, expected)

    val query2 = testRelation2.select(
        CsvToStructs(schema, options,
          StructsToCsv(options,
            CsvToStructs(schema, options,
              StructsToCsv(options, $"struct")))).as("struct"))
    val optimized2 = Optimizer.execute(query2.analyze)

    comparePlans(optimized2, expected)
  }

  test("SPARK-xxxxxx: not optimize from_csv + to_csv if schema is different") {
    val options = Map.empty[String, String]
    val schema = StructType.fromDDL("a int")

    val query = testRelation2.select(
      CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelation2.select(
      CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-xxxxxx: not optimize from_csv + to_csv " +
    "if user gives schema with different letter case under case-insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = Map.empty[String, String]
      val schema = StructType.fromDDL("a int, B int")

      val query = testRelation2.select(
        CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct"))
      val optimized = Optimizer.execute(query.analyze)

      val expected = testRelation2.select(
        CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct")).analyze
      comparePlans(optimized, expected)
    }
  }

  test("SPARK-xxxxxx: not optimize from_csv + to_csv if nullability is different") {
    val options = Map.empty[String, String]
    val nonNullSchema = StructType(
      StructField("a", IntegerType, nullable = false) ::
        StructField("b", IntegerType, nullable = false) :: Nil)

    val structAtt = $"struct".struct(nonNullSchema).notNull
    val testRelationWithNonNullAttr = LocalRelation(structAtt)

    val schema = StructType.fromDDL("a int, b int")

    val query = testRelationWithNonNullAttr.select(
      CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelationWithNonNullAttr.select(
      CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-xxxxxx: not optimize from_csv + to_csv if option is not empty") {
    val options = Map("testOption" -> "test")

    val query = testRelation2.select(
      CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct"))
    val optimized = Optimizer.execute(query.analyze)

    val expected = testRelation2.select(
      CsvToStructs(schema, options, StructsToCsv(options, $"struct")).as("struct")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-xxxxx: not optimize from_csv + to_csv if timezone is different") {
    val options = Map.empty[String, String]
    val UTC_OPT = Option("UTC")
    val PST = getZoneId("-08:00")

    // timezone is different
    val query1 = testRelation2.select(
      CsvToStructs(schema, options,
        StructsToCsv(options, $"struct", Option(PST.getId)), UTC_OPT).as("struct"))
    val optimized1 = Optimizer.execute(query1.analyze)

    val expected1 = testRelation2.select(
      CsvToStructs(schema, options,
        StructsToCsv(options, $"struct", Option(PST.getId)), UTC_OPT).as("struct")).analyze
    comparePlans(optimized1, expected1)

    // timezone is same
    val query2 = testRelation2.select(
      CsvToStructs(schema, options,
        StructsToCsv(options, $"struct", UTC_OPT), UTC_OPT).as("struct"))
    val optimized2 = Optimizer.execute(query2.analyze)
    val expected2 = testRelation2.select($"struct".as("struct")).analyze
    comparePlans(optimized2, expected2)
  }
}
