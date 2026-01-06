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

package org.apache.spark.sql.connector

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


// SQL-based tests for delta-based row-level operations
class DeltaBasedMergeIntoSchemaEvolutionSQLSuite
  extends MergeIntoSchemaEvolutionSQLSuiteBase
  with MergeIntoSchemaEvolutionTests {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  test("source missing struct field violating check constraints") {
    Seq(true, false).foreach { withSchemaEvolution =>
      Seq(true, false).foreach { coercionEnabled =>
        withSQLConf(SQLConf.MERGE_INTO_NESTED_TYPE_COERCION_ENABLED.key ->
          coercionEnabled.toString) {
          withTempView("source") {
            // Target table has struct with nested field c2
            createAndInitTable(
              s"""pk INT NOT NULL,
                 |s STRUCT<c1: INT, c2: INT>,
                 |dep STRING""".stripMargin,
              """{ "pk": 0, "s": { "c1": 1, "c2": 10 }, "dep": "sales" }
                |{ "pk": 1, "s": { "c1": 2, "c2": 20 }, "dep": "hr" }"""
                .stripMargin)

            // Add CHECK constraint on nested field c2 using ALTER TABLE
            sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT check_c2 CHECK " +
              s"(s.c2 IS NOT NULL AND s.c2 > 1)")

            // Source table schema with struct missing the c2 field
            val sourceTableSchema = StructType(Seq(
              StructField("pk", IntegerType),
              StructField("s", StructType(Seq(
                StructField("c1", IntegerType)
                // missing field 'c2' which has CHECK constraint IS NOT NULL AND > 1
              ))),
              StructField("dep", StringType)
            ))

            val data = Seq(
              Row(1, Row(100), "engineering"),
              Row(2, Row(200), "finance")
            )
            spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
              .createOrReplaceTempView("source")

            val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
            val mergeStmt =
              s"""MERGE $schemaEvolutionClause INTO $tableNameAsString t USING source
                 |ON t.pk = source.pk
                 |WHEN MATCHED THEN
                 | UPDATE SET s = source.s, dep = source.dep
                 |""".stripMargin

            if (withSchemaEvolution && coercionEnabled) {
              val error = intercept[SparkRuntimeException] {
                sql(mergeStmt)
              }
              assert(error.getCondition == "CHECK_CONSTRAINT_VIOLATION")
              assert(error.getMessage.contains("CHECK constraint check_c2 s.c2 IS NOT NULL AND " +
                "s.c2 > 1 violated by row with values:\n - s.c2 : null"))
            } else {
              // Without schema evolution or coercion, the schema mismatch is rejected
              val error = intercept[AnalysisException] {
                sql(mergeStmt)
              }
              assert(error.errorClass.get == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA")
            }
          }
          sql(s"DROP TABLE IF EXISTS $tableNameAsString")
        }
      }
    }
  }
}

// Scala/DataFrame API-based tests for delta-based row-level operations
class DeltaBasedMergeIntoSchemaEvolutionScalaSuite
  extends MergeIntoSchemaEvolutionScalaSuiteBase
  with MergeIntoSchemaEvolutionTests {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }
}
