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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

/**
 * DataFrame API-only tests for MERGE INTO schema evolution with aliased assignments.
 * These tests use the `.as()` DataFrame method which has no SQL equivalent, so they only run
 * in the Scala/DataFrame API test suites.
 */
trait MergeIntoSchemaEvolutionExtraScalaTests extends MergeIntoSchemaEvolutionSuiteBase {

  private def withNestedTestData(body: => Unit): Unit = {
    withTable(tableNameAsString) {
      withTempView("source") {
        val targetSchema = StructType(Seq(
          StructField("pk", IntegerType, nullable = false),
          StructField("info", StructType(Seq(
            StructField("a", IntegerType)
          )))
        ))
        createTable(CatalogV2Util.structTypeToV2Columns(targetSchema), Seq.empty)
        val targetDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(1, Row(10)),
          Row(2, Row(20))
        )), targetSchema)
        targetDf.writeTo(tableNameAsString).append()

        val sourceSchema = StructType(Seq(
          StructField("pk", IntegerType, nullable = false),
          StructField("info", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType)
          )))
        ))
        val sourceDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(2, Row(30, 50)),
          Row(3, Row(40, 75))
        )), sourceSchema)
        sourceDf.createOrReplaceTempView("source")

        body
      }
    }
  }

  test("schema evolution - top-level aliased struct column is not evolved") {
    withNestedTestData {
      val ex = intercept[AnalysisException] {
        spark.table("source")
          .mergeInto(tableNameAsString,
            col(s"$tableNameAsString.pk") === col("source.pk"))
          .whenMatched().update(Map("info" -> col("source.info").as("info")))
          .withSchemaEvolution()
          .merge()
      }
      assert(ex.getCondition === "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS")
    }
  }

  // Same as above with a mismatched alias name.
  test("schema evolution - top-level aliased struct column with mismatched name is not evolved") {
    withNestedTestData {
      val ex = intercept[AnalysisException] {
        spark.table("source")
          .mergeInto(tableNameAsString,
            col(s"$tableNameAsString.pk") === col("source.pk"))
          .whenMatched().update(Map(
            "info" -> col("source.info").as("something_else")))
          .withSchemaEvolution()
          .merge()
      }
      assert(ex.getCondition === "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS")
    }
  }

  test("schema evolution - nested field through aliased struct column is not evolved") {
    withNestedTestData {
      val ex = intercept[AnalysisException] {
        spark.table("source")
          .mergeInto(tableNameAsString,
            col(s"$tableNameAsString.pk") === col("source.pk"))
          .whenMatched().update(Map(
            "info.b" -> col("source.info").as("x").getField("b")))
          .withSchemaEvolution()
          .merge()
      }
      assert(ex.getCondition === "UNRESOLVED_COLUMN.WITH_SUGGESTION")
    }
  }

  test("schema evolution - complex expression value is not considered for evolution") {
    withNestedTestData {
      val ex = intercept[AnalysisException] {
        spark.table("source")
          .mergeInto(tableNameAsString,
            col(s"$tableNameAsString.pk") === col("source.pk"))
          .whenMatched().update(Map(
            "info.b" -> (col("source.info.b") + 1).as("b")))
          .withSchemaEvolution()
          .merge()
      }
      assert(ex.getCondition === "UNRESOLVED_COLUMN.WITH_SUGGESTION")
    }
  }
}
