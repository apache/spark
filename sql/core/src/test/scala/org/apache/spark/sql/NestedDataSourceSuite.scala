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
package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}

// Datasource tests for nested schemas
trait NestedDataSourceSuiteBase extends QueryTest with SharedSparkSession {
  protected val nestedDataSources: Seq[String] = Seq("orc", "parquet", "json")
  protected def readOptions(schema: StructType): Map[String, String] = Map.empty
  protected def save(selectExpr: Seq[String], format: String, path: String): Unit = {
    spark
      .range(1L)
      .selectExpr(selectExpr: _*)
      .write.mode("overwrite")
      .format(format)
      .save(path)
  }
  protected val colType: String = "in the data schema"

  test("SPARK-32431: consistent error for nested and top-level duplicate columns") {
    Seq(
      Seq("id AS lowercase", "id + 1 AS camelCase") ->
        new StructType()
          .add("LowerCase", LongType)
          .add("camelcase", LongType)
          .add("CamelCase", LongType),
      Seq("NAMED_STRUCT('lowercase', id, 'camelCase', id + 1) AS StructColumn") ->
        new StructType().add("StructColumn",
          new StructType()
            .add("LowerCase", LongType)
            .add("camelcase", LongType)
            .add("CamelCase", LongType))
    ).foreach { case (selectExpr: Seq[String], caseInsensitiveSchema: StructType) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        nestedDataSources.map { format =>
          withClue(s"format = $format select = ${selectExpr.mkString(",")}") {
            withTempPath { dir =>
              val path = dir.getCanonicalPath
              save(selectExpr, format, path)
              val e = intercept[AnalysisException] {
                spark
                  .read
                  .options(readOptions(caseInsensitiveSchema))
                  .schema(caseInsensitiveSchema)
                  .format(format)
                  .load(path)
                  .collect()
              }
              assert(e.getErrorClass == "COLUMN_ALREADY_EXISTS")
              assert(e.getMessageParameters().get("columnName") == "`camelcase`")
            }
          }
        }
      }
    }
  }
}

class NestedDataSourceV1Suite extends NestedDataSourceSuiteBase {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, nestedDataSources.mkString(","))
}

class NestedDataSourceV2Suite extends NestedDataSourceSuiteBase {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
