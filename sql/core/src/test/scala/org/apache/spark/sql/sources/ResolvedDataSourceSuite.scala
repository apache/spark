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

package org.apache.spark.sql.sources

import java.time.ZoneId

import org.apache.spark.SparkClassNotFoundException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.test.SharedSparkSession

class ResolvedDataSourceSuite extends SharedSparkSession {
  private def getProvidingClass(name: String): Class[_] =
    DataSource(
      sparkSession = spark,
      className = name,
      options = Map(DateTimeUtils.TIMEZONE_OPTION -> ZoneId.systemDefault().getId)
    ).providingClass

  test("jdbc") {
    assert(
      getProvidingClass("jdbc") ===
      classOf[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider])
    assert(
      getProvidingClass("org.apache.spark.sql.execution.datasources.jdbc") ===
      classOf[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider])
    assert(
      getProvidingClass("org.apache.spark.sql.jdbc") ===
        classOf[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider])
  }

  test("json") {
    assert(
      getProvidingClass("json") ===
      classOf[org.apache.spark.sql.execution.datasources.json.JsonFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.execution.datasources.json") ===
        classOf[org.apache.spark.sql.execution.datasources.json.JsonFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.json") ===
        classOf[org.apache.spark.sql.execution.datasources.json.JsonFileFormat])
  }

  test("parquet") {
    assert(
      getProvidingClass("parquet") ===
      classOf[org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.execution.datasources.parquet") ===
        classOf[org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.parquet") ===
        classOf[org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat])
  }

  test("csv") {
    assert(
      getProvidingClass("csv") ===
        classOf[org.apache.spark.sql.execution.datasources.csv.CSVFileFormat])
    assert(
      getProvidingClass("com.databricks.spark.csv") ===
        classOf[org.apache.spark.sql.execution.datasources.csv.CSVFileFormat])
  }

  test("avro: show deploy guide for loading the external avro module") {
    Seq("avro", "org.apache.spark.sql.avro").foreach { provider =>
      checkError(
        exception = intercept[AnalysisException] {
          getProvidingClass(provider)
        },
        condition = "_LEGACY_ERROR_TEMP_1139",
        parameters = Map("provider" -> provider)
      )
    }
  }

  test("kafka: show deploy guide for loading the external kafka module") {
    checkError(
      exception = intercept[AnalysisException] {
        getProvidingClass("kafka")
      },
      condition = "_LEGACY_ERROR_TEMP_1140",
      parameters = Map("provider" -> "kafka")
    )
  }

  test("error message for unknown data sources") {
    val error = intercept[SparkClassNotFoundException] {
      getProvidingClass("asfdwefasdfasdf")
    }
    checkError(
      exception = error,
      condition = "DATA_SOURCE_NOT_FOUND",
      parameters = Map("provider" -> "asfdwefasdfasdf")
    )
  }
}
