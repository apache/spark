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

package org.apache.spark.sql.execution.datasources.orc

import java.time.LocalDateTime

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.SchemaPruningSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

@ExtendedSQLTest
class OrcV1SchemaPruningSuite extends SchemaPruningSuite {
  override protected val dataSourceName: String = "orc"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.ORC_VECTORIZED_READER_ENABLED.key
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "orc")

  test("dynamic array index preserves the positions of ORC timestamp fields") {
    withTempPath { dir =>
      sql("""
        |SELECT array(named_struct('old', TIMESTAMP '2020-01-01',
        |  'wanted', TIMESTAMP_NTZ '2021-01-01')) AS arr, 0 AS idx
        |""".stripMargin).write.orc(dir.getCanonicalPath)
      val query = spark.read.orc(dir.getCanonicalPath).selectExpr("arr[idx].wanted")
      checkScanSchemata(query,
        "struct<arr:array<struct<old:timestamp,wanted:timestamp_ntz>>,idx:int>")
      checkAnswer(query, Row(LocalDateTime.of(2021, 1, 1, 0, 0)))
    }
  }
}
