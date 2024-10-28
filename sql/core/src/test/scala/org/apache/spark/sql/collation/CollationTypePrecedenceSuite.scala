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

package org.apache.spark.sql.collation

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CollationTypePrecedenceSuite extends DatasourceV2SQLBase with AdaptiveSparkPlanHelper {

  val dataSource: String = "parquet"

  private def assertThrowsError(df: => DataFrame, errorClass: String): Unit = {
    val exception = intercept[SparkThrowable] {
      df
    }
    assert(exception.getCondition === errorClass)
  }

  test("access collated map via literal") {
    val tableName = "map_with_lit"

    def selectQuery(condition: String): DataFrame =
      sql(s"SELECT c1 FROM $tableName WHERE $condition = 'B'")

    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 MAP<STRING COLLATE UNICODE_CI, STRING COLLATE UNICODE_CI>,
           |  c2 STRING
           |) USING $dataSource
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES (map('a', 'b'), 'a')")

      Seq("c1['A']",
        "c1['A' COLLATE UNICODE_CI]",
        "c1[c2 COLLATE UNICODE_CI]").foreach { condition =>
        checkAnswer(selectQuery(condition), Seq(Row(Map("a" -> "b"))))
      }

      Seq(
        // different explicit collation
        "c1['A' COLLATE UNICODE]",
        // different implicit collation
        "c1[c2]").foreach { condition =>
        assertThrowsError(selectQuery(condition), "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
      }
    }
  }
}
