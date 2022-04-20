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

package org.apache.spark.sql.errors

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.connector.{DatasourceV2SQLBase, FakeV2Provider, InsertIntoSQLOnlyTests}
import org.apache.spark.sql.test.SharedSparkSession

class QueryCompilationErrorsDSv2Suite
  extends QueryTest
  with SharedSparkSession
  with DatasourceV2SQLBase
  with InsertIntoSQLOnlyTests {

  private val v2Source = classOf[FakeV2Provider].getName
  override protected val v2Format = v2Source
  override protected val catalogAndNamespace = "testcat.ns1.ns2."
  override protected val supportsDynamicOverwrite: Boolean = false
  override protected val includeSQLOnlyTests: Boolean = false
  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  test("UNSUPPORTED_FEATURE: IF PARTITION NOT EXISTS not supported by INSERT") {
    val tbl = s"${catalogAndNamespace}tbl"

    withTable(tbl) {
      val view = "tmp_view"
      val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
      df.createOrReplaceTempView(view)
      withTempView(view) {
        sql(s"CREATE TABLE $tbl (id bigint, data string) USING $v2Format PARTITIONED BY (id)")

        val e = intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE TABLE $tbl PARTITION (id = 1) IF NOT EXISTS SELECT * FROM $view")
        }

        checkAnswer(spark.table(tbl), spark.emptyDataFrame)
        assert(e.getMessage === "The feature is not supported: " +
          s""""IF NOT EXISTS" for the table `testcat`.`ns1`.`ns2`.`tbl` by "INSERT INTO".""")
        assert(e.getErrorClass === "UNSUPPORTED_FEATURE")
        assert(e.getSqlState === "0A000")
      }
    }
  }

  test("NON_PARTITION_COLUMN: static PARTITION clause fails with non-partition column") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTableAndData(t1) { view =>
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (data)")

      val e = intercept[AnalysisException] {
        sql(s"INSERT INTO TABLE $t1 PARTITION (id=1) SELECT data FROM $view")
      }

      verifyTable(t1, spark.emptyDataFrame)
      assert(e.getMessage === "PARTITION clause cannot contain a non-partition column name: `id`")
      assert(e.getErrorClass === "NON_PARTITION_COLUMN")
    }
  }

  test("NON_PARTITION_COLUMN: dynamic PARTITION clause fails with non-partition column") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTableAndData(t1) { view =>
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")

      val e = intercept[AnalysisException] {
        sql(s"INSERT INTO TABLE $t1 PARTITION (data) SELECT * FROM $view")
      }

      verifyTable(t1, spark.emptyDataFrame)
      assert(e.getMessage === "PARTITION clause cannot contain a non-partition column name: `data`")
      assert(e.getErrorClass === "NON_PARTITION_COLUMN")
    }
  }
}
