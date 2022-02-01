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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.connector.{DatasourceV2SQLBase, FakeV2Provider}
import org.apache.spark.sql.test.SharedSparkSession

class QueryCompilationErrorsDSv2Suite
  extends QueryTest
  with SharedSparkSession
  with DatasourceV2SQLBase {

  test("UNSUPPORTED_FEATURE: IF PARTITION NOT EXISTS not supported by INSERT") {
    val v2Format = classOf[FakeV2Provider].getName
    val tbl = "testcat.ns1.ns2.tbl"

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
          s"IF NOT EXISTS for the table '$tbl' by INSERT INTO.")
        assert(e.getErrorClass === "UNSUPPORTED_FEATURE")
        assert(e.getSqlState === "0A000")
      }
    }
  }
}
