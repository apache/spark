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

package org.apache.spark.sql.hive

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.test.{TestHiveSingleton, TestHiveSparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils


class TestHiveSuite extends TestHiveSingleton with SQLTestUtils {
  test("load test table based on case sensitivity") {
    val testHiveSparkSession = spark.asInstanceOf[TestHiveSparkSession]

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      sql("SELECT * FROM SRC").queryExecution.analyzed
      assert(testHiveSparkSession.getLoadedTables.contains("src"))
      assert(testHiveSparkSession.getLoadedTables.size == 1)
    }
    testHiveSparkSession.reset()

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val err = intercept[AnalysisException] {
        sql("SELECT * FROM SRC").queryExecution.analyzed
      }
      assert(err.message.contains("Table or view not found"))
    }
    testHiveSparkSession.reset()
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    assert(hiveClient.getConf("hive.in.test", "") == "true")
  }
}
