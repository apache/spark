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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class TableLocationSuite extends QueryTest with SharedSparkSession {

  test("SPARK-44185: relative LOCATION in CTAS should be qualified with warehouse") {
    withSQLConf(SQLConf.ALLOW_NON_EMPTY_LOCATION_IN_CTAS.key -> "false") {
      withTable("ctas1", "ctas2") {
        sql("CREATE TABLE ctas1 USING parquet AS SELECT 1 AS ID")
        val m = intercept[AnalysisException] {
          // 'ctas1' should be qualified with warehouse path for the checker as same as
          // table creation in catalog. Otherwise, the data could be polluted accidentally.
          sql("CREATE TABLE ctas2 USING parquet LOCATION 'ctas1' AS SELECT 1 AS ID")
        }.getMessage
        assert(m.contains("CREATE-TABLE-AS-SELECT cannot create table with location to a " +
          "non-empty directory"))
      }
    }
  }


  test("SPARK-44185: relative LOCATION with Append SaveMode shall check the qualified path") {
    withTable("ctas1", "ctas2") {
      sql("CREATE TABLE ctas1 USING parquet AS SELECT 1L AS ID")
      spark.range(10)
        .write
        .mode("append")
        .option("path", "ctas1")
        .format("parquet")
        .saveAsTable("ctas2")
      checkAnswer(spark.table("ctas1"), spark.table("ctas2"))
    }
  }

  test("SPARK-44185: relative LOCATION in CREATE TABLE shall lookup the path qualified with" +
    " warehouse for consistency") {
    withTable("ct2", "ct1") {
      try {
        sql("CREATE TABLE ct1 USING parquet SELECT 1 AS ID")
        // TODO(SPARK-44185): INSERT OVERWRITE DIRECTORY shall be qualified with current working
        //   directory(AS-IS) or with warehouse path?
        sql("INSERT OVERWRITE DIRECTORY 'ct1' USING parquet " + "SELECT 1 AS ID1, 2 AS ID2")

        // When schema is absent, ct2's infered from data files. Here table 'ct2' should not
        // use 'current working directory'/ct1 to infer and `warehouse path`/xx..x/ct1 to read
        // data, which shall be consistent to each other.
        sql("CREATE TABLE ct2 USING parquet LOCATION 'ct1'")
        checkAnswer(spark.table("ct1"), spark.table("ct2"))
      } finally {
        val path = new Path("ct1")
        val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
        val qualified = path.makeQualified(fs.getUri, fs.getWorkingDirectory)
        fs.delete(qualified, true)
      }
    }
  }
}
