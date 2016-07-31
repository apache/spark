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

package org.apache.spark.sql.hive.execution

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.execution.command.CreateTableCommand
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveDDLSemanticAnalysisSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  test("duplicate columns in bucketBy, sortBy and partitionBy in CTAS") {
    withTable("t") {
      var e = intercept[AnalysisException] {
        sql(
          """
            |CREATE TABLE t USING PARQUET
            |OPTIONS (PATH '/path/to/file')
            |CLUSTERED BY (a, a) SORTED BY (b) INTO 2 BUCKETS
            |AS SELECT 1 AS a, 2 AS b
          """.stripMargin)
      }
      assert(e.getMessage.contains("Found duplicate column(s) in Bucketing: `a`"))

      e = intercept[AnalysisException] {
        sql(
          """
            |CREATE TABLE t USING PARQUET
            |OPTIONS (PATH '/path/to/file')
            |CLUSTERED BY (a) SORTED BY (b, b) INTO 2 BUCKETS
            |AS SELECT 1 AS a, 2 AS b
          """.stripMargin)
      }
      assert(e.getMessage.contains("Found duplicate column(s) in Sorting: `b`"))

      e = intercept[AnalysisException] {
        sql(
          """
            |CREATE TABLE t USING PARQUET
            |OPTIONS (PATH '/path/to/file')
            |PARTITIONED BY (ds, ds, hr, hr)
            |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
            |AS SELECT 1 AS a, 2 AS b
          """.stripMargin)
      }
      assert(e.getMessage.contains("Found duplicate column(s) in Partition: `hr`, `ds`"))
    }
  }

  test("duplicate columns in bucketBy, sortBy and partitionBy in Create Data Source Tables") {
    withTable("t") {
      var e = intercept[AnalysisException] {
        sql(
          """
            |CREATE TABLE t (a String, b String) USING PARQUET
            |OPTIONS (PATH '/path/to/file')
            |CLUSTERED BY (a, a) SORTED BY (b) INTO 2 BUCKETS
          """.stripMargin)
      }
      assert(e.getMessage.contains("Found duplicate column(s) in Bucketing: `a`"))

      e = intercept[AnalysisException] {
        sql(
          """
            |CREATE TABLE t (a String, b String) USING PARQUET
            |OPTIONS (PATH '/path/to/file')
            |CLUSTERED BY (a) SORTED BY (b, b) INTO 2 BUCKETS
          """.stripMargin)
      }
      assert(e.getMessage.contains("Found duplicate column(s) in Sorting: `b`"))

      e = intercept[AnalysisException] {
        sql(
          """
            |CREATE TABLE t (a String, b String) USING PARQUET
            |OPTIONS (PATH '/path/to/file')
            |PARTITIONED BY (ds, ds, hr, hr)
            |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
          """.stripMargin)
      }
      assert(e.getMessage.contains("Found duplicate column(s) in Partition: `hr`, `ds`"))
    }
  }

  test("duplicate columns in partitionBy in Create Cataloged Table") {
    withTable("t") {
      val errorQuery = "CREATE TABLE boxes (b INT, c INT) PARTITIONED BY (a INT, a INT)"
      val correctQuery = "CREATE TABLE boxes (b INT, c INT) PARTITIONED BY (a INT)"
      val e = intercept[AnalysisException] { sql(errorQuery) }
      assert(e.getMessage.contains("Found duplicate column(s) in Partition: `a`"))
      assert(sql(correctQuery).queryExecution.analyzed.isInstanceOf[CreateTableCommand])
    }
  }

  test("duplicate columns in partitionBy in Create Cataloged Table As Select") {
    import spark.implicits._
    withTable("t", "t1") {
      spark.range(1).select('id as 'a, 'id as 'b).write.saveAsTable("t1")
      val errorQuery = "CREATE TABLE t SELECT a, b as a from t1"
      val correctQuery = "CREATE TABLE t SELECT a, b from t1"
      val e = intercept[AnalysisException] { sql(errorQuery) }
      assert(e.getMessage.contains("Found duplicate column(s) in table definition of `t`: `a`"))
      assert(
        sql(correctQuery).queryExecution.analyzed.isInstanceOf[CreateHiveTableAsSelectCommand])
    }
  }

}
