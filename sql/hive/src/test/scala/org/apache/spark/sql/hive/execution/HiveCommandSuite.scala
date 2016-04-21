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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveCommandSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
   protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(
      """
        |CREATE TABLE parquet_tab1 (c1 INT, c2 STRING)
        |USING org.apache.spark.sql.parquet.DefaultSource
      """.stripMargin)

     sql(
      """
        |CREATE EXTERNAL TABLE parquet_tab2 (c1 INT, c2 STRING)
        |STORED AS PARQUET
        |TBLPROPERTIES('prop1Key'="prop1Val", '`prop2Key`'="prop2Val")
      """.stripMargin)
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS parquet_tab1")
      sql("DROP TABLE IF EXISTS parquet_tab2")
    } finally {
      super.afterAll()
    }
  }

  test("show tables") {
    withTable("show1a", "show2b") {
      sql("CREATE TABLE show1a(c1 int)")
      sql("CREATE TABLE show2b(c2 int)")
      checkAnswer(
        sql("SHOW TABLES IN default 'show1*'"),
        Row("show1a", false) :: Nil)
      checkAnswer(
        sql("SHOW TABLES IN default 'show1*|show2*'"),
        Row("show1a", false) ::
          Row("show2b", false) :: Nil)
      checkAnswer(
        sql("SHOW TABLES 'show1*|show2*'"),
        Row("show1a", false) ::
          Row("show2b", false) :: Nil)
      assert(
        sql("SHOW TABLES").count() >= 2)
      assert(
        sql("SHOW TABLES IN default").count() >= 2)
    }
  }

  test("show tblproperties of data source tables - basic") {
    checkAnswer(
      sql("SHOW TBLPROPERTIES parquet_tab1")
        .filter(s"key = 'spark.sql.sources.provider'"),
      Row("spark.sql.sources.provider", "org.apache.spark.sql.parquet.DefaultSource") :: Nil
    )

    checkAnswer(
      sql("SHOW TBLPROPERTIES parquet_tab1(spark.sql.sources.provider)"),
      Row("org.apache.spark.sql.parquet.DefaultSource") :: Nil
    )

    checkAnswer(
      sql("SHOW TBLPROPERTIES parquet_tab1")
        .filter(s"key = 'spark.sql.sources.schema.numParts'"),
      Row("spark.sql.sources.schema.numParts", "1") :: Nil
    )

    checkAnswer(
      sql("SHOW TBLPROPERTIES parquet_tab1('spark.sql.sources.schema.numParts')"),
      Row("1"))
  }

  test("show tblproperties for datasource table - errors") {
    val message1 = intercept[AnalysisException] {
      sql("SHOW TBLPROPERTIES badtable")
    }.getMessage
    assert(message1.contains("Table or View badtable not found in database default"))

    // When key is not found, a row containing the error is returned.
    checkAnswer(
      sql("SHOW TBLPROPERTIES parquet_tab1('invalid.prop.key')"),
      Row("Table default.parquet_tab1 does not have property: invalid.prop.key") :: Nil
    )
  }

  test("show tblproperties for hive table") {
    checkAnswer(sql("SHOW TBLPROPERTIES parquet_tab2('prop1Key')"), Row("prop1Val"))
    checkAnswer(sql("SHOW TBLPROPERTIES parquet_tab2('`prop2Key`')"), Row("prop2Val"))
  }

  test("show tblproperties for spark temporary table - empty row") {
    withTempTable("parquet_temp") {
      sql(
        """
          |CREATE TEMPORARY TABLE parquet_temp (c1 INT, c2 STRING)
          |USING org.apache.spark.sql.parquet.DefaultSource
        """.stripMargin)

      // An empty sequence of row is returned for session temporary table.
      checkAnswer(sql("SHOW TBLPROPERTIES parquet_temp"), Nil)
    }
  }
}
