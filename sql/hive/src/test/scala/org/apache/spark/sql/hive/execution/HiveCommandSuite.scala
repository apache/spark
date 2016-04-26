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

  test("LOAD DATA") {
    withTable("non_part_table", "part_table") {
      sql(
        """
          |CREATE TABLE non_part_table (employeeID INT, employeeName STRING)
          |ROW FORMAT DELIMITED
          |FIELDS TERMINATED BY '|'
          |LINES TERMINATED BY '\n'
        """.stripMargin)

      // employee.dat has two columns separated by '|', the first is an int, the second is a string.
      // Its content looks like:
      // 16|john
      // 17|robert
      val testData = hiveContext.getHiveFile("data/files/employee.dat").getCanonicalPath

      // LOAD DATA INTO non-partitioned table can't specify partition
      intercept[AnalysisException] {
        sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE non_part_table PARTITION(ds="1")""")
      }

      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE non_part_table""")
      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Nil)

      sql(
        """
          |CREATE TABLE part_table (employeeID INT, employeeName STRING)
          |PARTITIONED BY (c STRING, d STRING)
          |ROW FORMAT DELIMITED
          |FIELDS TERMINATED BY '|'
          |LINES TERMINATED BY '\n'
        """.stripMargin)

      // LOAD DATA INTO partitioned table must specify partition
      intercept[AnalysisException] {
        sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table""")
      }

      intercept[AnalysisException] {
        sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(c="1")""")
      }
      intercept[AnalysisException] {
        sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(d="1")""")
      }
      intercept[AnalysisException] {
        sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(c="1", k="2")""")
      }

      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(c="1", d="2")""")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1' AND d = '2'"),
        sql("SELECT * FROM non_part_table").collect())

      // Different order of partition columns.
      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(d="1", c="2")""")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '2' AND d = '1'"),
        sql("SELECT * FROM non_part_table").collect())
    }
  }

  test("LOAD DATA: input path") {
    withTable("non_part_table") {
      sql(
        """
          |CREATE TABLE non_part_table (employeeID INT, employeeName STRING)
          |ROW FORMAT DELIMITED
          |FIELDS TERMINATED BY '|'
          |LINES TERMINATED BY '\n'
        """.stripMargin)

      // Non-existing inpath
      intercept[AnalysisException] {
        sql("""LOAD DATA LOCAL INPATH "/non-existing/data.txt" INTO TABLE non_part_table""")
      }

      val testData = hiveContext.getHiveFile("data/files/employee.dat").getCanonicalPath

      // Non-local inpath: without URI Scheme and Authority
      sql(s"""LOAD DATA INPATH "$testData" INTO TABLE non_part_table""")
      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Nil)

      // Use URI as LOCAL inpath:
      // file:/path/to/data/files/employee.dat
      val uri = "file:" + testData
      sql(s"""LOAD DATA LOCAL INPATH "$uri" INTO TABLE non_part_table""")

      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Row(16, "john") :: Nil)

      // Use URI as non-LOCAL inpath
      sql(s"""LOAD DATA INPATH "$uri" INTO TABLE non_part_table""")

      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Row(16, "john") :: Row(16, "john") :: Nil)

      sql(s"""LOAD DATA INPATH "$uri" OVERWRITE INTO TABLE non_part_table""")

      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Nil)

      // Incorrect URI:
      // file://path/to/data/files/employee.dat
      val incorrectUri = "file:/" + testData
      intercept[AnalysisException] {
        sql(s"""LOAD DATA LOCAL INPATH "$incorrectUri" INTO TABLE non_part_table""")
      }

      // Unset default URI Scheme and Authority: throw exception
      val originalFsName = hiveContext.sparkContext.hadoopConfiguration.get("fs.default.name")
      hiveContext.sparkContext.hadoopConfiguration.unset("fs.default.name")
      intercept[AnalysisException] {
        sql(s"""LOAD DATA INPATH "$testData" INTO TABLE non_part_table""")
      }
      hiveContext.sparkContext.hadoopConfiguration.set("fs.default.name", originalFsName)
    }
  }
}
