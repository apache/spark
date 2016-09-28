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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

class HiveCommandSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    // Use catalog to create table instead of SQL string here, because we don't support specifying
    // table properties for data source table with SQL API now.
    hiveContext.sessionState.catalog.createTable(
      CatalogTable(
        identifier = TableIdentifier("parquet_tab1"),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType().add("c1", "int").add("c2", "string"),
        provider = Some("parquet"),
        properties = Map("my_key1" -> "v1")
      ),
      ignoreIfExists = false
    )

    sql(
      """
        |CREATE TABLE parquet_tab2 (c1 INT, c2 STRING)
        |STORED AS PARQUET
        |TBLPROPERTIES('prop1Key'="prop1Val", '`prop2Key`'="prop2Val")
      """.stripMargin)
    sql("CREATE TABLE parquet_tab3(col1 int, `col 2` int)")
    sql("CREATE TABLE parquet_tab4 (price int, qty int) partitioned by (year int, month int)")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2015, month = 1) SELECT 1, 1")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2015, month = 2) SELECT 2, 2")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2016, month = 2) SELECT 3, 3")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2016, month = 3) SELECT 3, 3")
    sql(
      """
        |CREATE TABLE parquet_tab5 (price int, qty int)
        |PARTITIONED BY (year int, month int, hour int, minute int, sec int, extra int)
      """.stripMargin)
    sql(
      """
        |INSERT INTO parquet_tab5
        |PARTITION(year = 2016, month = 3, hour = 10, minute = 10, sec = 10, extra = 1) SELECT 3, 3
      """.stripMargin)
    sql(
      """
        |INSERT INTO parquet_tab5
        |PARTITION(year = 2016, month = 4, hour = 10, minute = 10, sec = 10, extra = 1) SELECT 3, 3
      """.stripMargin)
    sql("CREATE VIEW parquet_view1 as select * from parquet_tab4")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS parquet_tab1")
      sql("DROP TABLE IF EXISTS parquet_tab2")
      sql("DROP TABLE IF EXISTS parquet_tab3")
      sql("DROP VIEW IF EXISTS parquet_view1")
      sql("DROP TABLE IF EXISTS parquet_tab4")
      sql("DROP TABLE IF EXISTS parquet_tab5")
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
      sql("SHOW TBLPROPERTIES parquet_tab1").filter(s"key = 'my_key1'"),
      Row("my_key1", "v1") :: Nil
    )

    checkAnswer(
      sql(s"SHOW TBLPROPERTIES parquet_tab1('my_key1')"),
      Row("v1") :: Nil
    )
  }

  test("show tblproperties for datasource table - errors") {
    val message1 = intercept[NoSuchTableException] {
      sql("SHOW TBLPROPERTIES badtable")
    }.getMessage
    assert(message1.contains("Table or view 'badtable' not found in database 'default'"))

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
    withTempView("parquet_temp") {
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
    }
  }

  test("Truncate Table") {
    withTable("non_part_table", "part_table") {
      sql(
        """
          |CREATE TABLE non_part_table (employeeID INT, employeeName STRING)
          |ROW FORMAT DELIMITED
          |FIELDS TERMINATED BY '|'
          |LINES TERMINATED BY '\n'
        """.stripMargin)

      val testData = hiveContext.getHiveFile("data/files/employee.dat").getCanonicalPath

      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE non_part_table""")
      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Nil)

      val testResults = sql("SELECT * FROM non_part_table").collect()

      sql("TRUNCATE TABLE non_part_table")
      checkAnswer(sql("SELECT * FROM non_part_table"), Seq.empty[Row])

      sql(
        """
          |CREATE TABLE part_table (employeeID INT, employeeName STRING)
          |PARTITIONED BY (c STRING, d STRING)
          |ROW FORMAT DELIMITED
          |FIELDS TERMINATED BY '|'
          |LINES TERMINATED BY '\n'
        """.stripMargin)

      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(c="1", d="1")""")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1' AND d = '1'"),
        testResults)

      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(c="1", d="2")""")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1' AND d = '2'"),
        testResults)

      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE part_table PARTITION(c="2", d="2")""")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '2' AND d = '2'"),
        testResults)

      sql("TRUNCATE TABLE part_table PARTITION(c='1', d='1')")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1' AND d = '1'"),
        Seq.empty[Row])
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1' AND d = '2'"),
        testResults)

      sql("TRUNCATE TABLE part_table PARTITION(c='1')")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1'"),
        Seq.empty[Row])

      sql("TRUNCATE TABLE part_table")
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table"),
        Seq.empty[Row])
    }
  }

  test("show columns") {
    checkAnswer(
      sql("SHOW COLUMNS IN parquet_tab3"),
      Row("col1") :: Row("col 2") :: Nil)

    checkAnswer(
      sql("SHOW COLUMNS IN default.parquet_tab3"),
      Row("col1") :: Row("col 2") :: Nil)

    checkAnswer(
      sql("SHOW COLUMNS IN parquet_tab3 FROM default"),
      Row("col1") :: Row("col 2") :: Nil)

    checkAnswer(
      sql("SHOW COLUMNS IN parquet_tab4 IN default"),
      Row("price") :: Row("qty") :: Row("year") :: Row("month") :: Nil)

    val message = intercept[NoSuchTableException] {
      sql("SHOW COLUMNS IN badtable FROM default")
    }.getMessage
    assert(message.contains("'badtable' not found in database"))
  }

  test("show partitions - show everything") {
    checkAnswer(
      sql("show partitions parquet_tab4"),
      Row("year=2015/month=1") ::
        Row("year=2015/month=2") ::
        Row("year=2016/month=2") ::
        Row("year=2016/month=3") :: Nil)

    checkAnswer(
      sql("show partitions default.parquet_tab4"),
      Row("year=2015/month=1") ::
        Row("year=2015/month=2") ::
        Row("year=2016/month=2") ::
        Row("year=2016/month=3") :: Nil)
  }

  test("show partitions - show everything more than 5 part keys") {
    checkAnswer(
      sql("show partitions parquet_tab5"),
      Row("year=2016/month=3/hour=10/minute=10/sec=10/extra=1") ::
        Row("year=2016/month=4/hour=10/minute=10/sec=10/extra=1") :: Nil)
  }

  test("show partitions - filter") {
    checkAnswer(
      sql("show partitions default.parquet_tab4 PARTITION(year=2015)"),
      Row("year=2015/month=1") ::
        Row("year=2015/month=2") :: Nil)

    checkAnswer(
      sql("show partitions default.parquet_tab4 PARTITION(year=2015, month=1)"),
      Row("year=2015/month=1") :: Nil)

    checkAnswer(
      sql("show partitions default.parquet_tab4 PARTITION(month=2)"),
      Row("year=2015/month=2") ::
        Row("year=2016/month=2") :: Nil)
  }

  test("show partitions - empty row") {
    withTempView("parquet_temp") {
      sql(
        """
          |CREATE TEMPORARY TABLE parquet_temp (c1 INT, c2 STRING)
          |USING org.apache.spark.sql.parquet.DefaultSource
        """.stripMargin)
      // An empty sequence of row is returned for session temporary table.
      intercept[NoSuchTableException] {
        sql("SHOW PARTITIONS parquet_temp")
      }

      val message1 = intercept[AnalysisException] {
        sql("SHOW PARTITIONS parquet_tab3")
      }.getMessage
      assert(message1.contains("not allowed on a table that is not partitioned"))

      val message2 = intercept[AnalysisException] {
        sql("SHOW PARTITIONS parquet_tab4 PARTITION(abcd=2015, xyz=1)")
      }.getMessage
      assert(message2.contains("Non-partitioning column(s) [abcd, xyz] are specified"))

      val message3 = intercept[AnalysisException] {
        sql("SHOW PARTITIONS parquet_view1")
      }.getMessage
      assert(message3.contains("is not allowed on a view"))
    }
  }

  test("show partitions - datasource") {
    withTable("part_datasrc") {
      val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
      df.write
        .partitionBy("a")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("part_datasrc")

      val message1 = intercept[AnalysisException] {
        sql("SHOW PARTITIONS part_datasrc")
      }.getMessage
      assert(message1.contains("is not allowed on a datasource table"))
    }
  }
}
