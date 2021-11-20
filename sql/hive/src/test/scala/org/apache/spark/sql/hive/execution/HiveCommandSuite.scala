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

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.fs.{FileContext, FsConstants, Path}

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.LoadDataCommand
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

class HiveCommandSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

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
    sql("CREATE TABLE parquet_tab4 (price int, qty int) partitioned by (year int, month int)")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2015, month = 1) SELECT 1, 1")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2015, month = 2) SELECT 2, 2")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2016, month = 2) SELECT 3, 3")
    sql("INSERT INTO parquet_tab4 PARTITION(year = 2016, month = 3) SELECT 3, 3")
    sql("CREATE VIEW parquet_view1 as select * from parquet_tab4")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS parquet_tab1")
      sql("DROP TABLE IF EXISTS parquet_tab2")
      sql("DROP VIEW IF EXISTS parquet_view1")
      sql("DROP TABLE IF EXISTS parquet_tab4")
    } finally {
      super.afterAll()
    }
  }

  test("show views") {
    withView("show1a", "show2b", "global_temp.temp1", "temp2") {
      sql("CREATE VIEW show1a AS SELECT 1 AS id")
      sql("CREATE VIEW show2b AS SELECT 1 AS id")
      sql("CREATE GLOBAL TEMP VIEW temp1 AS SELECT 1 AS id")
      sql("CREATE TEMP VIEW temp2 AS SELECT 1 AS id")
      checkAnswer(
        sql("SHOW VIEWS"),
        Row("default", "show1a", false) ::
          Row("default", "show2b", false) ::
          Row("default", "parquet_view1", false) ::
          Row("", "temp2", true) :: Nil)
      checkAnswer(
        sql("SHOW VIEWS IN default"),
        Row("default", "show1a", false) ::
          Row("default", "show2b", false) ::
          Row("default", "parquet_view1", false) ::
          Row("", "temp2", true) :: Nil)
      checkAnswer(
        sql("SHOW VIEWS FROM default"),
        Row("default", "show1a", false) ::
          Row("default", "show2b", false) ::
          Row("default", "parquet_view1", false) ::
          Row("", "temp2", true) :: Nil)
      checkAnswer(
        sql("SHOW VIEWS FROM global_temp"),
        Row("global_temp", "temp1", true) ::
          Row("", "temp2", true) :: Nil)
      checkAnswer(
        sql("SHOW VIEWS 'show1*|show2*'"),
        Row("default", "show1a", false) ::
          Row("default", "show2b", false) :: Nil)
      checkAnswer(
        sql("SHOW VIEWS LIKE 'show1*|show2*'"),
        Row("default", "show1a", false) ::
          Row("default", "show2b", false) :: Nil)
      checkAnswer(
        sql("SHOW VIEWS IN default 'show1*'"),
        Row("default", "show1a", false) :: Nil)
      checkAnswer(
        sql("SHOW VIEWS IN default LIKE 'show1*|show2*'"),
        Row("default", "show1a", false) ::
          Row("default", "show2b", false) :: Nil)
    }
  }

  Seq(true, false).foreach { local =>
    val loadQuery = if (local) "LOAD DATA LOCAL" else "LOAD DATA"
    test(loadQuery) {
      testLoadData(loadQuery, local)
    }
  }

  private def testLoadData(loadQuery: String, local: Boolean): Unit = {
    // employee.dat has two columns separated by '|', the first is an int, the second is a string.
    // Its content looks like:
    // 16|john
    // 17|robert
    val testData = hiveContext.getHiveFile("data/files/employee.dat").getCanonicalFile()

    /**
     * Run a function with a copy of the input data file when running with non-local input. The
     * semantics in this mode are that the input file is moved to the destination, so we have
     * to make a copy so that subsequent tests have access to the original file.
     */
    def withInputFile(fn: File => Unit): Unit = {
      if (local) {
        fn(testData)
      } else {
        val tmp = File.createTempFile(testData.getName(), ".tmp")
        Files.copy(testData, tmp)
        try {
          fn(tmp)
        } finally {
          tmp.delete()
        }
      }
    }

    withTable("non_part_table", "part_table") {
      sql(
        """
          |CREATE TABLE non_part_table (employeeID INT, employeeName STRING)
          |ROW FORMAT DELIMITED
          |FIELDS TERMINATED BY '|'
          |LINES TERMINATED BY '\n'
        """.stripMargin)

      // LOAD DATA INTO non-partitioned table can't specify partition
      intercept[AnalysisException] {
        sql(
          s"""$loadQuery INPATH "${testData.toURI}" INTO TABLE non_part_table PARTITION(ds="1")""")
      }

      withInputFile { path =>
        sql(s"""$loadQuery INPATH "${path.toURI}" INTO TABLE non_part_table""")

        // Non-local mode is expected to move the file, while local mode is expected to copy it.
        // Check once here that the behavior is the expected.
        assert(local === path.exists())
      }

      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Nil)

      // Incorrect URI.
      // file://path/to/data/files/employee.dat
      //
      // TODO: need a similar test for non-local mode.
      if (local) {
        val incorrectUri = "file://path/to/data/files/employee.dat"
        intercept[AnalysisException] {
          sql(s"""LOAD DATA LOCAL INPATH "$incorrectUri" INTO TABLE non_part_table""")
        }
      }

      // Use URI as inpath:
      // file:/path/to/data/files/employee.dat
      withInputFile { path =>
        sql(s"""$loadQuery INPATH "${path.toURI}" INTO TABLE non_part_table""")
      }

      checkAnswer(
        sql("SELECT * FROM non_part_table WHERE employeeID = 16"),
        Row(16, "john") :: Row(16, "john") :: Nil)

      // Overwrite existing data.
      withInputFile { path =>
        sql(s"""$loadQuery INPATH "${path.toURI}" OVERWRITE INTO TABLE non_part_table""")
      }

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
      withInputFile { f =>
        val path = f.toURI
        intercept[AnalysisException] {
          sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table""")
        }

        intercept[AnalysisException] {
          sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table PARTITION(c="1")""")
        }
        intercept[AnalysisException] {
          sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table PARTITION(d="1")""")
        }
        intercept[AnalysisException] {
          sql(s"""$loadQuery INPATH "$path" INTO TABLE part_table PARTITION(c="1", k="2")""")
        }
      }

      withInputFile { f =>
        sql(s"""$loadQuery INPATH "${f.toURI}" INTO TABLE part_table PARTITION(c="1", d="2")""")
      }
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '1' AND d = '2'"),
        sql("SELECT * FROM non_part_table").collect())

      // Different order of partition columns.
      withInputFile { f =>
        sql(s"""$loadQuery INPATH "${f.toURI}" INTO TABLE part_table PARTITION(d="1", c="2")""")
      }
      checkAnswer(
        sql("SELECT employeeID, employeeName FROM part_table WHERE c = '2' AND d = '1'"),
        sql("SELECT * FROM non_part_table"))
    }
  }

  test("SPARK-28084 case insensitive names of static partitioning in INSERT commands") {
    withTable("part_table") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql("CREATE TABLE part_table (price int, qty int) partitioned by (year int, month int)")
        sql("INSERT INTO part_table PARTITION(YEar = 2015, month = 1) SELECT 1, 1")
        checkAnswer(sql("SELECT * FROM part_table"), Row(1, 1, 2015, 1))
      }
    }
  }

  test("SPARK-28084 case insensitive names of dynamic partitioning in INSERT commands") {
    withTable("part_table") {
      withSQLConf(
        SQLConf.CASE_SENSITIVE.key -> "false",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql("CREATE TABLE part_table (price int) partitioned by (year int)")
        sql("INSERT INTO part_table PARTITION(YEar) SELECT 1, 2019")
        checkAnswer(sql("SELECT * FROM part_table"), Row(1, 2019))
      }
    }
  }

  test("SPARK-25918: LOAD DATA LOCAL INPATH should handle a relative path") {
    val localFS = FileContext.getLocalFSFileContext()
    val workingDir = localFS.getWorkingDirectory
    val r = LoadDataCommand.makeQualified(
      FsConstants.LOCAL_FS_URI, workingDir, new Path("kv1.txt"))
    assert(r === new Path(s"$workingDir/kv1.txt"))
  }
}
