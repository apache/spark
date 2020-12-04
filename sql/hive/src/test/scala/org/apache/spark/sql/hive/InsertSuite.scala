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

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, _}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

case class TestData(key: Int, value: String)

case class ThreeCloumntable(key: Int, value: String, key1: String)

class InsertSuite extends QueryTest with TestHiveSingleton with BeforeAndAfter
    with SQLTestUtils  with PrivateMethodTester  {
  import spark.implicits._

  override lazy val testData = spark.sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i.toString))).toDF()

  before {
    // Since every we are doing tests for DDL statements,
    // it is better to reset before every test.
    hiveContext.reset()
    // Creates a temporary view with testData, which will be used in all tests.
    testData.createOrReplaceTempView("testData")
  }

  test("insertInto() HiveTable") {
    withTable("createAndInsertTest") {
      sql("CREATE TABLE createAndInsertTest (key int, value string)")

      // Add some data.
      testData.write.mode(SaveMode.Append).insertInto("createAndInsertTest")

      // Make sure the table has also been updated.
      checkAnswer(
        sql("SELECT * FROM createAndInsertTest"),
        testData.collect().toSeq
      )

      // Add more data.
      testData.write.mode(SaveMode.Append).insertInto("createAndInsertTest")

      // Make sure the table has been updated.
      checkAnswer(
        sql("SELECT * FROM createAndInsertTest"),
        testData.toDF().collect().toSeq ++ testData.toDF().collect().toSeq
      )

      // Now overwrite.
      testData.write.mode(SaveMode.Overwrite).insertInto("createAndInsertTest")

      // Make sure the registered table has also been updated.
      checkAnswer(
        sql("SELECT * FROM createAndInsertTest"),
        testData.collect().toSeq
      )
    }
  }

  test("Double create fails when allowExisting = false") {
    withTable("doubleCreateAndInsertTest") {
      sql("CREATE TABLE doubleCreateAndInsertTest (key int, value string)")

      intercept[AnalysisException] {
        sql("CREATE TABLE doubleCreateAndInsertTest (key int, value string)")
      }
    }
  }

  test("Double create does not fail when allowExisting = true") {
    withTable("doubleCreateAndInsertTest") {
      sql("CREATE TABLE doubleCreateAndInsertTest (key int, value string)")
      sql("CREATE TABLE IF NOT EXISTS doubleCreateAndInsertTest (key int, value string)")
    }
  }

  test("SPARK-4052: scala.collection.Map as value type of MapType") {
    val schema = StructType(StructField("m", MapType(StringType, StringType), true) :: Nil)
    val rowRDD = spark.sparkContext.parallelize(
      (1 to 100).map(i => Row(scala.collection.mutable.HashMap(s"key$i" -> s"value$i"))))
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("tableWithMapValue")
    sql("CREATE TABLE hiveTableWithMapValue(m MAP <STRING, STRING>)")
    sql("INSERT OVERWRITE TABLE hiveTableWithMapValue SELECT m FROM tableWithMapValue")

    checkAnswer(
      sql("SELECT * FROM hiveTableWithMapValue"),
      rowRDD.collect().toSeq
    )

    sql("DROP TABLE hiveTableWithMapValue")
  }

  test("SPARK-4203:random partition directory order") {
    sql("CREATE TABLE tmp_table (key int, value string)")
    val tmpDir = Utils.createTempDir()
    // The default value of hive.exec.stagingdir.
    val stagingDir = ".hive-staging"

    sql(
      s"""
         |CREATE TABLE table_with_partition(c1 string)
         |PARTITIONED by (p1 string,p2 string,p3 string,p4 string,p5 string)
         |location '${tmpDir.toURI.toString}'
        """.stripMargin)
    sql(
      """
        |INSERT OVERWRITE TABLE table_with_partition
        |partition (p1='a',p2='b',p3='c',p4='c',p5='1')
        |SELECT 'blarr' FROM tmp_table
      """.stripMargin)
    sql(
      """
        |INSERT OVERWRITE TABLE table_with_partition
        |partition (p1='a',p2='b',p3='c',p4='c',p5='2')
        |SELECT 'blarr' FROM tmp_table
      """.stripMargin)
    sql(
      """
        |INSERT OVERWRITE TABLE table_with_partition
        |partition (p1='a',p2='b',p3='c',p4='c',p5='3')
        |SELECT 'blarr' FROM tmp_table
      """.stripMargin)
    sql(
      """
        |INSERT OVERWRITE TABLE table_with_partition
        |partition (p1='a',p2='b',p3='c',p4='c',p5='4')
        |SELECT 'blarr' FROM tmp_table
      """.stripMargin)
    def listFolders(path: File, acc: List[String]): List[List[String]] = {
      val dir = path.listFiles()
      val folders = dir.filter { e => e.isDirectory && !e.getName().startsWith(stagingDir) }.toList
      if (folders.isEmpty) {
        List(acc.reverse)
      } else {
        folders.flatMap(x => listFolders(x, x.getName :: acc))
      }
    }
    val expected = List(
      "p1=a"::"p2=b"::"p3=c"::"p4=c"::"p5=2"::Nil,
      "p1=a"::"p2=b"::"p3=c"::"p4=c"::"p5=3"::Nil,
      "p1=a"::"p2=b"::"p3=c"::"p4=c"::"p5=1"::Nil,
      "p1=a"::"p2=b"::"p3=c"::"p4=c"::"p5=4"::Nil
    )
    assert(listFolders(tmpDir, List()).sortBy(_.toString()) === expected.sortBy(_.toString))
    sql("DROP TABLE table_with_partition")
    sql("DROP TABLE tmp_table")
  }

  testPartitionedTable("INSERT OVERWRITE - partition IF NOT EXISTS") { tableName =>
    val selQuery = s"select a, b, c, d from $tableName"
    sql(
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |partition (b=2, c=3)
         |SELECT 1, 4
        """.stripMargin)
    checkAnswer(sql(selQuery), Row(1, 2, 3, 4))

    sql(
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |partition (b=2, c=3)
         |SELECT 5, 6
        """.stripMargin)
    checkAnswer(sql(selQuery), Row(5, 2, 3, 6))

    val e = intercept[AnalysisException] {
      sql(
        s"""
           |INSERT OVERWRITE TABLE $tableName
           |partition (b=2, c) IF NOT EXISTS
           |SELECT 7, 8, 3
          """.stripMargin)
    }
    assert(e.getMessage.contains("IF NOT EXISTS with dynamic partitions: c"))

    // If the partition already exists, the insert will overwrite the data
    // unless users specify IF NOT EXISTS
    sql(
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |partition (b=2, c=3) IF NOT EXISTS
         |SELECT 9, 10
        """.stripMargin)
    checkAnswer(sql(selQuery), Row(5, 2, 3, 6))

    // ADD PARTITION has the same effect, even if no actual data is inserted.
    sql(s"ALTER TABLE $tableName ADD PARTITION (b=21, c=31)")
    sql(
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |partition (b=21, c=31) IF NOT EXISTS
         |SELECT 20, 24
        """.stripMargin)
    checkAnswer(sql(selQuery), Row(5, 2, 3, 6))
  }

  test("Insert ArrayType.containsNull == false") {
    val schema = StructType(Seq(
      StructField("a", ArrayType(StringType, containsNull = false))))
    val rowRDD = spark.sparkContext.parallelize((1 to 100).map(i => Row(Seq(s"value$i"))))
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("tableWithArrayValue")
    sql("CREATE TABLE hiveTableWithArrayValue(a Array <STRING>)")
    sql("INSERT OVERWRITE TABLE hiveTableWithArrayValue SELECT a FROM tableWithArrayValue")

    checkAnswer(
      sql("SELECT * FROM hiveTableWithArrayValue"),
      rowRDD.collect().toSeq)

    sql("DROP TABLE hiveTableWithArrayValue")
  }

  test("Insert MapType.valueContainsNull == false") {
    val schema = StructType(Seq(
      StructField("m", MapType(StringType, StringType, valueContainsNull = false))))
    val rowRDD = spark.sparkContext.parallelize(
      (1 to 100).map(i => Row(Map(s"key$i" -> s"value$i"))))
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("tableWithMapValue")
    sql("CREATE TABLE hiveTableWithMapValue(m Map <STRING, STRING>)")
    sql("INSERT OVERWRITE TABLE hiveTableWithMapValue SELECT m FROM tableWithMapValue")

    checkAnswer(
      sql("SELECT * FROM hiveTableWithMapValue"),
      rowRDD.collect().toSeq)

    sql("DROP TABLE hiveTableWithMapValue")
  }

  test("Insert StructType.fields.exists(_.nullable == false)") {
    val schema = StructType(Seq(
      StructField("s", StructType(Seq(StructField("f", StringType, nullable = false))))))
    val rowRDD = spark.sparkContext.parallelize(
      (1 to 100).map(i => Row(Row(s"value$i"))))
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("tableWithStructValue")
    sql("CREATE TABLE hiveTableWithStructValue(s Struct <f: STRING>)")
    sql("INSERT OVERWRITE TABLE hiveTableWithStructValue SELECT s FROM tableWithStructValue")

    checkAnswer(
      sql("SELECT * FROM hiveTableWithStructValue"),
      rowRDD.collect().toSeq)

    sql("DROP TABLE hiveTableWithStructValue")
  }

  test("Test partition mode = strict") {
    withSQLConf(("hive.exec.dynamic.partition.mode", "strict")) {
      withTable("partitioned") {
        sql("CREATE TABLE partitioned (id bigint, data string) USING hive " +
          "PARTITIONED BY (part string)")
        val data = (1 to 10).map(i => (i, s"data-$i", if ((i % 2) == 0) "even" else "odd"))
          .toDF("id", "data", "part")

        intercept[SparkException] {
          data.write.insertInto("partitioned")
        }
      }
    }
  }

  test("Detect table partitioning") {
    withSQLConf(("hive.exec.dynamic.partition.mode", "nonstrict")) {
      withTable("source", "partitioned") {
        sql("CREATE TABLE source (id bigint, data string, part string)")
        val data = (1 to 10).map(i => (i, s"data-$i", if ((i % 2) == 0) "even" else "odd")).toDF()

        data.write.insertInto("source")
        checkAnswer(sql("SELECT * FROM source"), data.collect().toSeq)

        sql("CREATE TABLE partitioned (id bigint, data string) PARTITIONED BY (part string)")
        // this will pick up the output partitioning from the table definition
        spark.table("source").write.insertInto("partitioned")

        checkAnswer(sql("SELECT * FROM partitioned"), data.collect().toSeq)
      }
    }
  }

  private def testPartitionedHiveSerDeTable(testName: String)(f: String => Unit): Unit = {
    test(s"Hive SerDe table - $testName") {
      val hiveTable = "hive_table"

      withTable(hiveTable) {
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          sql(
            s"""
              |CREATE TABLE $hiveTable (a INT, d INT)
              |PARTITIONED BY (b INT, c INT) STORED AS TEXTFILE
            """.stripMargin)
          f(hiveTable)
        }
      }
    }
  }

  private def testPartitionedDataSourceTable(testName: String)(f: String => Unit): Unit = {
    test(s"Data source table - $testName") {
      val dsTable = "ds_table"

      withTable(dsTable) {
        sql(
          s"""
             |CREATE TABLE $dsTable (a INT, b INT, c INT, d INT)
             |USING PARQUET PARTITIONED BY (b, c)
           """.stripMargin)
        f(dsTable)
      }
    }
  }

  private def testPartitionedTable(testName: String)(f: String => Unit): Unit = {
    testPartitionedHiveSerDeTable(testName)(f)
    testPartitionedDataSourceTable(testName)(f)
  }

  testPartitionedTable("partitionBy() can't be used together with insertInto()") { tableName =>
    val cause = intercept[AnalysisException] {
      Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d").write.partitionBy("b", "c").insertInto(tableName)
    }

    assert(cause.getMessage.contains("insertInto() can't be used together with partitionBy()."))
  }

  testPartitionedTable(
    "SPARK-16036: better error message when insert into a table with mismatch schema") {
    tableName =>
      val e = intercept[AnalysisException] {
        sql(s"INSERT INTO TABLE $tableName PARTITION(b=1, c=2) SELECT 1, 2, 3")
      }
      assert(e.message.contains(
        "target table has 4 column(s) but the inserted data has 5 column(s)"))
  }

  testPartitionedTable("SPARK-16037: INSERT statement should match columns by position") {
    tableName =>
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql(s"INSERT INTO TABLE $tableName SELECT 1, 4, 2 AS c, 3 AS b")
        checkAnswer(sql(s"SELECT a, b, c, d FROM $tableName"), Row(1, 2, 3, 4))
        sql(s"INSERT OVERWRITE TABLE $tableName SELECT 1, 4, 2, 3")
        checkAnswer(sql(s"SELECT a, b, c, 4 FROM $tableName"), Row(1, 2, 3, 4))
      }
  }

  testPartitionedTable("INSERT INTO a partitioned table (semantic and error handling)") {
    tableName =>
      withSQLConf(("hive.exec.dynamic.partition.mode", "nonstrict")) {
        sql(s"INSERT INTO TABLE $tableName PARTITION (b=2, c=3) SELECT 1, 4")

        sql(s"INSERT INTO TABLE $tableName PARTITION (b=6, c=7) SELECT 5, 8")

        sql(s"INSERT INTO TABLE $tableName PARTITION (c=11, b=10) SELECT 9, 12")

        // c is defined twice. Analyzer will complain.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (b=14, c=15, c=16) SELECT 13")
        }

        // d is not a partitioning column.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (b=14, c=15, d=16) SELECT 13, 14")
        }

        // d is not a partitioning column. The total number of columns is correct.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (b=14, c=15, d=16) SELECT 13")
        }

        // The data is missing a column.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (c=15, b=16) SELECT 13")
        }

        // d is not a partitioning column.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (b=15, d=15) SELECT 13, 14")
        }

        // The statement is missing a column.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (b=15) SELECT 13, 14")
        }

        // The statement is missing a column.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (b=15) SELECT 13, 14, 16")
        }

        sql(s"INSERT INTO TABLE $tableName PARTITION (b=14, c) SELECT 13, 16, 15")

        // Dynamic partitioning columns need to be after static partitioning columns.
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName PARTITION (b, c=19) SELECT 17, 20, 18")
        }

        sql(s"INSERT INTO TABLE $tableName PARTITION (b, c) SELECT 17, 20, 18, 19")

        sql(s"INSERT INTO TABLE $tableName PARTITION (c, b) SELECT 21, 24, 22, 23")

        sql(s"INSERT INTO TABLE $tableName SELECT 25, 28, 26, 27")

        checkAnswer(
          sql(s"SELECT a, b, c, d FROM $tableName"),
          Row(1, 2, 3, 4) ::
            Row(5, 6, 7, 8) ::
            Row(9, 10, 11, 12) ::
            Row(13, 14, 15, 16) ::
            Row(17, 18, 19, 20) ::
            Row(21, 22, 23, 24) ::
            Row(25, 26, 27, 28) :: Nil
        )
      }
  }

  testPartitionedTable("insertInto() should match columns by position and ignore column names") {
    tableName =>
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        // Columns `df.c` and `df.d` are resolved by position, and thus mapped to partition columns
        // `b` and `c` of the target table.
        val df = Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d")
        df.write.insertInto(tableName)

        checkAnswer(
          sql(s"SELECT a, b, c, d FROM $tableName"),
          Row(1, 3, 4, 2)
        )
      }
  }

  testPartitionedTable("insertInto() should match unnamed columns by position") {
    tableName =>
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        // Columns `c + 1` and `d + 1` are resolved by position, and thus mapped to partition
        // columns `b` and `c` of the target table.
        val df = Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d")
        df.select($"a" + 1, $"b" + 1, $"c" + 1, $"d" + 1).write.insertInto(tableName)

        checkAnswer(
          sql(s"SELECT a, b, c, d FROM $tableName"),
          Row(2, 4, 5, 3)
        )
      }
  }

  testPartitionedTable("insertInto() should reject missing columns") {
    tableName =>
      withTable("t") {
        sql("CREATE TABLE t (a INT, b INT)")

        intercept[AnalysisException] {
          spark.table("t").write.insertInto(tableName)
        }
      }
  }

  testPartitionedTable("insertInto() should reject extra columns") {
    tableName =>
      withTable("t") {
        sql("CREATE TABLE t (a INT, b INT, c INT, d INT, e INT)")

        intercept[AnalysisException] {
          spark.table("t").write.insertInto(tableName)
        }
      }
  }

  private def testBucketedTable(testName: String)(f: String => Unit): Unit = {
    test(s"Hive SerDe table - $testName") {
      val hiveTable = "hive_table"

      withTable(hiveTable) {
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          sql(
            s"""
               |CREATE TABLE $hiveTable (a INT, d INT)
               |PARTITIONED BY (b INT, c INT)
               |CLUSTERED BY(a)
               |SORTED BY(a, d) INTO 256 BUCKETS
               |STORED AS TEXTFILE
            """.stripMargin)
          f(hiveTable)
        }
      }
    }
  }

  testBucketedTable("INSERT should NOT fail if strict bucketing is NOT enforced") {
    tableName =>
      withSQLConf("hive.enforce.bucketing" -> "false", "hive.enforce.sorting" -> "false") {
        sql(s"INSERT INTO TABLE $tableName SELECT 1, 4, 2 AS c, 3 AS b")
        checkAnswer(sql(s"SELECT a, b, c, d FROM $tableName"), Row(1, 2, 3, 4))
      }
  }

  testBucketedTable("INSERT should fail if strict bucketing / sorting is enforced") {
    tableName =>
      withSQLConf("hive.enforce.bucketing" -> "true", "hive.enforce.sorting" -> "false") {
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName SELECT 1, 2, 3, 4")
        }
      }
      withSQLConf("hive.enforce.bucketing" -> "false", "hive.enforce.sorting" -> "true") {
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName SELECT 1, 2, 3, 4")
        }
      }
      withSQLConf("hive.enforce.bucketing" -> "true", "hive.enforce.sorting" -> "true") {
        intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $tableName SELECT 1, 2, 3, 4")
        }
      }
  }

  test("SPARK-20594: hive.exec.stagingdir was deleted by Hive") {
    // Set hive.exec.stagingdir under the table directory without start with ".".
    withSQLConf("hive.exec.stagingdir" -> "./test") {
      withTable("test_table") {
        sql("CREATE TABLE test_table (key int)")
        sql("INSERT OVERWRITE TABLE test_table SELECT 1")
        checkAnswer(sql("SELECT * FROM test_table"), Row(1))
      }
    }
  }

  test("SPARK-27552: hive.exec.stagingdir is invalid on Windows OS") {
    val conf = spark.sessionState.newHadoopConf()
    val inputPath = new Path("/tmp/b/c")
    var stagingDir = "tmp/b"
    val saveHiveFile = InsertIntoHiveTable(null, Map.empty, null, false, false, null)
    val getStagingDir = PrivateMethod[Path](Symbol("getStagingDir"))
    var path = saveHiveFile invokePrivate getStagingDir(inputPath, conf, stagingDir)
    assert(path.toString.indexOf("/tmp/b_hive_") != -1)

    stagingDir = "tmp/b/c"
    path = saveHiveFile invokePrivate getStagingDir(inputPath, conf, stagingDir)
    assert(path.toString.indexOf("/tmp/b/c/.hive-staging_hive_") != -1)

    stagingDir = "d/e"
    path = saveHiveFile invokePrivate getStagingDir(inputPath, conf, stagingDir)
    assert(path.toString.indexOf("/tmp/b/c/.hive-staging_hive_") != -1)

    stagingDir = ".d/e"
    path = saveHiveFile invokePrivate getStagingDir(inputPath, conf, stagingDir)
    assert(path.toString.indexOf("/tmp/b/c/.d/e_hive_") != -1)

    stagingDir = "/tmp/c/"
    path = saveHiveFile invokePrivate getStagingDir(inputPath, conf, stagingDir)
    assert(path.toString.indexOf("/tmp/c_hive_") != -1)
  }

  test("insert overwrite to dir from hive metastore table") {
    withTempDir { dir =>
      val path = dir.toURI.getPath

      sql(s"INSERT OVERWRITE LOCAL DIRECTORY '${path}' SELECT * FROM src where key < 10")

      sql(
        s"""
           |INSERT OVERWRITE LOCAL DIRECTORY '${path}'
           |STORED AS orc
           |SELECT * FROM src where key < 10
         """.stripMargin)

      // use orc data source to check the data of path is right.
      withTempView("orc_source") {
        sql(
          s"""
             |CREATE TEMPORARY VIEW orc_source
             |USING org.apache.spark.sql.hive.orc
             |OPTIONS (
             |  PATH '${dir.getCanonicalPath}'
             |)
           """.stripMargin)

        checkAnswer(
          sql("select * from orc_source"),
          sql("select * from src where key < 10"))
      }
    }
  }

  test("insert overwrite to local dir from temp table") {
    withTempView("test_insert_table") {
      spark.range(10).selectExpr("id", "id AS str").createOrReplaceTempView("test_insert_table")

      withTempDir { dir =>
        val path = dir.toURI.getPath

        sql(
          s"""
             |INSERT OVERWRITE LOCAL DIRECTORY '${path}'
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             |SELECT * FROM test_insert_table
           """.stripMargin)

        sql(
          s"""
             |INSERT OVERWRITE LOCAL DIRECTORY '${path}'
             |STORED AS orc
             |SELECT * FROM test_insert_table
           """.stripMargin)

        // use orc data source to check the data of path is right.
        checkAnswer(
          spark.read.orc(dir.getCanonicalPath),
          sql("select * from test_insert_table"))
      }
    }
  }

  test("insert overwrite to dir from temp table") {
    withTempView("test_insert_table") {
      spark.range(10).selectExpr("id", "id AS str").createOrReplaceTempView("test_insert_table")

      withTempDir { dir =>
        val pathUri = dir.toURI

        sql(
          s"""
             |INSERT OVERWRITE DIRECTORY '${pathUri}'
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             |SELECT * FROM test_insert_table
           """.stripMargin)

        sql(
          s"""
             |INSERT OVERWRITE DIRECTORY '${pathUri}'
             |STORED AS orc
             |SELECT * FROM test_insert_table
           """.stripMargin)

        // use orc data source to check the data of path is right.
        checkAnswer(
          spark.read.orc(dir.getCanonicalPath),
          sql("select * from test_insert_table"))
      }
    }
  }

  test("multi insert overwrite to dir") {
    withTempView("test_insert_table") {
      spark.range(10).selectExpr("id", "id AS str").createOrReplaceTempView("test_insert_table")

      withTempDir { dir =>
        val pathUri = dir.toURI

        withTempDir { dir2 =>
          val pathUri2 = dir2.toURI

          sql(
            s"""
               |FROM test_insert_table
               |INSERT OVERWRITE DIRECTORY '${pathUri}'
               |STORED AS orc
               |SELECT id
               |INSERT OVERWRITE DIRECTORY '${pathUri2}'
               |STORED AS orc
               |SELECT *
             """.stripMargin)

          // use orc data source to check the data of path is right.
          checkAnswer(
            spark.read.orc(dir.getCanonicalPath),
            sql("select id from test_insert_table"))

          checkAnswer(
            spark.read.orc(dir2.getCanonicalPath),
            sql("select * from test_insert_table"))
        }
      }
    }
  }

  test("insert overwrite to dir to illegal path") {
    withTempView("test_insert_table") {
      spark.range(10).selectExpr("id", "id AS str").createOrReplaceTempView("test_insert_table")

      val e = intercept[IllegalArgumentException] {
        sql(
          s"""
             |INSERT OVERWRITE LOCAL DIRECTORY 'abc://a'
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             |SELECT * FROM test_insert_table
           """.stripMargin)
      }.getMessage

      assert(e.contains("Wrong FS: abc://a, expected: file:///"))
    }
  }

  test("insert overwrite to dir with mixed syntax") {
    withTempView("test_insert_table") {
      spark.range(10).selectExpr("id", "id AS str").createOrReplaceTempView("test_insert_table")

      val e = intercept[ParseException] {
        sql(
          s"""
             |INSERT OVERWRITE DIRECTORY 'file://tmp'
             |USING json
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             |SELECT * FROM test_insert_table
           """.stripMargin)
      }.getMessage

      assert(e.contains("mismatched input 'ROW'"))
    }
  }

  test("insert overwrite to dir with multi inserts") {
    withTempView("test_insert_table") {
      spark.range(10).selectExpr("id", "id AS str").createOrReplaceTempView("test_insert_table")

      val e = intercept[ParseException] {
        sql(
          s"""
             |INSERT OVERWRITE DIRECTORY 'file://tmp2'
             |USING json
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             |SELECT * FROM test_insert_table
             |INSERT OVERWRITE DIRECTORY 'file://tmp2'
             |USING json
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             |SELECT * FROM test_insert_table
           """.stripMargin)
      }.getMessage

      assert(e.contains("mismatched input 'ROW'"))
    }
  }


  test("insert overwrite to dir from non-existent table") {
    withTempDir { dir =>
      val path = dir.toURI.getPath

      val e = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE LOCAL DIRECTORY '${path}' TABLE notexists")
      }.getMessage
      assert(e.contains("Table or view not found"))
    }
  }

  test("SPARK-21165: FileFormatWriter should only rely on attributes from analyzed plan") {
    withSQLConf(("hive.exec.dynamic.partition.mode", "nonstrict")) {
      withTable("tab1", "tab2") {
        Seq(("a", "b", 3)).toDF("word", "first", "length").write.saveAsTable("tab1")

        spark.sql(
          """
            |CREATE TABLE tab2 (word string, length int)
            |PARTITIONED BY (first string)
          """.stripMargin)

        spark.sql(
          """
            |INSERT INTO TABLE tab2 PARTITION(first)
            |SELECT word, length, cast(first as string) as first FROM tab1
          """.stripMargin)

        checkAnswer(spark.table("tab2"), Row("a", 3, "b"))
      }
    }
  }

  test("SPARK-26307: CTAS - INSERT a partitioned table using Hive serde") {
    withTable("tab1") {
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        val df = Seq(("a", 100)).toDF("part", "id")
        df.write.format("hive").partitionBy("part").mode("overwrite").saveAsTable("tab1")
        df.write.format("hive").partitionBy("part").mode("append").saveAsTable("tab1")
      }
    }
  }


  Seq("LOCAL", "").foreach { local =>
    Seq(true, false).foreach { caseSensitivity =>
      Seq("orc", "parquet").foreach { format =>
        test(s"SPARK-25389 INSERT OVERWRITE $local DIRECTORY ... STORED AS with duplicated names" +
          s"(caseSensitivity=$caseSensitivity, format=$format)") {
          withTempDir { dir =>
            withSQLConf(SQLConf.CASE_SENSITIVE.key -> s"$caseSensitivity") {
              val m = intercept[AnalysisException] {
                sql(
                  s"""
                     |INSERT OVERWRITE $local DIRECTORY '${dir.toURI}'
                     |STORED AS $format
                     |SELECT 'id', 'id2' ${if (caseSensitivity) "id" else "ID"}
                   """.stripMargin)
              }.getMessage
              assert(m.contains("Found duplicate column(s) when inserting into"))
            }
          }
        }
      }
    }
  }

  test("SPARK-30201 HiveOutputWriter standardOI should use ObjectInspectorCopyOption.DEFAULT") {
    withTable("t1", "t2") {
      withTempDir { dir =>
        val file = new File(dir, "test.hex")
        val hex = "AABBCC"
        val bs = org.apache.commons.codec.binary.Hex.decodeHex(hex.toCharArray)
        Files.write(bs, file)
        val path = file.getParent
        sql(s"create table t1 (c string) STORED AS TEXTFILE location '$path'")
        checkAnswer(
          sql("select hex(c) from t1"),
          Row(hex)
        )

        sql("create table t2 as select c from t1")
        checkAnswer(
          sql("select hex(c) from t2"),
          Row(hex)
        )
      }
    }
  }

  test("SPARK-32508 " +
    "Disallow empty part col values in partition spec before static partition writing") {
    withTable("t1") {
      spark.sql(
        """
          |CREATE TABLE t1 (c1 int)
          |PARTITIONED BY (d string)
          """.stripMargin)

      val e = intercept[AnalysisException] {
        spark.sql(
          """
            |INSERT OVERWRITE TABLE t1 PARTITION(d='')
            |SELECT 1
          """.stripMargin)
      }.getMessage

      assert(!e.contains("get partition: Value for key d is null or empty"))
      assert(e.contains("Partition spec is invalid"))
    }
  }
}
