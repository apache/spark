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

package org.apache.spark.sql.sources

import java.io.{File, IOException}
import java.sql.Date

import org.apache.hadoop.fs.{FileAlreadyExistsException, FSDataOutputStream, Path, RawLocalFileSystem}

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class SimpleInsertSource extends SchemaRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    SimpleInsert(schema)(sqlContext.sparkSession)
  }
}

case class SimpleInsert(userSpecifiedSchema: StructType)(@transient val sparkSession: SparkSession)
  extends BaseRelation with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = userSpecifiedSchema

  override def insert(input: DataFrame, overwrite: Boolean): Unit = {
    input.collect
  }
}

class InsertSuite extends DataSourceTest with SharedSparkSession {
  import testImplicits._

  protected override lazy val sql = spark.sql _
  private var path: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    path = Utils.createTempDir()
    val ds = (1 to 10).map(i => s"""{"a":$i, "b":"str$i"}""").toDS()
    spark.read.json(ds).createOrReplaceTempView("jt")
    sql(
      s"""
        |CREATE TEMPORARY VIEW jsonTable (a int, b string)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toURI.toString}'
        |)
      """.stripMargin)
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("jsonTable")
      spark.catalog.dropTempView("jt")
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  test("Simple INSERT OVERWRITE a JSONRelation") {
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )
  }

  test("insert into a temp view that does not point to an insertable data source") {
    import testImplicits._
    withTempView("t1", "t2") {
      sql(
        """
          |CREATE TEMPORARY VIEW t1
          |USING org.apache.spark.sql.sources.SimpleScanSource
          |OPTIONS (
          |  From '1',
          |  To '10')
        """.stripMargin)
      sparkContext.parallelize(1 to 10).toDF("a").createOrReplaceTempView("t2")

      val message = intercept[AnalysisException] {
        sql("INSERT INTO TABLE t1 SELECT a FROM t2")
      }.getMessage
      assert(message.contains("does not allow insertion"))
    }
  }

  test("PreInsert casting and renaming") {
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a * 2, a * 4 FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i * 2, s"${i * 4}"))
    )

    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a * 4 AS A, a * 6 as c FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i * 4, s"${i * 6}"))
    )
  }

  test("SELECT clause generating a different number of columns is not allowed.") {
    val message = intercept[AnalysisException] {
      sql(
        s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a FROM jt
      """.stripMargin)
    }.getMessage
    assert(message.contains("target table has 2 column(s) but the inserted data has 1 column(s)")
    )
  }

  test("INSERT OVERWRITE a JSONRelation multiple times") {
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )

    // Writing the table to less part files.
    val rdd1 = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str$i"}"""), 5)
    spark.read.json(rdd1.toDS()).createOrReplaceTempView("jt1")
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt1
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )

    // Writing the table to more part files.
    val rdd2 = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str$i"}"""), 10)
    spark.read.json(rdd2.toDS()).createOrReplaceTempView("jt2")
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt2
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )

    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a * 10, b FROM jt1
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i * 10, s"str$i"))
    )

    spark.catalog.dropTempView("jt1")
    spark.catalog.dropTempView("jt2")
  }

  test("INSERT INTO JSONRelation for now") {
    sql(
      s"""
      |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt").collect()
    )

    sql(
      s"""
         |INSERT INTO TABLE jsonTable SELECT a, b FROM jt
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt UNION ALL SELECT a, b FROM jt").collect()
    )
  }

  test("INSERT INTO TABLE with Comment in columns") {
    val tabName = "tab1"
    withTable(tabName) {
      sql(
        s"""
           |CREATE TABLE $tabName(col1 int COMMENT 'a', col2 int)
           |USING parquet
         """.stripMargin)
      sql(s"INSERT INTO TABLE $tabName SELECT 1, 2")

      checkAnswer(
        sql(s"SELECT col1, col2 FROM $tabName"),
        Row(1, 2) :: Nil
      )
    }
  }

  test("INSERT INTO TABLE - complex type but different names") {
    val tab1 = "tab1"
    val tab2 = "tab2"
    withTable(tab1, tab2) {
      sql(
        s"""
           |CREATE TABLE $tab1 (s struct<a: string, b: string>)
           |USING parquet
         """.stripMargin)
      sql(s"INSERT INTO TABLE $tab1 SELECT named_struct('col1','1','col2','2')")

      sql(
        s"""
           |CREATE TABLE $tab2 (p struct<c: string, d: string>)
           |USING parquet
         """.stripMargin)
      sql(s"INSERT INTO TABLE $tab2 SELECT * FROM $tab1")

      checkAnswer(
        spark.table(tab1),
        spark.table(tab2)
      )
    }
  }

  test("it is not allowed to write to a table while querying it.") {
    val message = intercept[AnalysisException] {
      sql(
        s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jsonTable
      """.stripMargin)
    }.getMessage
    assert(
      message.contains("Cannot overwrite a path that is also being read from."),
      "INSERT OVERWRITE to a table while querying it should not be allowed.")
  }

  test("SPARK-30112: it is allowed to write to a table while querying it for " +
    "dynamic partition overwrite.") {
    Seq(PartitionOverwriteMode.DYNAMIC.toString,
        PartitionOverwriteMode.STATIC.toString).foreach { mode =>
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> mode) {
        withTable("insertTable") {
          sql(
            """
              |CREATE TABLE insertTable(i int, part1 int, part2 int) USING PARQUET
              |PARTITIONED BY (part1, part2)
            """.stripMargin)

          sql("INSERT INTO TABLE insertTable PARTITION(part1=1, part2=1) SELECT 1")
          checkAnswer(spark.table("insertTable"), Row(1, 1, 1))
          sql("INSERT OVERWRITE TABLE insertTable PARTITION(part1=1, part2=2) SELECT 2")
          checkAnswer(spark.table("insertTable"), Row(1, 1, 1) :: Row(2, 1, 2) :: Nil)

          if (mode == PartitionOverwriteMode.DYNAMIC.toString) {
            sql(
              """
                |INSERT OVERWRITE TABLE insertTable PARTITION(part1=1, part2)
                |SELECT i + 1, part2 FROM insertTable
              """.stripMargin)
            checkAnswer(spark.table("insertTable"), Row(2, 1, 1) :: Row(3, 1, 2) :: Nil)

            sql(
              """
                |INSERT OVERWRITE TABLE insertTable PARTITION(part1=1, part2)
                |SELECT i + 1, part2 + 1 FROM insertTable
              """.stripMargin)
            checkAnswer(spark.table("insertTable"),
              Row(2, 1, 1) :: Row(3, 1, 2) :: Row(4, 1, 3) :: Nil)
          } else {
            val message = intercept[AnalysisException] {
              sql(
                """
                  |INSERT OVERWRITE TABLE insertTable PARTITION(part1=1, part2)
                  |SELECT i + 1, part2 FROM insertTable
                """.stripMargin)
            }.getMessage
            assert(
              message.contains("Cannot overwrite a path that is also being read from."),
              "INSERT OVERWRITE to a table while querying it should not be allowed.")
          }
        }
      }
    }
  }

  test("Caching")  {
    // write something to the jsonTable
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)
    // Cached Query Execution
    spark.catalog.cacheTable("jsonTable")
    assertCached(sql("SELECT * FROM jsonTable"))
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i")))

    assertCached(sql("SELECT a FROM jsonTable"))
    checkAnswer(
      sql("SELECT a FROM jsonTable"),
      (1 to 10).map(Row(_)).toSeq)

    assertCached(sql("SELECT a FROM jsonTable WHERE a < 5"))
    checkAnswer(
      sql("SELECT a FROM jsonTable WHERE a < 5"),
      (1 to 4).map(Row(_)).toSeq)

    assertCached(sql("SELECT a * 2 FROM jsonTable"))
    checkAnswer(
      sql("SELECT a * 2 FROM jsonTable"),
      (1 to 10).map(i => Row(i * 2)).toSeq)

    assertCached(sql(
      "SELECT x.a, y.a FROM jsonTable x JOIN jsonTable y ON x.a = y.a + 1"), 2)
    checkAnswer(sql(
      "SELECT x.a, y.a FROM jsonTable x JOIN jsonTable y ON x.a = y.a + 1"),
      (2 to 10).map(i => Row(i, i - 1)).toSeq)

    // Insert overwrite and keep the same schema.
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a * 2, b FROM jt
      """.stripMargin)
    // jsonTable should be recached.
    assertCached(sql("SELECT * FROM jsonTable"))

    // The cached data is the new data.
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a * 2, b FROM jt").collect())

    // Verify uncaching
    spark.catalog.uncacheTable("jsonTable")
    assertCached(sql("SELECT * FROM jsonTable"), 0)
  }

  test("it's not allowed to insert into a relation that is not an InsertableRelation") {
    sql(
      """
        |CREATE TEMPORARY VIEW oneToTen
        |USING org.apache.spark.sql.sources.SimpleScanSource
        |OPTIONS (
        |  From '1',
        |  To '10'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM oneToTen"),
      (1 to 10).map(Row(_)).toSeq
    )

    val message = intercept[AnalysisException] {
      sql(
        s"""
        |INSERT OVERWRITE TABLE oneToTen SELECT CAST(a AS INT) FROM jt
        """.stripMargin)
    }.getMessage
    assert(
      message.contains("does not allow insertion."),
      "It is not allowed to insert into a table that is not an InsertableRelation."
    )

    spark.catalog.dropTempView("oneToTen")
  }

  test("SPARK-15824 - Execute an INSERT wrapped in a WITH statement immediately") {
    withTable("target", "target2") {
      sql(s"CREATE TABLE target(a INT, b STRING) USING JSON")
      sql("WITH tbl AS (SELECT * FROM jt) INSERT OVERWRITE TABLE target SELECT a, b FROM tbl")
      checkAnswer(
        sql("SELECT a, b FROM target"),
        sql("SELECT a, b FROM jt")
      )

      sql(s"CREATE TABLE target2(a INT, b STRING) USING JSON")
      val e = sql(
        """
          |WITH tbl AS (SELECT * FROM jt)
          |FROM tbl
          |INSERT INTO target2 SELECT a, b WHERE a <= 5
          |INSERT INTO target2 SELECT a, b WHERE a > 5
        """.stripMargin)
      checkAnswer(
        sql("SELECT a, b FROM target2"),
        sql("SELECT a, b FROM jt")
      )
    }
  }

  test("SPARK-21203 wrong results of insertion of Array of Struct") {
    val tabName = "tab1"
    withTable(tabName) {
      spark.sql(
        """
          |CREATE TABLE `tab1`
          |(`custom_fields` ARRAY<STRUCT<`id`: BIGINT, `value`: STRING>>)
          |USING parquet
        """.stripMargin)
      spark.sql(
        """
          |INSERT INTO `tab1`
          |SELECT ARRAY(named_struct('id', 1, 'value', 'a'), named_struct('id', 2, 'value', 'b'))
        """.stripMargin)

      checkAnswer(
        spark.sql("SELECT custom_fields.id, custom_fields.value FROM tab1"),
        Row(Array(1, 2), Array("a", "b")))
    }
  }

  test("insert overwrite directory") {
    withTempDir { dir =>
      val path = dir.toURI.getPath

      val v1 =
        s"""
           | INSERT OVERWRITE DIRECTORY '${path}'
           | USING json
           | OPTIONS (a 1, b 0.1, c TRUE)
           | SELECT 1 as a, 'c' as b
         """.stripMargin

      spark.sql(v1)

      checkAnswer(
        spark.read.json(dir.getCanonicalPath),
        sql("SELECT 1 as a, 'c' as b"))
    }
  }

  test("insert overwrite directory with path in options") {
    withTempDir { dir =>
      val path = dir.toURI.getPath

      val v1 =
        s"""
           | INSERT OVERWRITE DIRECTORY
           | USING json
           | OPTIONS ('path' '${path}')
           | SELECT 1 as a, 'c' as b
         """.stripMargin

      spark.sql(v1)

      checkAnswer(
        spark.read.json(dir.getCanonicalPath),
        sql("SELECT 1 as a, 'c' as b"))
    }
  }

  test("Insert overwrite directory using Hive serde without turning on Hive support") {
    withTempDir { dir =>
      val path = dir.toURI.getPath
      val e = intercept[AnalysisException] {
        sql(
          s"""
             |INSERT OVERWRITE LOCAL DIRECTORY '$path'
             |STORED AS orc
             |SELECT 1, 2
           """.stripMargin)
      }.getMessage
      assert(e.contains(
        "Hive support is required to INSERT OVERWRITE DIRECTORY with the Hive format"))
    }
  }

  test("insert overwrite directory to data source not providing FileFormat") {
    withTempDir { dir =>
      val path = dir.toURI.getPath

      val v1 =
        s"""
           | INSERT OVERWRITE DIRECTORY '${path}'
           | USING JDBC
           | OPTIONS (a 1, b 0.1, c TRUE)
           | SELECT 1 as a, 'c' as b
         """.stripMargin
      val e = intercept[SparkException] {
        spark.sql(v1)
      }.getMessage

      assert(e.contains("Only Data Sources providing FileFormat are supported"))
    }
  }

  test("new partitions should be added to catalog after writing to catalog table") {
    val table = "partitioned_catalog_table"
    val tempTable = "partitioned_catalog_temp_table"
    val numParts = 210
    withTable(table) {
      withTempView(tempTable) {
        val df = (1 to numParts).map(i => (i, i)).toDF("part", "col1")
        df.createOrReplaceTempView(tempTable)
        sql(s"CREATE TABLE $table (part Int, col1 Int) USING parquet PARTITIONED BY (part)")
        sql(s"INSERT INTO TABLE $table SELECT * from $tempTable")
        val partitions = spark.sessionState.catalog.listPartitionNames(TableIdentifier(table))
        assert(partitions.size == numParts)
      }
    }
  }

  test("SPARK-20236: dynamic partition overwrite without catalog table") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      withTempPath { path =>
        Seq((1, 1, 1)).toDF("i", "part1", "part2")
          .write.partitionBy("part1", "part2").parquet(path.getAbsolutePath)
        checkAnswer(spark.read.parquet(path.getAbsolutePath), Row(1, 1, 1))

        Seq((2, 1, 1)).toDF("i", "part1", "part2")
          .write.partitionBy("part1", "part2").mode("overwrite").parquet(path.getAbsolutePath)
        checkAnswer(spark.read.parquet(path.getAbsolutePath), Row(2, 1, 1))

        Seq((2, 2, 2)).toDF("i", "part1", "part2")
          .write.partitionBy("part1", "part2").mode("overwrite").parquet(path.getAbsolutePath)
        checkAnswer(spark.read.parquet(path.getAbsolutePath), Row(2, 1, 1) :: Row(2, 2, 2) :: Nil)
      }
    }
  }

  test("SPARK-20236: dynamic partition overwrite") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      withTable("t") {
        sql(
          """
            |create table t(i int, part1 int, part2 int) using parquet
            |partitioned by (part1, part2)
          """.stripMargin)

        sql("insert into t partition(part1=1, part2=1) select 1")
        checkAnswer(spark.table("t"), Row(1, 1, 1))

        sql("insert overwrite table t partition(part1=1, part2=1) select 2")
        checkAnswer(spark.table("t"), Row(2, 1, 1))

        sql("insert overwrite table t partition(part1=2, part2) select 2, 2")
        checkAnswer(spark.table("t"), Row(2, 1, 1) :: Row(2, 2, 2) :: Nil)

        sql("insert overwrite table t partition(part1=1, part2=2) select 3")
        checkAnswer(spark.table("t"), Row(2, 1, 1) :: Row(2, 2, 2) :: Row(3, 1, 2) :: Nil)

        sql("insert overwrite table t partition(part1=1, part2) select 4, 1")
        checkAnswer(spark.table("t"), Row(4, 1, 1) :: Row(2, 2, 2) :: Row(3, 1, 2) :: Nil)
      }
    }
  }

  test("SPARK-20236: dynamic partition overwrite with customer partition path") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      withTable("t") {
        sql(
          """
            |create table t(i int, part1 int, part2 int) using parquet
            |partitioned by (part1, part2)
          """.stripMargin)

        val path1 = Utils.createTempDir()
        sql(s"alter table t add partition(part1=1, part2=1) location '$path1'")
        sql(s"insert into t partition(part1=1, part2=1) select 1")
        checkAnswer(spark.table("t"), Row(1, 1, 1))

        sql("insert overwrite table t partition(part1=1, part2=1) select 2")
        checkAnswer(spark.table("t"), Row(2, 1, 1))

        sql("insert overwrite table t partition(part1=2, part2) select 2, 2")
        checkAnswer(spark.table("t"), Row(2, 1, 1) :: Row(2, 2, 2) :: Nil)

        val path2 = Utils.createTempDir()
        sql(s"alter table t add partition(part1=1, part2=2) location '$path2'")
        sql("insert overwrite table t partition(part1=1, part2=2) select 3")
        checkAnswer(spark.table("t"), Row(2, 1, 1) :: Row(2, 2, 2) :: Row(3, 1, 2) :: Nil)

        sql("insert overwrite table t partition(part1=1, part2) select 4, 1")
        checkAnswer(spark.table("t"), Row(4, 1, 1) :: Row(2, 2, 2) :: Row(3, 1, 2) :: Nil)
      }
    }
  }

  test("Throw exception on unsafe cast with strict casting policy") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.STRICT.toString) {
      withTable("t") {
        sql("create table t(i int, d double) using parquet")
        var msg = intercept[AnalysisException] {
          sql("insert into t select 1L, 2")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'i': bigint to int"))

        msg = intercept[AnalysisException] {
          sql("insert into t select 1, 2.0")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'd': decimal(2,1) to double"))

        msg = intercept[AnalysisException] {
          sql("insert into t select 1, 2.0D, 3")
        }.getMessage
        assert(msg.contains("`t` requires that the data to be inserted have the same number of " +
          "columns as the target table: target table has 2 column(s)" +
          " but the inserted data has 3 column(s)"))

        msg = intercept[AnalysisException] {
          sql("insert into t select 1")
        }.getMessage
        assert(msg.contains("`t` requires that the data to be inserted have the same number of " +
          "columns as the target table: target table has 2 column(s)" +
          " but the inserted data has 1 column(s)"))

        // Insert into table successfully.
        sql("insert into t select 1, 2.0D")
        checkAnswer(sql("select * from t"), Row(1, 2.0D))
      }
    }
  }

  test("Throw exception on unsafe cast with ANSI casting policy") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(i int, d double) using parquet")
        var msg = intercept[AnalysisException] {
          sql("insert into t values('a', 'b')")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'i': string to int") &&
          msg.contains("Cannot safely cast 'd': string to double"))
        msg = intercept[AnalysisException] {
          sql("insert into t values(now(), now())")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'i': timestamp to int") &&
          msg.contains("Cannot safely cast 'd': timestamp to double"))
        msg = intercept[AnalysisException] {
          sql("insert into t values(true, false)")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'i': boolean to int") &&
          msg.contains("Cannot safely cast 'd': boolean to double"))
      }
    }
  }

  test("Allow on writing any numeric value to numeric type with ANSI policy") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(i int, d float) using parquet")
        sql("insert into t values(1L, 2.0)")
        sql("insert into t values(3.0, 4)")
        sql("insert into t values(5.0, 6L)")
        checkAnswer(sql("select * from t"), Seq(Row(1, 2.0F), Row(3, 4.0F), Row(5, 6.0F)))
      }
    }
  }

  test("Allow on writing timestamp value to date type with ANSI policy") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(i date) using parquet")
        sql("insert into t values(TIMESTAMP('2010-09-02 14:10:10'))")
        checkAnswer(sql("select * from t"), Seq(Row(Date.valueOf("2010-09-02"))))
      }
    }
  }

  test("Throw exceptions on inserting out-of-range int value with ANSI casting policy") {
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(b int) using parquet")
        val outOfRangeValue1 = (Int.MaxValue + 1L).toString
        var msg = intercept[SparkException] {
          sql(s"insert into t values($outOfRangeValue1)")
        }.getCause.getMessage
        assert(msg.contains(s"Casting $outOfRangeValue1 to int causes overflow"))

        val outOfRangeValue2 = (Int.MinValue - 1L).toString
        msg = intercept[SparkException] {
          sql(s"insert into t values($outOfRangeValue2)")
        }.getCause.getMessage
        assert(msg.contains(s"Casting $outOfRangeValue2 to int causes overflow"))
      }
    }
  }

  test("Throw exceptions on inserting out-of-range long value with ANSI casting policy") {
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(b long) using parquet")
        val outOfRangeValue1 = Math.nextUp(Long.MaxValue)
        var msg = intercept[SparkException] {
          sql(s"insert into t values(${outOfRangeValue1}D)")
        }.getCause.getMessage
        assert(msg.contains(s"Casting $outOfRangeValue1 to long causes overflow"))

        val outOfRangeValue2 = Math.nextDown(Long.MinValue)
        msg = intercept[SparkException] {
          sql(s"insert into t values(${outOfRangeValue2}D)")
        }.getCause.getMessage
        assert(msg.contains(s"Casting $outOfRangeValue2 to long causes overflow"))
      }
    }
  }

  test("Throw exceptions on inserting out-of-range decimal value with ANSI casting policy") {
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(b decimal(3,2)) using parquet")
        val outOfRangeValue = "123.45"
        val msg = intercept[SparkException] {
          sql(s"insert into t values(${outOfRangeValue})")
        }.getCause.getMessage
        assert(msg.contains("cannot be represented as Decimal(3, 2)"))
      }
    }
  }

  test("SPARK-33354: Throw exceptions on inserting invalid cast with ANSI casting policy") {
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("CREATE TABLE t(i int, t timestamp) USING parquet")
        val msg = intercept[AnalysisException] {
          sql("INSERT INTO t VALUES (TIMESTAMP('2010-09-02 14:10:10'), 1)")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'i': timestamp to int"))
        assert(msg.contains("Cannot safely cast 't': int to timestamp"))
      }

      withTable("t") {
        sql("CREATE TABLE t(i int, d date) USING parquet")
        val msg = intercept[AnalysisException] {
          sql("INSERT INTO t VALUES (date('2010-09-02'), 1)")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'i': date to int"))
        assert(msg.contains("Cannot safely cast 'd': int to date"))
      }

      withTable("t") {
        sql("CREATE TABLE t(b boolean, t timestamp) USING parquet")
        val msg = intercept[AnalysisException] {
          sql("INSERT INTO t VALUES (TIMESTAMP('2010-09-02 14:10:10'), true)")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'b': timestamp to boolean"))
        assert(msg.contains("Cannot safely cast 't': boolean to timestamp"))
      }

      withTable("t") {
        sql("CREATE TABLE t(b boolean, d date) USING parquet")
        val msg = intercept[AnalysisException] {
          sql("INSERT INTO t VALUES (date('2010-09-02'), true)")
        }.getMessage
        assert(msg.contains("Cannot safely cast 'b': date to boolean"))
        assert(msg.contains("Cannot safely cast 'd': boolean to date"))
      }
    }
  }

  test("SPARK-24860: dynamic partition overwrite specified per source without catalog table") {
    withTempPath { path =>
      Seq((1, 1), (2, 2)).toDF("i", "part")
        .write.partitionBy("part")
        .parquet(path.getAbsolutePath)
      checkAnswer(spark.read.parquet(path.getAbsolutePath), Row(1, 1) :: Row(2, 2) :: Nil)

      Seq((1, 2), (1, 3)).toDF("i", "part")
        .write.partitionBy("part").mode("overwrite")
        .option("partitionOverwriteMode", "dynamic").parquet(path.getAbsolutePath)
      checkAnswer(spark.read.parquet(path.getAbsolutePath),
        Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Nil)

      Seq((1, 2), (1, 3)).toDF("i", "part")
        .write.partitionBy("part").mode("overwrite")
        .option("partitionOverwriteMode", "static").parquet(path.getAbsolutePath)
      checkAnswer(spark.read.parquet(path.getAbsolutePath), Row(1, 2) :: Row(1, 3) :: Nil)
    }
  }

  test("SPARK-24583 Wrong schema type in InsertIntoDataSourceCommand") {
    withTable("test_table") {
      val schema = new StructType()
        .add("i", LongType, false)
        .add("s", StringType, false)
      val newTable = CatalogTable(
        identifier = TableIdentifier("test_table", None),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          properties = Map.empty),
        schema = schema,
        provider = Some(classOf[SimpleInsertSource].getName))

      spark.sessionState.catalog.createTable(newTable, false)

      sql("INSERT INTO TABLE test_table SELECT 1, 'a'")
      val msg = intercept[AnalysisException] {
        sql("INSERT INTO TABLE test_table SELECT 2, null")
      }.getMessage
      assert(msg.contains("Cannot write nullable values to non-null column 's'"))
    }
  }

  test("Stop task set if FileAlreadyExistsException was thrown") {
    Seq(true, false).foreach { fastFail =>
      withSQLConf("fs.file.impl" -> classOf[FileExistingTestFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true",
        SQLConf.FASTFAIL_ON_FILEFORMAT_OUTPUT.key -> fastFail.toString) {
        withTable("t") {
          sql(
            """
              |CREATE TABLE t(i INT, part1 INT) USING PARQUET
              |PARTITIONED BY (part1)
          """.stripMargin)

          val df = Seq((1, 1)).toDF("i", "part1")
          val err = intercept[SparkException] {
            df.write.mode("overwrite").format("parquet").insertInto("t")
          }

          if (fastFail) {
            assert(err.getCause.getMessage.contains("can not write to output file: " +
              "org.apache.hadoop.fs.FileAlreadyExistsException"))
          } else {
            assert(err.getCause.getMessage.contains("Task failed while writing rows"))
          }
        }
      }
    }
  }

  test("SPARK-29174 Support LOCAL in INSERT OVERWRITE DIRECTORY to data source") {
    withTempPath { dir =>
      val path = dir.toURI.getPath
      sql(s"""create table tab1 ( a int) using parquet location '$path'""")
      sql("insert into tab1 values(1)")
      checkAnswer(sql("select * from tab1"), Seq(1).map(i => Row(i)))
      sql("create table tab2 ( a int) using parquet")
      sql("insert into tab2 values(2)")
      checkAnswer(sql("select * from tab2"), Seq(2).map(i => Row(i)))
      sql(s"""insert overwrite local directory '$path' using parquet select * from tab2""")
      sql("refresh table tab1")
      checkAnswer(sql("select * from tab1"), Seq(2).map(i => Row(i)))
    }
  }

  test("SPARK-29174 fail LOCAL in INSERT OVERWRITE DIRECT remote path") {
    val message = intercept[ParseException] {
      sql("insert overwrite local directory 'hdfs:/abcd' using parquet select 1")
    }.getMessage
    assert(message.contains("LOCAL is supported only with file: scheme"))
  }

  test("SPARK-32508 " +
    "Disallow empty part col values in partition spec before static partition writing") {
    withTable("insertTable") {
      sql(
        """
          |CREATE TABLE insertTable(i int, part1 string, part2 string) USING PARQUET
          |PARTITIONED BY (part1, part2)
            """.stripMargin)
      val msg = "Partition spec is invalid"
      assert(intercept[AnalysisException] {
        sql("INSERT INTO TABLE insertTable PARTITION(part1=1, part2='') SELECT 1")
      }.getMessage.contains(msg))
      assert(intercept[AnalysisException] {
        sql("INSERT INTO TABLE insertTable PARTITION(part1='', part2) SELECT 1 ,'' AS part2")
      }.getMessage.contains(msg))

      sql("INSERT INTO TABLE insertTable PARTITION(part1='1', part2='2') SELECT 1")
      sql("INSERT INTO TABLE insertTable PARTITION(part1='1', part2) SELECT 1 ,'2' AS part2")
      sql("INSERT INTO TABLE insertTable PARTITION(part1='1', part2) SELECT 1 ,'' AS part2")
    }
  }

  test("SPARK-33294: Add query resolved check before analyze InsertIntoDir") {
    withTempPath { path =>
      val msg = intercept[AnalysisException] {
        sql(
          s"""
            |INSERT OVERWRITE DIRECTORY '${path.getAbsolutePath}' USING PARQUET
            |SELECT * FROM (
            | SELECT c3 FROM (
            |  SELECT c1, c2 from values(1,2) t(c1, c2)
            |  )
            |)
          """.stripMargin)
      }.getMessage
      assert(msg.contains("cannot resolve '`c3`' given input columns"))
    }
  }

  test("SPARK-34926: PartitioningUtils.getPathFragment() should respect partition value is null") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1(id INT) USING PARQUET")
      sql(
        """
          |CREATE TABLE t2 (c1 INT, part STRING)
          |  USING parquet
          |PARTITIONED BY (part)
          |""".stripMargin)
      sql(
        """
          |INSERT INTO TABLE t2 PARTITION (part = null)
          |SELECT * FROM t1 where 1=0""".stripMargin)
      checkAnswer(spark.table("t2"), Nil)
    }
  }

  test("SPARK-35106: insert overwrite with custom partition path") {
    withTempPath { path =>
      withTable("t") {
      sql(
        """
          |create table t(i int, part1 int, part2 int) using parquet
          |partitioned by (part1, part2)
        """.stripMargin)

        sql(s"alter table t add partition(part1=1, part2=1) location '${path.getAbsolutePath}'")
        sql(s"insert into t partition(part1=1, part2=1) select 1")
        checkAnswer(spark.table("t"), Row(1, 1, 1))

        sql("insert overwrite table t partition(part1=1, part2=1) select 2")
        checkAnswer(spark.table("t"), Row(2, 1, 1))

        sql("insert overwrite table t partition(part1=2, part2) select 2, 2")
        checkAnswer(spark.table("t"), Row(2, 1, 1) :: Row(2, 2, 2) :: Nil)

        sql("insert overwrite table t partition(part1=1, part2=2) select 3")
        checkAnswer(spark.table("t"), Row(2, 1, 1) :: Row(2, 2, 2) :: Row(3, 1, 2) :: Nil)

        sql("insert overwrite table t partition(part1=1, part2) select 4, 1")
        checkAnswer(spark.table("t"), Row(4, 1, 1) :: Row(2, 2, 2) :: Nil)
      }
    }
  }

  test("SPARK-35106: dynamic partition overwrite with custom partition path") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      withTempPath { path =>
        withTable("t") {
          sql(
            """
              |create table t(i int, part1 int, part2 int) using parquet
              |partitioned by (part1, part2)
            """.stripMargin)

          sql(s"insert into t partition(part1=1, part2=1) select 1")
          checkAnswer(spark.table("t"), Row(1, 1, 1))

          sql(s"alter table t add partition(part1=1, part2=2) location '${path.getAbsolutePath}'")

          // dynamic partition overwrite to empty custom partition
          sql(s"insert overwrite table t partition(part1=1, part2=2) select 1")
          checkAnswer(spark.table("t"), Row(1, 1, 1) :: Row(1, 1, 2) :: Nil)

          // dynamic partition overwrite to non-empty custom partition
          sql("insert overwrite table t partition(part1=1, part2=2) select 2")
          checkAnswer(spark.table("t"), Row(1, 1, 1) :: Row(2, 1, 2) :: Nil)
        }
      }
    }
  }

  test("SPARK-35106: Throw exception when rename custom partition paths returns false") {
    withSQLConf(
      "fs.file.impl" -> classOf[RenameFromSparkStagingToFinalDirAlwaysTurnsFalseFilesystem].getName,
      "fs.file.impl.disable.cache" -> "true") {
      withTempPath { path =>
        withTable("t") {
          sql(
            """
              |create table t(i int, part1 int, part2 int) using parquet
              |partitioned by (part1, part2)
            """.stripMargin)

          sql(s"alter table t add partition(part1=1, part2=1) location '${path.getAbsolutePath}'")

          val e = intercept[SparkException] {
            sql(s"insert into t partition(part1=1, part2=1) select 1")
          }.getCause
          assert(e.isInstanceOf[IOException])
          assert(e.getMessage.contains("Failed to rename"))
          assert(e.getMessage.contains("when committing files staged for absolute location"))
        }
      }
    }
  }

  test("SPARK-35106: Throw exception when rename dynamic partition paths returns false") {
    withSQLConf(
      "fs.file.impl" -> classOf[RenameFromSparkStagingToFinalDirAlwaysTurnsFalseFilesystem].getName,
      "fs.file.impl.disable.cache" -> "true",
      SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {

      withTable("t") {
        sql(
          """
            |create table t(i int, part1 int, part2 int) using parquet
            |partitioned by (part1, part2)
          """.stripMargin)

        val e = intercept[SparkException] {
          sql(s"insert overwrite table t partition(part1, part2) values (1, 1, 1)")
        }.getCause
        assert(e.isInstanceOf[IOException])
        assert(e.getMessage.contains("Failed to rename"))
        assert(e.getMessage.contains(
          "when committing files staged for overwriting dynamic partitions"))
      }
    }
  }
}

class FileExistingTestFileSystem extends RawLocalFileSystem {
  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long): FSDataOutputStream = {
    throw new FileAlreadyExistsException(s"${f.toString} already exists")
  }
}

class RenameFromSparkStagingToFinalDirAlwaysTurnsFalseFilesystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    (!isSparkStagingDir(src) || isSparkStagingDir(dst)) && super.rename(src, dst)
  }

  private def isSparkStagingDir(path: Path): Boolean = {
    path.toString.contains(".spark-staging-")
  }
}
