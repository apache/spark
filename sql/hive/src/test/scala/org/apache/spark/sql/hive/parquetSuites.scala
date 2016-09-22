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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

// The data where the partitioning key exists only in the directory structure.
case class ParquetData(intField: Int, stringField: String)
// The data that also includes the partitioning key
case class ParquetDataWithKey(p: Int, intField: Int, stringField: String)

case class StructContainer(intStructField: Int, stringStructField: String)

case class ParquetDataWithComplexTypes(
    intField: Int,
    stringField: String,
    structField: StructContainer,
    arrayField: Seq[Int])

case class ParquetDataWithKeyAndComplexTypes(
    p: Int,
    intField: Int,
    stringField: String,
    structField: StructContainer,
    arrayField: Seq[Int])

/**
 * A suite to test the automatic conversion of metastore tables with parquet data to use the
 * built in parquet support.
 */
class ParquetMetastoreSuite extends ParquetPartitioningTest {
  import hiveContext._
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropTables("partitioned_parquet",
      "partitioned_parquet_with_key",
      "partitioned_parquet_with_complextypes",
      "partitioned_parquet_with_key_and_complextypes",
      "normal_parquet",
      "jt",
      "jt_array",
      "test_parquet")
    sql(s"""
      create external table partitioned_parquet
      (
        intField INT,
        stringField STRING
      )
      PARTITIONED BY (p int)
      ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
       STORED AS
       INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
       OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      location '${partitionedTableDir.getCanonicalPath}'
    """)

    sql(s"""
      create external table partitioned_parquet_with_key
      (
        intField INT,
        stringField STRING
      )
      PARTITIONED BY (p int)
      ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
       STORED AS
       INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
       OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      location '${partitionedTableDirWithKey.getCanonicalPath}'
    """)

    sql(s"""
      create external table normal_parquet
      (
        intField INT,
        stringField STRING
      )
      ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
       STORED AS
       INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
       OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      location '${new File(normalTableDir, "normal").getCanonicalPath}'
    """)

    sql(s"""
      CREATE EXTERNAL TABLE partitioned_parquet_with_complextypes
      (
        intField INT,
        stringField STRING,
        structField STRUCT<intStructField: INT, stringStructField: STRING>,
        arrayField ARRAY<INT>
      )
      PARTITIONED BY (p int)
      ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
       STORED AS
       INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
       OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      LOCATION '${partitionedTableDirWithComplexTypes.getCanonicalPath}'
    """)

    sql(s"""
      CREATE EXTERNAL TABLE partitioned_parquet_with_key_and_complextypes
      (
        intField INT,
        stringField STRING,
        structField STRUCT<intStructField: INT, stringStructField: STRING>,
        arrayField ARRAY<INT>
      )
      PARTITIONED BY (p int)
      ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
       STORED AS
       INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
       OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      LOCATION '${partitionedTableDirWithKeyAndComplexTypes.getCanonicalPath}'
    """)

    sql(
      """
        |create table test_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet_with_key ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet_with_key_and_complextypes ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet_with_complextypes ADD PARTITION (p=$p)")
    }

    (1 to 10).map(i => (i, s"str$i")).toDF("a", "b").createOrReplaceTempView("jt")
    (1 to 10).map(i => Tuple1(Seq(new Integer(i), null))).toDF("a")
      .createOrReplaceTempView("jt_array")

    setConf(HiveUtils.CONVERT_METASTORE_PARQUET, true)
  }

  override def afterAll(): Unit = {
    dropTables("partitioned_parquet",
      "partitioned_parquet_with_key",
      "partitioned_parquet_with_complextypes",
      "partitioned_parquet_with_key_and_complextypes",
      "normal_parquet",
      "jt",
      "jt_array",
       "test_parquet")
    setConf(HiveUtils.CONVERT_METASTORE_PARQUET, false)
  }

  test(s"conversion is working") {
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.sparkPlan.collect {
        case _: HiveTableScanExec => true
      }.isEmpty)
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.sparkPlan.collect {
        case _: DataSourceScanExec => true
      }.nonEmpty)
  }

  test("scan an empty parquet table") {
    checkAnswer(sql("SELECT count(*) FROM test_parquet"), Row(0))
  }

  test("scan an empty parquet table with upper case") {
    checkAnswer(sql("SELECT count(INTFIELD) FROM TEST_parquet"), Row(0))
  }

  test("insert into an empty parquet table") {
    dropTables("test_insert_parquet")
    sql(
      """
        |create table test_insert_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    // Insert into am empty table.
    sql("insert into table test_insert_parquet select a, b from jt where jt.a > 5")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField < 8"),
      Row(6, "str6") :: Row(7, "str7") :: Nil
    )
    // Insert overwrite.
    sql("insert overwrite table test_insert_parquet select a, b from jt where jt.a < 5")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField > 2"),
      Row(3, "str3") :: Row(4, "str4") :: Nil
    )
    dropTables("test_insert_parquet")

    // Create it again.
    sql(
      """
        |create table test_insert_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)
    // Insert overwrite an empty table.
    sql("insert overwrite table test_insert_parquet select a, b from jt where jt.a < 5")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField > 2"),
      Row(3, "str3") :: Row(4, "str4") :: Nil
    )
    // Insert into the table.
    sql("insert into table test_insert_parquet select a, b from jt")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet"),
      (1 to 10).map(i => Row(i, s"str$i")) ++ (1 to 4).map(i => Row(i, s"str$i"))
    )
    dropTables("test_insert_parquet")
  }

  test("scan a parquet table created through a CTAS statement") {
    withTable("test_parquet_ctas") {
      sql(
        """
          |create table test_parquet_ctas ROW FORMAT
          |SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
          |AS select * from jt
        """.stripMargin)

      checkAnswer(
        sql(s"SELECT a, b FROM test_parquet_ctas WHERE a = 1"),
        Seq(Row(1, "str1"))
      )

      table("test_parquet_ctas").queryExecution.optimizedPlan match {
        case LogicalRelation(_: HadoopFsRelation, _, _) => // OK
        case _ => fail(
          "test_parquet_ctas should be converted to " +
              s"${classOf[HadoopFsRelation ].getCanonicalName }")
      }
    }
  }

  test("MetastoreRelation in InsertIntoTable will be converted") {
    withTable("test_insert_parquet") {
      sql(
        """
          |create table test_insert_parquet
          |(
          |  intField INT
          |)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)

      val df = sql("INSERT INTO TABLE test_insert_parquet SELECT a FROM jt")
      df.queryExecution.sparkPlan match {
        case ExecutedCommandExec(_: InsertIntoHadoopFsRelationCommand) => // OK
        case o => fail("test_insert_parquet should be converted to a " +
          s"${classOf[HadoopFsRelation ].getCanonicalName} and " +
          s"${classOf[InsertIntoDataSourceCommand].getCanonicalName} should have been SparkPlan. " +
          s"However, found a ${o.toString} ")
      }

      checkAnswer(
        sql("SELECT intField FROM test_insert_parquet WHERE test_insert_parquet.intField > 5"),
        sql("SELECT a FROM jt WHERE jt.a > 5").collect()
      )
    }
  }

  test("MetastoreRelation in InsertIntoHiveTable will be converted") {
    withTable("test_insert_parquet") {
      sql(
        """
          |create table test_insert_parquet
          |(
          |  int_array array<int>
          |)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)

      val df = sql("INSERT INTO TABLE test_insert_parquet SELECT a FROM jt_array")
      df.queryExecution.sparkPlan match {
        case ExecutedCommandExec(_: InsertIntoHadoopFsRelationCommand) => // OK
        case o => fail("test_insert_parquet should be converted to a " +
          s"${classOf[HadoopFsRelation ].getCanonicalName} and " +
          s"${classOf[InsertIntoDataSourceCommand].getCanonicalName} should have been SparkPlan." +
          s"However, found a ${o.toString} ")
      }

      checkAnswer(
        sql("SELECT int_array FROM test_insert_parquet"),
        sql("SELECT a FROM jt_array").collect()
      )
    }
  }

  test("SPARK-6450 regression test") {
    withTable("ms_convert") {
      sql(
        """CREATE TABLE IF NOT EXISTS ms_convert (key INT)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)

      // This shouldn't throw AnalysisException
      val analyzed = sql(
        """SELECT key FROM ms_convert
          |UNION ALL
          |SELECT key FROM ms_convert
        """.stripMargin).queryExecution.analyzed

      assertResult(2) {
        analyzed.collect {
          case r @ LogicalRelation(_: HadoopFsRelation, _, _) => r
        }.size
      }
    }
  }

  def collectHadoopFsRelation(df: DataFrame): HadoopFsRelation = {
    val plan = df.queryExecution.analyzed
    plan.collectFirst {
      case LogicalRelation(r: HadoopFsRelation, _, _) => r
    }.getOrElse {
      fail(s"Expecting a HadoopFsRelation 2, but got:\n$plan")
    }
  }

  test("SPARK-7749: non-partitioned metastore Parquet table lookup should use cached relation") {
    withTable("nonPartitioned") {
      sql(
        """
          |CREATE TABLE nonPartitioned (
          |  key INT,
          |  value STRING
          |)
          |STORED AS PARQUET
        """.stripMargin)

      // First lookup fills the cache
      val r1 = collectHadoopFsRelation(table("nonPartitioned"))
      // Second lookup should reuse the cache
      val r2 = collectHadoopFsRelation(table("nonPartitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  test("SPARK-7749: partitioned metastore Parquet table lookup should use cached relation") {
    withTable("partitioned") {
      sql(
        """
          |CREATE TABLE partitioned (
          |  key INT,
          |  value STRING
          |)
          |PARTITIONED BY (part INT)
          |STORED AS PARQUET
        """.stripMargin)

      // First lookup fills the cache
      val r1 = collectHadoopFsRelation(table("partitioned"))
      // Second lookup should reuse the cache
      val r2 = collectHadoopFsRelation(table("partitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  test("SPARK-15968: nonempty partitioned metastore Parquet table lookup should use cached " +
      "relation") {
    withTable("partitioned") {
      sql(
        """
          |CREATE TABLE partitioned (
          |  key INT,
          |  value STRING
          |)
          |PARTITIONED BY (part INT)
          |STORED AS PARQUET
        """.stripMargin)
      sql("INSERT INTO TABLE partitioned PARTITION(part=0) SELECT 1 as key, 'one' as value")

      // First lookup fills the cache
      val r1 = collectHadoopFsRelation(table("partitioned"))
      // Second lookup should reuse the cache
      val r2 = collectHadoopFsRelation(table("partitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  test("Caching converted data source Parquet Relations") {
    def checkCached(tableIdentifier: TableIdentifier): Unit = {
      // Converted test_parquet should be cached.
      sessionState.catalog.getCachedDataSourceTable(tableIdentifier) match {
        case null => fail("Converted test_parquet should be cached in the cache.")
        case logical @ LogicalRelation(parquetRelation: HadoopFsRelation, _, _) => // OK
        case other =>
          fail(
            "The cached test_parquet should be a Parquet Relation. " +
              s"However, $other is returned form the cache.")
      }
    }

    dropTables("test_insert_parquet", "test_parquet_partitioned_cache_test")

    sql(
      """
        |create table test_insert_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    var tableIdentifier = TableIdentifier("test_insert_parquet", Some("default"))

    // First, make sure the converted test_parquet is not cached.
    assert(sessionState.catalog.getCachedDataSourceTable(tableIdentifier) === null)
    // Table lookup will make the table cached.
    table("test_insert_parquet")
    checkCached(tableIdentifier)
    // For insert into non-partitioned table, we will do the conversion,
    // so the converted test_insert_parquet should be cached.
    sessionState.refreshTable("test_insert_parquet")
    assert(sessionState.catalog.getCachedDataSourceTable(tableIdentifier) === null)
    sql(
      """
        |INSERT INTO TABLE test_insert_parquet
        |select a, b from jt
      """.stripMargin)
    checkCached(tableIdentifier)
    // Make sure we can read the data.
    checkAnswer(
      sql("select * from test_insert_parquet"),
      sql("select a, b from jt").collect())
    // Invalidate the cache.
    sessionState.refreshTable("test_insert_parquet")
    assert(sessionState.catalog.getCachedDataSourceTable(tableIdentifier) === null)

    // Create a partitioned table.
    sql(
      """
        |create table test_parquet_partitioned_cache_test
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |PARTITIONED BY (`date` string)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    tableIdentifier = TableIdentifier("test_parquet_partitioned_cache_test", Some("default"))
    assert(sessionState.catalog.getCachedDataSourceTable(tableIdentifier) === null)
    sql(
      """
        |INSERT INTO TABLE test_parquet_partitioned_cache_test
        |PARTITION (`date`='2015-04-01')
        |select a, b from jt
      """.stripMargin)
    // Right now, insert into a partitioned Parquet is not supported in data source Parquet.
    // So, we expect it is not cached.
    assert(sessionState.catalog.getCachedDataSourceTable(tableIdentifier) === null)
    sql(
      """
        |INSERT INTO TABLE test_parquet_partitioned_cache_test
        |PARTITION (`date`='2015-04-02')
        |select a, b from jt
      """.stripMargin)
    assert(sessionState.catalog.getCachedDataSourceTable(tableIdentifier) === null)

    // Make sure we can cache the partitioned table.
    table("test_parquet_partitioned_cache_test")
    checkCached(tableIdentifier)
    // Make sure we can read the data.
    checkAnswer(
      sql("select STRINGField, `date`, intField from test_parquet_partitioned_cache_test"),
      sql(
        """
          |select b, '2015-04-01', a FROM jt
          |UNION ALL
          |select b, '2015-04-02', a FROM jt
        """.stripMargin).collect())

    sessionState.refreshTable("test_parquet_partitioned_cache_test")
    assert(sessionState.catalog.getCachedDataSourceTable(tableIdentifier) === null)

    dropTables("test_insert_parquet", "test_parquet_partitioned_cache_test")
  }

  test("SPARK-15248: explicitly added partitions should be readable") {
    withTable("test_added_partitions", "test_temp") {
      withTempDir { src =>
        val partitionDir = new File(src, "partition").getCanonicalPath
        sql(
          """
            |CREATE TABLE test_added_partitions (a STRING)
            |PARTITIONED BY (b INT)
            |STORED AS PARQUET
          """.stripMargin)

        // Temp view that is used to insert data into partitioned table
        Seq("foo", "bar").toDF("a").createOrReplaceTempView("test_temp")
        sql("INSERT INTO test_added_partitions PARTITION(b='0') SELECT a FROM test_temp")

        checkAnswer(
          sql("SELECT * FROM test_added_partitions"),
          Seq(("foo", 0), ("bar", 0)).toDF("a", "b"))

        // Create partition without data files and check whether it can be read
        sql(s"ALTER TABLE test_added_partitions ADD PARTITION (b='1') LOCATION '$partitionDir'")
        checkAnswer(
          sql("SELECT * FROM test_added_partitions"),
          Seq(("foo", 0), ("bar", 0)).toDF("a", "b"))

        // Add data files to partition directory and check whether they can be read
        sql("INSERT INTO TABLE test_added_partitions PARTITION (b=1) select 'baz' as a")
        checkAnswer(
          sql("SELECT * FROM test_added_partitions"),
          Seq(("foo", 0), ("bar", 0), ("baz", 1)).toDF("a", "b"))
      }
    }
  }

  test("self-join") {
    val table = spark.table("normal_parquet")
    val selfJoin = table.as("t1").crossJoin(table.as("t2"))
    checkAnswer(selfJoin,
      sql("SELECT * FROM normal_parquet x CROSS JOIN normal_parquet y"))
  }
}

/**
 * A suite of tests for the Parquet support through the data sources API.
 */
class ParquetSourceSuite extends ParquetPartitioningTest {
  import testImplicits._
  import spark._

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropTables("partitioned_parquet",
      "partitioned_parquet_with_key",
      "partitioned_parquet_with_complextypes",
      "partitioned_parquet_with_key_and_complextypes",
      "normal_parquet")

    sql( s"""
      CREATE TEMPORARY VIEW partitioned_parquet
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDir.getCanonicalPath}'
      )
    """)

    sql( s"""
      CREATE TEMPORARY VIEW partitioned_parquet_with_key
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithKey.getCanonicalPath}'
      )
    """)

    sql( s"""
      CREATE TEMPORARY VIEW normal_parquet
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${new File(partitionedTableDir, "p=1").getCanonicalPath}'
      )
    """)

    sql( s"""
      CREATE TEMPORARY VIEW partitioned_parquet_with_key_and_complextypes
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithKeyAndComplexTypes.getCanonicalPath}'
      )
    """)

    sql( s"""
      CREATE TEMPORARY VIEW partitioned_parquet_with_complextypes
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithComplexTypes.getCanonicalPath}'
      )
    """)
  }

  test("SPARK-6016 make sure to use the latest footers") {
    sql("drop table if exists spark_6016_fix")

    // Create a DataFrame with two partitions. So, the created table will have two parquet files.
    val df1 = (1 to 10).map(Tuple1(_)).toDF("a").coalesce(2)
    df1.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("spark_6016_fix")
    checkAnswer(
      sql("select * from spark_6016_fix"),
      (1 to 10).map(i => Row(i))
    )

    // Create a DataFrame with four partitions. So, the created table will have four parquet files.
    val df2 = (1 to 10).map(Tuple1(_)).toDF("b").coalesce(4)
    df2.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("spark_6016_fix")
    // For the bug of SPARK-6016, we are caching two outdated footers for df1. Then,
    // since the new table has four parquet files, we are trying to read new footers from two files
    // and then merge metadata in footers of these four (two outdated ones and two latest one),
    // which will cause an error.
    checkAnswer(
      sql("select * from spark_6016_fix"),
      (1 to 10).map(i => Row(i))
    )

    sql("drop table spark_6016_fix")
  }

  test("SPARK-8811: compatibility with array of struct in Hive") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("array_of_struct") {
        val conf = Seq(
          HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false",
          SQLConf.PARQUET_BINARY_AS_STRING.key -> "true",
          SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false")

        withSQLConf(conf: _*) {
          sql(
            s"""CREATE TABLE array_of_struct
               |STORED AS PARQUET LOCATION '$path'
               |AS SELECT
               |  '1st' AS a,
               |  '2nd' AS b,
               |  ARRAY(NAMED_STRUCT('a', 'val_a', 'b', 'val_b')) AS c
             """.stripMargin)

          checkAnswer(
            spark.read.parquet(path),
            Row("1st", "2nd", Seq(Row("val_a", "val_b"))))
        }
      }
    }
  }

  test("Verify the PARQUET conversion parameter: CONVERT_METASTORE_PARQUET") {
    withTempView("single") {
      val singleRowDF = Seq((0, "foo")).toDF("key", "value")
      singleRowDF.createOrReplaceTempView("single")

      Seq("true", "false").foreach { parquetConversion =>
        withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> parquetConversion) {
          val tableName = "test_parquet_ctas"
          withTable(tableName) {
            sql(
              s"""
                 |CREATE TABLE $tableName STORED AS PARQUET
                 |AS SELECT tmp.key, tmp.value FROM single tmp
               """.stripMargin)

            val df = spark.sql(s"SELECT * FROM $tableName WHERE key=0")
            checkAnswer(df, singleRowDF)

            val queryExecution = df.queryExecution
            if (parquetConversion == "true") {
              queryExecution.analyzed.collectFirst {
                case _: LogicalRelation =>
              }.getOrElse {
                fail(s"Expecting the query plan to convert parquet to data sources, " +
                  s"but got:\n$queryExecution")
              }
            } else {
              queryExecution.analyzed.collectFirst {
                case _: MetastoreRelation =>
              }.getOrElse {
                fail(s"Expecting no conversion from parquet to data sources, " +
                  s"but got:\n$queryExecution")
              }
            }
          }
        }
      }
    }
  }

  test("values in arrays and maps stored in parquet are always nullable") {
    val df = createDataFrame(Tuple2(Map(2 -> 3), Seq(4, 5, 6)) :: Nil).toDF("m", "a")
    val mapType1 = MapType(IntegerType, IntegerType, valueContainsNull = false)
    val arrayType1 = ArrayType(IntegerType, containsNull = false)
    val expectedSchema1 =
      StructType(
        StructField("m", mapType1, nullable = true) ::
          StructField("a", arrayType1, nullable = true) :: Nil)
    assert(df.schema === expectedSchema1)

    withTable("alwaysNullable") {
      df.write.format("parquet").saveAsTable("alwaysNullable")

      val mapType2 = MapType(IntegerType, IntegerType, valueContainsNull = true)
      val arrayType2 = ArrayType(IntegerType, containsNull = true)
      val expectedSchema2 =
        StructType(
          StructField("m", mapType2, nullable = true) ::
              StructField("a", arrayType2, nullable = true) :: Nil)

      assert(table("alwaysNullable").schema === expectedSchema2)

      checkAnswer(
        sql("SELECT m, a FROM alwaysNullable"),
        Row(Map(2 -> 3), Seq(4, 5, 6)))
    }
  }

  test("Aggregation attribute names can't contain special chars \" ,;{}()\\n\\t=\"") {
    val tempDir = Utils.createTempDir()
    val filePath = new File(tempDir, "testParquet").getCanonicalPath
    val filePath2 = new File(tempDir, "testParquet2").getCanonicalPath

    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = df.as('x).join(df.as('y), $"x.str" === $"y.str").groupBy("y.str").max("y.int")
    intercept[Throwable](df2.write.parquet(filePath))

    val df3 = df2.toDF("str", "max_int")
    df3.write.parquet(filePath2)
    val df4 = read.parquet(filePath2)
    checkAnswer(df4, Row("1", 1) :: Row("2", 2) :: Row("3", 3) :: Nil)
    assert(df4.columns === Array("str", "max_int"))
  }
}

/**
 * A collection of tests for parquet data with various forms of partitioning.
 */
abstract class ParquetPartitioningTest extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  var partitionedTableDir: File = null
  var normalTableDir: File = null
  var partitionedTableDirWithKey: File = null
  var partitionedTableDirWithComplexTypes: File = null
  var partitionedTableDirWithKeyAndComplexTypes: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    partitionedTableDir = Utils.createTempDir()
    normalTableDir = Utils.createTempDir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDir, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetData(i, s"part-$p"))
        .toDF()
        .write.parquet(partDir.getCanonicalPath)
    }

    sparkContext
      .makeRDD(1 to 10)
      .map(i => ParquetData(i, s"part-1"))
      .toDF()
      .write.parquet(new File(normalTableDir, "normal").getCanonicalPath)

    partitionedTableDirWithKey = Utils.createTempDir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKey, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetDataWithKey(p, i, s"part-$p"))
        .toDF()
        .write.parquet(partDir.getCanonicalPath)
    }

    partitionedTableDirWithKeyAndComplexTypes = Utils.createTempDir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKeyAndComplexTypes, s"p=$p")
      sparkContext.makeRDD(1 to 10).map { i =>
        ParquetDataWithKeyAndComplexTypes(
          p, i, s"part-$p", StructContainer(i, f"${i}_string"), 1 to i)
      }.toDF().write.parquet(partDir.getCanonicalPath)
    }

    partitionedTableDirWithComplexTypes = Utils.createTempDir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithComplexTypes, s"p=$p")
      sparkContext.makeRDD(1 to 10).map { i =>
        ParquetDataWithComplexTypes(i, s"part-$p", StructContainer(i, f"${i}_string"), 1 to i)
      }.toDF().write.parquet(partDir.getCanonicalPath)
    }
  }

  override protected def afterAll(): Unit = {
    partitionedTableDir.delete()
    normalTableDir.delete()
    partitionedTableDirWithKey.delete()
    partitionedTableDirWithComplexTypes.delete()
    partitionedTableDirWithKeyAndComplexTypes.delete()
  }

  /**
   * Drop named tables if they exist
 *
   * @param tableNames tables to drop
   */
  def dropTables(tableNames: String*): Unit = {
    tableNames.foreach { name =>
      sql(s"DROP TABLE IF EXISTS $name")
    }
  }

  Seq(
    "partitioned_parquet",
    "partitioned_parquet_with_key",
    "partitioned_parquet_with_complextypes",
    "partitioned_parquet_with_key_and_complextypes").foreach { table =>

    test(s"ordering of the partitioning columns $table") {
      checkAnswer(
        sql(s"SELECT p, stringField FROM $table WHERE p = 1"),
        Seq.fill(10)(Row(1, "part-1"))
      )

      checkAnswer(
        sql(s"SELECT stringField, p FROM $table WHERE p = 1"),
        Seq.fill(10)(Row("part-1", 1))
      )
    }

    test(s"project the partitioning column $table") {
      checkAnswer(
        sql(s"SELECT p, count(*) FROM $table group by p"),
        Row(1, 10) ::
          Row(2, 10) ::
          Row(3, 10) ::
          Row(4, 10) ::
          Row(5, 10) ::
          Row(6, 10) ::
          Row(7, 10) ::
          Row(8, 10) ::
          Row(9, 10) ::
          Row(10, 10) :: Nil
      )
    }

    test(s"project partitioning and non-partitioning columns $table") {
      checkAnswer(
        sql(s"SELECT stringField, p, count(intField) FROM $table GROUP BY p, stringField"),
        Row("part-1", 1, 10) ::
          Row("part-2", 2, 10) ::
          Row("part-3", 3, 10) ::
          Row("part-4", 4, 10) ::
          Row("part-5", 5, 10) ::
          Row("part-6", 6, 10) ::
          Row("part-7", 7, 10) ::
          Row("part-8", 8, 10) ::
          Row("part-9", 9, 10) ::
          Row("part-10", 10, 10) :: Nil
      )
    }

    test(s"simple count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table"),
        Row(100))
    }

    test(s"pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p = 1"),
        Row(10))
    }

    test(s"non-existent partition $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p = 1000"),
        Row(0))
    }

    test(s"multi-partition pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p IN (1,2,3)"),
        Row(30))
    }

    test(s"non-partition predicates $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE intField IN (1,2,3)"),
        Row(30))
    }

    test(s"sum $table") {
      checkAnswer(
        sql(s"SELECT SUM(intField) FROM $table WHERE intField IN (1,2,3) AND p = 1"),
        Row(1 + 2 + 3))
    }

    test(s"hive udfs $table") {
      checkAnswer(
        sql(s"SELECT concat(stringField, stringField) FROM $table"),
        sql(s"SELECT stringField FROM $table").rdd.map {
          case Row(s: String) => Row(s + s)
        }.collect().toSeq)
    }
  }

  Seq(
    "partitioned_parquet_with_key_and_complextypes",
    "partitioned_parquet_with_complextypes").foreach { table =>

    test(s"SPARK-5775 read struct from $table") {
      checkAnswer(
        sql(
          s"""
             |SELECT p, structField.intStructField, structField.stringStructField
             |FROM $table WHERE p = 1
           """.stripMargin),
        (1 to 10).map(i => Row(1, i, f"${i}_string")))
    }

    test(s"SPARK-5775 read array from $table") {
      checkAnswer(
        sql(s"SELECT arrayField, p FROM $table WHERE p = 1"),
        (1 to 10).map(i => Row(1 to i, 1)))
    }
  }


  test("non-part select(*)") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM normal_parquet"),
      Row(10))
  }
}
