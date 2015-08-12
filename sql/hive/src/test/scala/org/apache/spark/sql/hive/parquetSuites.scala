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
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSource, InsertIntoHadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.{ExecutedCommand, PhysicalRDD}
import org.apache.spark.sql.hive.execution.HiveTableScan
import org.apache.spark.sql.hive.test.SharedHiveContext
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
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
    ctx.sql(s"""
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

    ctx.sql(s"""
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

    ctx.sql(s"""
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

    ctx.sql(s"""
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

    ctx.sql(s"""
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

    ctx.sql(
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
      ctx.sql(s"ALTER TABLE partitioned_parquet ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      ctx.sql(s"ALTER TABLE partitioned_parquet_with_key ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      ctx.sql(s"ALTER TABLE partitioned_parquet_with_key_and_complextypes ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      ctx.sql(s"ALTER TABLE partitioned_parquet_with_complextypes ADD PARTITION (p=$p)")
    }

    val rdd1 = ctx.sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str$i"}"""))
    ctx.read.json(rdd1).registerTempTable("jt")
    val rdd2 = ctx.sparkContext.parallelize((1 to 10).map(i => s"""{"a":[$i, null]}"""))
    ctx.read.json(rdd2).registerTempTable("jt_array")

    ctx.setConf(HiveContext.CONVERT_METASTORE_PARQUET, true)
  }

  override def afterAll(): Unit = {
    try {
      dropTables("partitioned_parquet",
        "partitioned_parquet_with_key",
        "partitioned_parquet_with_complextypes",
        "partitioned_parquet_with_key_and_complextypes",
        "normal_parquet",
        "jt",
        "jt_array",
        "test_parquet")
      ctx.setConf(HiveContext.CONVERT_METASTORE_PARQUET, false)
    } finally {
      super.afterAll()
    }
  }

  test(s"conversion is working") {
    assert(
      ctx.sql("SELECT * FROM normal_parquet").queryExecution.executedPlan.collect {
        case _: HiveTableScan => true
      }.isEmpty)
    assert(
      ctx.sql("SELECT * FROM normal_parquet").queryExecution.executedPlan.collect {
        case _: PhysicalRDD => true
      }.nonEmpty)
  }

  test("scan an empty parquet table") {
    checkAnswer(ctx.sql("SELECT count(*) FROM test_parquet"), Row(0))
  }

  test("scan an empty parquet table with upper case") {
    checkAnswer(ctx.sql("SELECT count(INTFIELD) FROM TEST_parquet"), Row(0))
  }

  test("insert into an empty parquet table") {
    dropTables("test_insert_parquet")
    ctx.sql(
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
    ctx.sql("insert into table test_insert_parquet select a, b from jt where jt.a > 5")
    checkAnswer(
      ctx.sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField < 8"),
      Row(6, "str6") :: Row(7, "str7") :: Nil
    )
    // Insert overwrite.
    ctx.sql("insert overwrite table test_insert_parquet select a, b from jt where jt.a < 5")
    checkAnswer(
      ctx.sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField > 2"),
      Row(3, "str3") :: Row(4, "str4") :: Nil
    )
    dropTables("test_insert_parquet")

    // Create it again.
    ctx.sql(
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
    ctx.sql("insert overwrite table test_insert_parquet select a, b from jt where jt.a < 5")
    checkAnswer(
      ctx.sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField > 2"),
      Row(3, "str3") :: Row(4, "str4") :: Nil
    )
    // Insert into the table.
    ctx.sql("insert into table test_insert_parquet select a, b from jt")
    checkAnswer(
      ctx.sql(s"SELECT intField, stringField FROM test_insert_parquet"),
      (1 to 10).map(i => Row(i, s"str$i")) ++ (1 to 4).map(i => Row(i, s"str$i"))
    )
    dropTables("test_insert_parquet")
  }

  test("scan a parquet table created through a CTAS statement") {
    withTable("test_parquet_ctas") {
      ctx.sql(
        """
          |create table test_parquet_ctas ROW FORMAT
          |SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
          |AS select * from jt
        """.stripMargin)

      checkAnswer(
        ctx.sql(s"SELECT a, b FROM test_parquet_ctas WHERE a = 1"),
        Seq(Row(1, "str1"))
      )

      ctx.table("test_parquet_ctas").queryExecution.optimizedPlan match {
        case LogicalRelation(_: ParquetRelation) => // OK
        case _ => fail(
          "test_parquet_ctas should be converted to " +
              s"${classOf[ParquetRelation].getCanonicalName }")
      }
    }
  }

  test("MetastoreRelation in InsertIntoTable will be converted") {
    withTable("test_insert_parquet") {
      ctx.sql(
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

      val df = ctx.sql("INSERT INTO TABLE test_insert_parquet SELECT a FROM jt")
      df.queryExecution.executedPlan match {
        case ExecutedCommand(InsertIntoHadoopFsRelation(_: ParquetRelation, _, _)) => // OK
        case o => fail("test_insert_parquet should be converted to a " +
          s"${classOf[ParquetRelation].getCanonicalName} and " +
          s"${classOf[InsertIntoDataSource].getCanonicalName} is expcted as the SparkPlan. " +
          s"However, found a ${o.toString} ")
      }

      checkAnswer(
        ctx.sql("SELECT intField FROM test_insert_parquet WHERE test_insert_parquet.intField > 5"),
        ctx.sql("SELECT a FROM jt WHERE jt.a > 5").collect()
      )
    }
  }

  test("MetastoreRelation in InsertIntoHiveTable will be converted") {
    withTable("test_insert_parquet") {
      ctx.sql(
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

      val df = ctx.sql("INSERT INTO TABLE test_insert_parquet SELECT a FROM jt_array")
      df.queryExecution.executedPlan match {
        case ExecutedCommand(InsertIntoHadoopFsRelation(r: ParquetRelation, _, _)) => // OK
        case o => fail("test_insert_parquet should be converted to a " +
          s"${classOf[ParquetRelation].getCanonicalName} and " +
          s"${classOf[InsertIntoDataSource].getCanonicalName} is expcted as the SparkPlan." +
          s"However, found a ${o.toString} ")
      }

      checkAnswer(
        ctx.sql("SELECT int_array FROM test_insert_parquet"),
        ctx.sql("SELECT a FROM jt_array").collect()
      )
    }
  }

  test("SPARK-6450 regression test") {
    withTable("ms_convert") {
      ctx.sql(
        """CREATE TABLE IF NOT EXISTS ms_convert (key INT)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)

      // This shouldn't throw AnalysisException
      val analyzed = ctx.sql(
        """SELECT key FROM ms_convert
          |UNION ALL
          |SELECT key FROM ms_convert
        """.stripMargin).queryExecution.analyzed

      assertResult(2) {
        analyzed.collect {
          case r@LogicalRelation(_: ParquetRelation) => r
        }.size
      }
    }
  }

  def collectParquetRelation(df: DataFrame): ParquetRelation = {
    val plan = df.queryExecution.analyzed
    plan.collectFirst {
      case LogicalRelation(r: ParquetRelation) => r
    }.getOrElse {
      fail(s"Expecting a ParquetRelation2, but got:\n$plan")
    }
  }

  test("SPARK-7749: non-partitioned metastore Parquet table lookup should use cached relation") {
    withTable("nonPartitioned") {
      ctx.sql(
        s"""CREATE TABLE nonPartitioned (
           |  key INT,
           |  value STRING
           |)
           |STORED AS PARQUET
         """.stripMargin)

      // First lookup fills the cache
      val r1 = collectParquetRelation(ctx.table("nonPartitioned"))
      // Second lookup should reuse the cache
      val r2 = collectParquetRelation(ctx.table("nonPartitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  test("SPARK-7749: partitioned metastore Parquet table lookup should use cached relation") {
    withTable("partitioned") {
      ctx.sql(
        s"""CREATE TABLE partitioned (
           | key INT,
           | value STRING
           |)
           |PARTITIONED BY (part INT)
           |STORED AS PARQUET
       """.stripMargin)

      // First lookup fills the cache
      val r1 = collectParquetRelation(ctx.table("partitioned"))
      // Second lookup should reuse the cache
      val r2 = collectParquetRelation(ctx.table("partitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  test("Caching converted data source Parquet Relations") {
    val _ctx = ctx
    def checkCached(tableIdentifier: _ctx.catalog.QualifiedTableName): Unit = {
      // Converted test_parquet should be cached.
      ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) match {
        case null => fail("Converted test_parquet should be cached in the cache.")
        case logical @ LogicalRelation(parquetRelation: ParquetRelation) => // OK
        case other =>
          fail(
            "The cached test_parquet should be a Parquet Relation. " +
              s"However, $other is returned form the cache.")
      }
    }

    dropTables("test_insert_parquet", "test_parquet_partitioned_cache_test")

    ctx.sql(
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

    var tableIdentifier = _ctx.catalog.QualifiedTableName("default", "test_insert_parquet")

    // First, make sure the converted test_parquet is not cached.
    assert(ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) === null)
    // Table lookup will make the table cached.
    ctx.table("test_insert_parquet")
    checkCached(tableIdentifier)
    // For insert into non-partitioned table, we will do the conversion,
    // so the converted test_insert_parquet should be cached.
    ctx.invalidateTable("test_insert_parquet")
    assert(ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) === null)
    ctx.sql(
      """
        |INSERT INTO TABLE test_insert_parquet
        |select a, b from jt
      """.stripMargin)
    checkCached(tableIdentifier)
    // Make sure we can read the data.
    checkAnswer(
      ctx.sql("select * from test_insert_parquet"),
      ctx.sql("select a, b from jt").collect())
    // Invalidate the cache.
    ctx.invalidateTable("test_insert_parquet")
    assert(ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) === null)

    // Create a partitioned table.
    ctx.sql(
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

    tableIdentifier = _ctx.catalog.QualifiedTableName(
      "default", "test_parquet_partitioned_cache_test")
    assert(ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) === null)
    ctx.sql(
      """
        |INSERT INTO TABLE test_parquet_partitioned_cache_test
        |PARTITION (`date`='2015-04-01')
        |select a, b from jt
      """.stripMargin)
    // Right now, insert into a partitioned Parquet is not supported in data source Parquet.
    // So, we expect it is not cached.
    assert(ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) === null)
    ctx.sql(
      """
        |INSERT INTO TABLE test_parquet_partitioned_cache_test
        |PARTITION (`date`='2015-04-02')
        |select a, b from jt
      """.stripMargin)
    assert(ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) === null)

    // Make sure we can cache the partitioned table.
    ctx.table("test_parquet_partitioned_cache_test")
    checkCached(tableIdentifier)
    // Make sure we can read the data.
    checkAnswer(
      ctx.sql("select STRINGField, `date`, intField from test_parquet_partitioned_cache_test"),
      ctx.sql(
        """
          |select b, '2015-04-01', a FROM jt
          |UNION ALL
          |select b, '2015-04-02', a FROM jt
        """.stripMargin).collect())

    ctx.invalidateTable("test_parquet_partitioned_cache_test")
    assert(ctx.catalog.cachedDataSourceTables.getIfPresent(tableIdentifier) === null)

    dropTables("test_insert_parquet", "test_parquet_partitioned_cache_test")
  }
}

/**
 * A suite of tests for the Parquet support through the data sources API.
 */
class ParquetSourceSuite extends ParquetPartitioningTest {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropTables("partitioned_parquet",
      "partitioned_parquet_with_key",
      "partitioned_parquet_with_complextypes",
      "partitioned_parquet_with_key_and_complextypes",
      "normal_parquet")

    ctx.sql( s"""
      create temporary table partitioned_parquet
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDir.getCanonicalPath}'
      )
    """)

    ctx.sql( s"""
      create temporary table partitioned_parquet_with_key
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithKey.getCanonicalPath}'
      )
    """)

    ctx.sql( s"""
      create temporary table normal_parquet
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${new File(partitionedTableDir, "p=1").getCanonicalPath}'
      )
    """)

    ctx.sql( s"""
      CREATE TEMPORARY TABLE partitioned_parquet_with_key_and_complextypes
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithKeyAndComplexTypes.getCanonicalPath}'
      )
    """)

    ctx.sql( s"""
      CREATE TEMPORARY TABLE partitioned_parquet_with_complextypes
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithComplexTypes.getCanonicalPath}'
      )
    """)
  }

  test("SPARK-6016 make sure to use the latest footers") {
    ctx.sql("drop table if exists spark_6016_fix")

    // Create a DataFrame with two partitions. So, the created table will have two parquet files.
    val df1 = ctx.read.json(ctx.sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i}"""), 2))
    df1.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("spark_6016_fix")
    checkAnswer(
      ctx.sql("select * from spark_6016_fix"),
      (1 to 10).map(i => Row(i))
    )

    // Create a DataFrame with four partitions. So, the created table will have four parquet files.
    val df2 = ctx.read.json(ctx.sparkContext.parallelize((1 to 10).map(i => s"""{"b":$i}"""), 4))
    df2.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("spark_6016_fix")
    // For the bug of SPARK-6016, we are caching two outdated footers for df1. Then,
    // since the new table has four parquet files, we are trying to read new footers from two files
    // and then merge metadata in footers of these four (two outdated ones and two latest one),
    // which will cause an error.
    checkAnswer(
      ctx.sql("select * from spark_6016_fix"),
      (1 to 10).map(i => Row(i))
    )

    ctx.sql("drop table spark_6016_fix")
  }

  test("SPARK-8811: compatibility with array of struct in Hive") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("array_of_struct") {
        val conf = Seq(
          HiveContext.CONVERT_METASTORE_PARQUET.key -> "false",
          SQLConf.PARQUET_BINARY_AS_STRING.key -> "true",
          SQLConf.PARQUET_FOLLOW_PARQUET_FORMAT_SPEC.key -> "true")

        withSQLConf(conf: _*) {
          ctx.sql(
            s"""CREATE TABLE array_of_struct
               |STORED AS PARQUET LOCATION '$path'
               |AS SELECT '1st', '2nd', ARRAY(NAMED_STRUCT('a', 'val_a', 'b', 'val_b'))
             """.stripMargin)

          checkAnswer(
            ctx.read.parquet(path),
            Row("1st", "2nd", Seq(Row("val_a", "val_b"))))
        }
      }
    }
  }

  test("values in arrays and maps stored in parquet are always nullable") {
    val df = ctx.createDataFrame(Tuple2(Map(2 -> 3), Seq(4, 5, 6)) :: Nil).toDF("m", "a")
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

      assert(ctx.table("alwaysNullable").schema === expectedSchema2)

      checkAnswer(
        ctx.sql("SELECT m, a FROM alwaysNullable"),
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
    val df4 = ctx.read.parquet(filePath2)
    checkAnswer(df4, Row("1", 1) :: Row("2", 2) :: Row("3", 3) :: Nil)
    assert(df4.columns === Array("str", "max_int"))
  }
}

/**
 * A collection of tests for parquet data with various forms of partitioning.
 */
abstract class ParquetPartitioningTest extends QueryTest with SharedHiveContext {
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
      ctx.sparkContext.makeRDD(1 to 10)
        .map(i => ParquetData(i, s"part-$p"))
        .toDF()
        .write.parquet(partDir.getCanonicalPath)
    }

    ctx.sparkContext
      .makeRDD(1 to 10)
      .map(i => ParquetData(i, s"part-1"))
      .toDF()
      .write.parquet(new File(normalTableDir, "normal").getCanonicalPath)

    partitionedTableDirWithKey = Utils.createTempDir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKey, s"p=$p")
      ctx.sparkContext.makeRDD(1 to 10)
        .map(i => ParquetDataWithKey(p, i, s"part-$p"))
        .toDF()
        .write.parquet(partDir.getCanonicalPath)
    }

    partitionedTableDirWithKeyAndComplexTypes = Utils.createTempDir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKeyAndComplexTypes, s"p=$p")
      ctx.sparkContext.makeRDD(1 to 10).map { i =>
        ParquetDataWithKeyAndComplexTypes(
          p, i, s"part-$p", StructContainer(i, f"${i}_string"), 1 to i)
      }.toDF().write.parquet(partDir.getCanonicalPath)
    }

    partitionedTableDirWithComplexTypes = Utils.createTempDir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithComplexTypes, s"p=$p")
      ctx.sparkContext.makeRDD(1 to 10).map { i =>
        ParquetDataWithComplexTypes(i, s"part-$p", StructContainer(i, f"${i}_string"), 1 to i)
      }.toDF().write.parquet(partDir.getCanonicalPath)
    }
  }

  override protected def afterAll(): Unit = {
    try {
      partitionedTableDir.delete()
      normalTableDir.delete()
      partitionedTableDirWithKey.delete()
      partitionedTableDirWithComplexTypes.delete()
      partitionedTableDirWithKeyAndComplexTypes.delete()
    } finally {
      super.afterAll()
    }
  }

  /**
   * Drop named tables if they exist
   * @param tableNames tables to drop
   */
  def dropTables(tableNames: String*): Unit = {
    tableNames.foreach { name =>
      ctx.sql(s"DROP TABLE IF EXISTS $name")
    }
  }

  Seq(
    "partitioned_parquet",
    "partitioned_parquet_with_key",
    "partitioned_parquet_with_complextypes",
    "partitioned_parquet_with_key_and_complextypes").foreach { table =>

    test(s"ordering of the partitioning columns $table") {
      checkAnswer(
        ctx.sql(s"SELECT p, stringField FROM $table WHERE p = 1"),
        Seq.fill(10)(Row(1, "part-1"))
      )

      checkAnswer(
        ctx.sql(s"SELECT stringField, p FROM $table WHERE p = 1"),
        Seq.fill(10)(Row("part-1", 1))
      )
    }

    test(s"project the partitioning column $table") {
      checkAnswer(
        ctx.sql(s"SELECT p, count(*) FROM $table group by p"),
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
        ctx.sql(s"SELECT stringField, p, count(intField) FROM $table GROUP BY p, stringField"),
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
        ctx.sql(s"SELECT COUNT(*) FROM $table"),
        Row(100))
    }

    test(s"pruned count $table") {
      checkAnswer(
        ctx.sql(s"SELECT COUNT(*) FROM $table WHERE p = 1"),
        Row(10))
    }

    test(s"non-existent partition $table") {
      checkAnswer(
        ctx.sql(s"SELECT COUNT(*) FROM $table WHERE p = 1000"),
        Row(0))
    }

    test(s"multi-partition pruned count $table") {
      checkAnswer(
        ctx.sql(s"SELECT COUNT(*) FROM $table WHERE p IN (1,2,3)"),
        Row(30))
    }

    test(s"non-partition predicates $table") {
      checkAnswer(
        ctx.sql(s"SELECT COUNT(*) FROM $table WHERE intField IN (1,2,3)"),
        Row(30))
    }

    test(s"sum $table") {
      checkAnswer(
        ctx.sql(s"SELECT SUM(intField) FROM $table WHERE intField IN (1,2,3) AND p = 1"),
        Row(1 + 2 + 3))
    }

    test(s"hive udfs $table") {
      checkAnswer(
        ctx.sql(s"SELECT concat(stringField, stringField) FROM $table"),
        ctx.sql(s"SELECT stringField FROM $table").map {
          case Row(s: String) => Row(s + s)
        }.collect().toSeq)
    }
  }

  Seq(
    "partitioned_parquet_with_key_and_complextypes",
    "partitioned_parquet_with_complextypes").foreach { table =>

    test(s"SPARK-5775 read struct from $table") {
      checkAnswer(
        ctx.sql(
          s"""
             |SELECT p, structField.intStructField, structField.stringStructField
             |FROM $table WHERE p = 1
           """.stripMargin),
        (1 to 10).map(i => Row(1, i, f"${i}_string")))
    }

    // Re-enable this after SPARK-5508 is fixed
    ignore(s"SPARK-5775 read array from $table") {
      checkAnswer(
        ctx.sql(s"SELECT arrayField, p FROM $table WHERE p = 1"),
        (1 to 10).map(i => Row(1 to i, 1)))
    }
  }


  test("non-part select(*)") {
    checkAnswer(
      ctx.sql("SELECT COUNT(*) FROM normal_parquet"),
      Row(10))
  }
}
