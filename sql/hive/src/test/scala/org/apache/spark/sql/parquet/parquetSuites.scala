
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

package org.apache.spark.sql.parquet

import java.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.execution.HiveTableScan
import org.apache.spark.sql.hive.test.TestHive._

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
class ParquetMetastoreSuite extends ParquetTest {
  override def beforeAll(): Unit = {
    super.beforeAll()

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
      create external table partitioned_parquet_with_complextypes
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
      location '${partitionedTableDirWithComplexTypes.getCanonicalPath}'
    """)
    
    sql(s"""
      create external table partitioned_parquet_with_key_and_complextypes
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
      location '${partitionedTableDirWithKeyAndComplexTypes.getCanonicalPath}'
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
      location '${new File(partitionedTableDir, "p=1").getCanonicalPath}'
    """)

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

    setConf("spark.sql.hive.convertMetastoreParquet", "true")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS partitioned_parquet")
    sql("DROP TABLE IF EXISTS partitioned_parquet_with_key")
    sql("DROP TABLE IF EXISTS partitioned_parquet_with_complextypes")
    sql("DROP TABLE IF EXISTS partitioned_parquet_with_key_and_complextypes")
    sql("DROP TABLE IF EXISTS normal_parquet")

    setConf("spark.sql.hive.convertMetastoreParquet", "false")
  }

  test("conversion is working") {
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.executedPlan.collect {
        case _: HiveTableScan => true
      }.isEmpty)
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.executedPlan.collect {
        case _: ParquetTableScan => true
      }.nonEmpty)
  }
}

/**
 * A suite of tests for the Parquet support through the data sources API.
 */
class ParquetSourceSuite extends ParquetTest {
  override def beforeAll(): Unit = {
    super.beforeAll()

    sql( s"""
      create temporary table partitioned_parquet
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDir.getCanonicalPath}'
      )
    """)

    sql( s"""
      create temporary table partitioned_parquet_with_key
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithKey.getCanonicalPath}'
      )
    """)

    sql( s"""
      create temporary table normal_parquet
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${new File(partitionedTableDir, "p=1").getCanonicalPath}'
      )
    """)

    sql( s"""
      CREATE TEMPORARY TABLE partitioned_parquet_with_key_and_complextypes
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithKeyAndComplexTypes.getCanonicalPath}'
      )
    """)

    sql( s"""
      CREATE TEMPORARY TABLE partitioned_parquet_with_complextypes
      USING org.apache.spark.sql.parquet
      OPTIONS (
        path '${partitionedTableDirWithComplexTypes.getCanonicalPath}'
      )
    """)
  }
}

/**
 * A collection of tests for parquet data with various forms of partitioning.
 */
abstract class ParquetTest extends QueryTest with BeforeAndAfterAll {
  var partitionedTableDir: File = null
  var normalTableDir: File = null
  var partitionedTableDirWithKey: File = null
  var partitionedTableDirWithKeyAndComplexTypes: File = null
  var partitionedTableDirWithComplexTypes: File = null

  override def beforeAll(): Unit = {
    partitionedTableDir = File.createTempFile("parquettests", "sparksql")
    partitionedTableDir.delete()
    partitionedTableDir.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDir, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetData(i, s"part-$p"))
        .saveAsParquetFile(partDir.getCanonicalPath)
    }

    normalTableDir = File.createTempFile("parquettests", "sparksql")
    normalTableDir.delete()
    normalTableDir.mkdir()

    sparkContext
      .makeRDD(1 to 10)
      .map(i => ParquetData(i, s"part-1"))
      .saveAsParquetFile(new File(normalTableDir, "normal").getCanonicalPath)

    partitionedTableDirWithKey = File.createTempFile("parquettests", "sparksql")
    partitionedTableDirWithKey.delete()
    partitionedTableDirWithKey.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKey, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetDataWithKey(p, i, s"part-$p"))
        .saveAsParquetFile(partDir.getCanonicalPath)
    }

    partitionedTableDirWithKeyAndComplexTypes = File.createTempFile("parquettests", "sparksql")
    partitionedTableDirWithKeyAndComplexTypes.delete()
    partitionedTableDirWithKeyAndComplexTypes.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKeyAndComplexTypes, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetDataWithKeyAndComplexTypes(
          p, i, s"part-$p", StructContainer(i, f"${i}_string"), 1 to i))
        .saveAsParquetFile(partDir.getCanonicalPath)
    }

    partitionedTableDirWithComplexTypes = File.createTempFile("parquettests", "sparksql")
    partitionedTableDirWithComplexTypes.delete()
    partitionedTableDirWithComplexTypes.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithComplexTypes, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetDataWithComplexTypes(
          i, s"part-$p", StructContainer(i, f"${i}_string"), 1 to i))
        .saveAsParquetFile(partDir.getCanonicalPath)
    }
  }

  override protected def afterAll(): Unit = {
    //delete temporary files
    partitionedTableDir.delete()
    normalTableDir.delete()
    partitionedTableDirWithKey.delete()
    partitionedTableDirWithKeyAndComplexTypes.delete()
    partitionedTableDirWithComplexTypes.delete()
  }

  Seq(
    "partitioned_parquet",
    "partitioned_parquet_with_key",
    "partitioned_parquet_with_complextypes",
    "partitioned_parquet_with_key_and_complextypes").foreach { table =>
    test(s"ordering of the partitioning columns $table") {
      checkAnswer(
        sql(s"SELECT p, stringField FROM $table WHERE p = 1"),
        Seq.fill(10)((1, "part-1"))
      )

      checkAnswer(
        sql(s"SELECT stringField, p FROM $table WHERE p = 1"),
        Seq.fill(10)(("part-1", 1))
      )
    }



    test(s"project the partitioning column $table") {
      checkAnswer(
        sql(s"SELECT p, count(*) FROM $table group by p"),
        (1, 10) ::
        (2, 10) ::
        (3, 10) ::
        (4, 10) ::
        (5, 10) ::
        (6, 10) ::
        (7, 10) ::
        (8, 10) ::
        (9, 10) ::
        (10, 10) :: Nil
      )
    }

    test(s"project partitioning and non-partitioning columns $table") {
      checkAnswer(
        sql(s"SELECT stringField, p, count(intField) FROM $table GROUP BY p, stringField"),
        ("part-1", 1, 10) ::
        ("part-2", 2, 10) ::
        ("part-3", 3, 10) ::
        ("part-4", 4, 10) ::
        ("part-5", 5, 10) ::
        ("part-6", 6, 10) ::
        ("part-7", 7, 10) ::
        ("part-8", 8, 10) ::
        ("part-9", 9, 10) ::
        ("part-10", 10, 10) :: Nil
      )
    }

    test(s"simple count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table"),
        100)
    }

    test(s"pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p = 1"),
        10)
    }

    test(s"non-existant partition $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p = 1000"),
        0)
    }

    test(s"multi-partition pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p IN (1,2,3)"),
        30)
    }

    test(s"non-partition predicates $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE intField IN (1,2,3)"),
        30)
    }

    test(s"sum $table") {
      checkAnswer(
        sql(s"SELECT SUM(intField) FROM $table WHERE intField IN (1,2,3) AND p = 1"),
        1 + 2 + 3)
    }

    test(s"hive udfs $table") {
      checkAnswer(
        sql(s"SELECT concat(stringField, stringField) FROM $table"),
        sql(s"SELECT stringField FROM $table").map {
          case Row(s: String) => Row(s + s)
        }.collect().toSeq)
    }
  }

  Seq(
    "partitioned_parquet_with_key_and_complextypes",
    "partitioned_parquet_with_complextypes").foreach { table =>
    test(s"SPARK-5775 read structure from $table") {
      checkAnswer(
        sql(s"""
          SELECT
            p,
            structField.intStructField,
            structField.stringStructField
          FROM $table
          WHERE p = 1"""),
        (1 to 10).map(i => Row(1, i, f"${i}_string")))
    }

    // Re-enable this after SPARK-5508 is fixed
    ignore(s"SPARK-5775 read array from $table") {
      checkAnswer(
        sql(s"SELECT arrayField, p FROM $table WHERE p = 1"),
        (1 to 10).map(i => Row(1 to i, 1)))
    }
  }

  test("non-part select(*)") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM normal_parquet"),
      10)
  }
}
