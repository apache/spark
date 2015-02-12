
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

import org.apache.spark.sql.{SQLConf, QueryTest}
import org.apache.spark.sql.execution.PhysicalRDD
import org.apache.spark.sql.hive.execution.HiveTableScan
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._


// The data where the partitioning key exists only in the directory structure.
case class ParquetData(intField: Int, stringField: String)
// The data that also includes the partitioning key
case class ParquetDataWithKey(p: Int, intField: Int, stringField: String)


/**
 * A suite to test the automatic conversion of metastore tables with parquet data to use the
 * built in parquet support.
 */
class ParquetMetastoreSuite extends ParquetPartitioningTest {
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

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet_with_key ADD PARTITION (p=$p)")
    }

    setConf("spark.sql.hive.convertMetastoreParquet", "true")
  }

  override def afterAll(): Unit = {
    setConf("spark.sql.hive.convertMetastoreParquet", "false")
  }

  test(s"conversion is working") {
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.executedPlan.collect {
        case _: HiveTableScan => true
      }.isEmpty)
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.executedPlan.collect {
        case _: ParquetTableScan => true
        case _: PhysicalRDD => true
      }.nonEmpty)
  }
}

/**
 * A suite of tests for the Parquet support through the data sources API.
 */
class ParquetSourceSuite extends ParquetPartitioningTest {
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
  }
}

/**
 * A collection of tests for parquet data with various forms of partitioning.
 */
abstract class ParquetPartitioningTest extends QueryTest with BeforeAndAfterAll {
  var partitionedTableDir: File = null
  var normalTableDir: File = null
  var partitionedTableDirWithKey: File = null


  override def beforeAll(): Unit = {
    partitionedTableDir = File.createTempFile("parquettests", "sparksql")
    partitionedTableDir.delete()
    partitionedTableDir.mkdir()

    normalTableDir = File.createTempFile("parquettests", "sparksql")
    normalTableDir.delete()
    normalTableDir.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDir, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetData(i, s"part-$p"))
        .toDF()
        .saveAsParquetFile(partDir.getCanonicalPath)
    }

    sparkContext
      .makeRDD(1 to 10)
      .map(i => ParquetData(i, s"part-1"))
      .toDF()
      .saveAsParquetFile(new File(normalTableDir, "normal").getCanonicalPath)

    partitionedTableDirWithKey = File.createTempFile("parquettests", "sparksql")
    partitionedTableDirWithKey.delete()
    partitionedTableDirWithKey.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKey, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetDataWithKey(p, i, s"part-$p"))
        .toDF()
        .saveAsParquetFile(partDir.getCanonicalPath)
    }
  }

  def run(prefix: String): Unit = {
    Seq("partitioned_parquet", "partitioned_parquet_with_key").foreach { table =>
      test(s"$prefix: ordering of the partitioning columns $table") {
        checkAnswer(
          sql(s"SELECT p, stringField FROM $table WHERE p = 1"),
          Seq.fill(10)(Row(1, "part-1"))
        )

        checkAnswer(
          sql(s"SELECT stringField, p FROM $table WHERE p = 1"),
          Seq.fill(10)(Row("part-1", 1))
        )
      }

      test(s"$prefix: project the partitioning column $table") {
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

      test(s"$prefix: project partitioning and non-partitioning columns $table") {
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

      test(s"$prefix: simple count $table") {
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $table"),
          Row(100))
      }

      test(s"$prefix: pruned count $table") {
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $table WHERE p = 1"),
          Row(10))
      }

      test(s"$prefix: non-existent partition $table") {
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $table WHERE p = 1000"),
          Row(0))
      }

      test(s"$prefix: multi-partition pruned count $table") {
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $table WHERE p IN (1,2,3)"),
          Row(30))
      }

      test(s"$prefix: non-partition predicates $table") {
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $table WHERE intField IN (1,2,3)"),
          Row(30))
      }

      test(s"$prefix: sum $table") {
        checkAnswer(
          sql(s"SELECT SUM(intField) FROM $table WHERE intField IN (1,2,3) AND p = 1"),
          Row(1 + 2 + 3))
      }

      test(s"$prefix: hive udfs $table") {
        checkAnswer(
          sql(s"SELECT concat(stringField, stringField) FROM $table"),
          sql(s"SELECT stringField FROM $table").map {
            case Row(s: String) => Row(s + s)
          }.collect().toSeq)
      }
    }

    test(s"$prefix: $prefix: non-part select(*)") {
      checkAnswer(
        sql("SELECT COUNT(*) FROM normal_parquet"),
        Row(10))
    }
  }

  setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "false")
  run("Parquet data source enabled")

  setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "true")
  run("Parquet data source disabled")
}
