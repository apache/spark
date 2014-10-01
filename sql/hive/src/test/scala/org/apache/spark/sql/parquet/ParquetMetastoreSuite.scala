
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

case class ParquetData(intField: Int, stringField: String)

/**
 * Tests for our SerDe -> Native parquet scan conversion.
 */
class ParquetMetastoreSuite extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    val partitionedTableDir = File.createTempFile("parquettests", "sparksql")
    partitionedTableDir.delete()
    partitionedTableDir.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDir, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => ParquetData(i, s"part-$p"))
        .saveAsParquetFile(partDir.getCanonicalPath)
    }

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

    setConf("spark.sql.hive.convertMetastoreParquet", "true")
  }

  override def afterAll(): Unit = {
    setConf("spark.sql.hive.convertMetastoreParquet", "false")
  }

  test("project the partitioning column") {
    checkAnswer(
      sql("SELECT p, count(*) FROM partitioned_parquet group by p"),
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

  test("project partitioning and non-partitioning columns") {
    checkAnswer(
      sql("SELECT stringField, p, count(intField) " +
        "FROM partitioned_parquet GROUP BY p, stringField"),
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

  test("simple count") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM partitioned_parquet"),
      100)
  }

  test("pruned count") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM partitioned_parquet WHERE p = 1"),
      10)
  }

  test("multi-partition pruned count") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM partitioned_parquet WHERE p IN (1,2,3)"),
      30)
  }

  test("non-partition predicates") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM partitioned_parquet WHERE intField IN (1,2,3)"),
      30)
  }

  test("sum") {
    checkAnswer(
      sql("SELECT SUM(intField) FROM partitioned_parquet WHERE intField IN (1,2,3) AND p = 1"),
      1 + 2 + 3)
  }

  test("hive udfs") {
    checkAnswer(
      sql("SELECT concat(stringField, stringField) FROM partitioned_parquet"),
      sql("SELECT stringField FROM partitioned_parquet").map {
        case Row(s: String) => Row(s + s)
      }.collect().toSeq)
  }

  test("non-part select(*)") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM normal_parquet"),
      10)
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
