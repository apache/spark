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
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
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
        (1 to 10).map(i => Row((1 to i).toArray, 1)))
    }
  }

  test("non-part select(*)") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM normal_parquet"),
      Row(10))
  }
}
