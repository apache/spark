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

import java.io.File

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class PartitionedWriteSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("write many partitions") {
    val path = Utils.createTempDir()
    path.delete()

    val df = spark.range(100).select($"id", lit(1).as("data"))
    df.write.partitionBy("id").save(path.getCanonicalPath)

    checkAnswer(
      spark.read.load(path.getCanonicalPath),
      (0 to 99).map(Row(1, _)).toSeq)

    Utils.deleteRecursively(path)
  }

  test("write many partitions with repeats") {
    val path = Utils.createTempDir()
    path.delete()

    val base = spark.range(100)
    val df = base.union(base).select($"id", lit(1).as("data"))
    df.write.partitionBy("id").save(path.getCanonicalPath)

    checkAnswer(
      spark.read.load(path.getCanonicalPath),
      (0 to 99).map(Row(1, _)).toSeq ++ (0 to 99).map(Row(1, _)).toSeq)

    Utils.deleteRecursively(path)
  }

  test("partitioned columns should appear at the end of schema") {
    withTempPath { f =>
      val path = f.getAbsolutePath
      Seq(1 -> "a").toDF("i", "j").write.partitionBy("i").parquet(path)
      assert(spark.read.parquet(path).schema.map(_.name) == Seq("j", "i"))
    }
  }

  test("maxRecordsPerFile setting in non-partitioned write path") {
    withTempDir { f =>
      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", 1).mode("overwrite").parquet(f.getAbsolutePath)
      assert(recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 4)

      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", 2).mode("overwrite").parquet(f.getAbsolutePath)
      assert(recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 2)

      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", -1).mode("overwrite").parquet(f.getAbsolutePath)
      assert(recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 1)
    }
  }

  test("maxRecordsPerFile setting in dynamic partition writes") {
    withTempDir { f =>
      spark.range(start = 0, end = 4, step = 1, numPartitions = 1).selectExpr("id", "id id1")
        .write
        .partitionBy("id")
        .option("maxRecordsPerFile", 1)
        .mode("overwrite")
        .parquet(f.getAbsolutePath)
      assert(recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 4)
    }
  }

  /** Lists files recursively. */
  private def recursiveList(f: File): Array[File] = {
    require(f.isDirectory)
    val current = f.listFiles
    current ++ current.filter(_.isDirectory).flatMap(recursiveList)
  }
}
