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

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.{AnalysisException, QueryTest}

class BucketedWriteSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("bucketed by non-existing column") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "k").saveAsTable("tt"))
  }

  test("numBuckets not greater than 0") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.bucketBy(0, "i").saveAsTable("tt"))
  }

  test("specify sorting columns without bucketing columns") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.sortBy("j").saveAsTable("tt"))
  }

  test("sorting by non-orderable column") {
    val df = Seq("a" -> Map(1 -> 1), "b" -> Map(2 -> 2)).toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "i").sortBy("j").saveAsTable("tt"))
  }

  test("write bucketed data to non-hive-table or existing hive table") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").parquet("/tmp/path"))
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").json("/tmp/path"))
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").insertInto("tt"))
  }

  private val parquetFileName = """.*-(\d+)\..*\.parquet""".r
  private def getBucketId(fileName: String): Int = {
    fileName match {
      case parquetFileName(bucketId) => bucketId.toInt
    }
  }

  test("write bucketed data") {
    val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("bucketedTable") {
      df.write
        .format("parquet")
        .partitionBy("i")
        .bucketBy(8, "j", "k")
        .saveAsTable("bucketedTable")

      val tableDir = new File(hiveContext.warehousePath, "bucketedTable")
      logWarning(tableDir.listFiles().map(_.getAbsolutePath).mkString("\n"))
      for (i <- 0 until 5) {
        val allBucketFiles = new File(tableDir, s"i=$i").listFiles().filter(!_.isHidden)
        val groupedBucketFiles = allBucketFiles.groupBy(f => getBucketId(f.getName))
        assert(groupedBucketFiles.size <= 8)

        for ((bucketId, bucketFiles) <- groupedBucketFiles) {
          for (bucketFile <- bucketFiles) {
            withSQLConf("spark.sql.parquet.enableUnsafeRowRecordReader" -> "false") {
              val df = sqlContext.read.parquet(bucketFile.getAbsolutePath).select("j", "k")
              val rows = df.queryExecution.toRdd.map(_.copy()).collect()

              for (row <- rows) {
                assert(row.isInstanceOf[UnsafeRow])
                val actualBucketId = (row.hashCode() % 8 + 8) % 8
                assert(actualBucketId == bucketId)
              }
            }
          }
        }
      }
    }
  }

  test("write bucketed data with sortBy") {
    val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("bucketedTable") {
      df.write
        .format("parquet")
        .partitionBy("i")
        .bucketBy(8, "j")
        .sortBy("k")
        .saveAsTable("bucketedTable")

      val tableDir = new File(hiveContext.warehousePath, "bucketedTable")
      for (i <- 0 until 5) {
        val allBucketFiles = new File(tableDir, s"i=$i").listFiles()
          .filter(_.getName.startsWith("part"))
        val groupedBucketFiles = allBucketFiles.groupBy(f => getBucketId(f.getName))
        assert(groupedBucketFiles.size <= 8)

        for ((bucketId, bucketFiles) <- groupedBucketFiles) {
          for (bucketFile <- bucketFiles) {
            withSQLConf("spark.sql.parquet.enableUnsafeRowRecordReader" -> "false") {
              val df = sqlContext.read.parquet(bucketFile.getAbsolutePath).select("j", "k")
              checkAnswer(df.sort("k"), df.collect())
              val rows = df.select("j").queryExecution.toRdd.map(_.copy()).collect()

              for (row <- rows) {
                assert(row.isInstanceOf[UnsafeRow])
                val actualBucketId = (row.hashCode() % 8 + 8) % 8
                assert(actualBucketId == bucketId)
              }
            }
          }
        }
      }
    }
  }

  test("write bucketed data without partitionBy") {
    val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("bucketedTable") {
      df.write
        .format("parquet")
        .bucketBy(8, "i", "j")
        .saveAsTable("bucketedTable")

      val tableDir = new File(hiveContext.warehousePath, "bucketedTable")
      val allBucketFiles = tableDir.listFiles().filter(_.getName.startsWith("part"))
      val groupedBucketFiles = allBucketFiles.groupBy(f => getBucketId(f.getName))
      assert(groupedBucketFiles.size <= 8)

      for ((bucketId, bucketFiles) <- groupedBucketFiles) {
        for (bucketFile <- bucketFiles) {
          withSQLConf("spark.sql.parquet.enableUnsafeRowRecordReader" -> "false") {
            val df = sqlContext.read.parquet(bucketFile.getAbsolutePath).select("i", "j")
            val rows = df.queryExecution.toRdd.map(_.copy()).collect()

            for (row <- rows) {
              assert(row.isInstanceOf[UnsafeRow])
              val actualBucketId = (row.hashCode() % 8 + 8) % 8
              assert(actualBucketId == bucketId)
            }
          }
        }
      }
    }
  }

  test("write bucketed data without partitionBy with sortBy") {
    val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("bucketedTable") {
      df.write
        .format("parquet")
        .bucketBy(8, "i", "j")
        .sortBy("k")
        .saveAsTable("bucketedTable")

      val tableDir = new File(hiveContext.warehousePath, "bucketedTable")
      val allBucketFiles = tableDir.listFiles().filter(_.getName.startsWith("part"))
      val groupedBucketFiles = allBucketFiles.groupBy(f => getBucketId(f.getName))
      assert(groupedBucketFiles.size <= 8)

      for ((bucketId, bucketFiles) <- groupedBucketFiles) {
        for (bucketFile <- bucketFiles) {
          withSQLConf("spark.sql.parquet.enableUnsafeRowRecordReader" -> "false") {
            val df = sqlContext.read.parquet(bucketFile.getAbsolutePath)
            checkAnswer(df.sort("k"), df.collect())
            val rows = df.select("i", "j").queryExecution.toRdd.map(_.copy()).collect()

            for (row <- rows) {
              assert(row.isInstanceOf[UnsafeRow])
              val actualBucketId = (row.hashCode() % 8 + 8) % 8
              assert(actualBucketId == bucketId)
            }
          }
        }
      }
    }
  }
}
