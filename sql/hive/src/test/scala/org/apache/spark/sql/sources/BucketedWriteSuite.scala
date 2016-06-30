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
import java.net.URI

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class BucketedWriteSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("bucketed by non-existing column") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "k").saveAsTable("tt"))
  }

  test("numBuckets not greater than 0 or less than 100000") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.bucketBy(0, "i").saveAsTable("tt"))
    intercept[IllegalArgumentException](df.write.bucketBy(100000, "i").saveAsTable("tt"))
  }

  test("specify sorting columns without bucketing columns") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.sortBy("j").saveAsTable("tt"))
  }

  test("sorting by non-orderable column") {
    val df = Seq("a" -> Map(1 -> 1), "b" -> Map(2 -> 2)).toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "i").sortBy("j").saveAsTable("tt"))
  }

  test("write bucketed data to unsupported data source") {
    val df = Seq(Tuple1("a"), Tuple1("b")).toDF("i")
    intercept[SparkException](df.write.bucketBy(3, "i").format("text").saveAsTable("tt"))
  }

  test("write bucketed data using save()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").parquet("/tmp/path")
    }
    assert(e.getMessage == "'save' does not support bucketing right now;")
  }

  test("write bucketed data using insertInto()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").insertInto("tt")
    }
    assert(e.getMessage == "'insertInto' does not support bucketing right now;")
  }

  private val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")

  def tableDir: File = {
    val identifier = spark.sessionState.sqlParser.parseTableIdentifier("bucketed_table")
    new File(URI.create(hiveContext.sessionState.catalog.hiveDefaultTableFilePath(identifier)))
  }

  /**
   * A helper method to check the bucket write functionality in low level, i.e. check the written
   * bucket files to see if the data are correct.  User should pass in a data dir that these bucket
   * files are written to, and the format of data(parquet, json, etc.), and the bucketing
   * information.
   */
  private def testBucketing(
      dataDir: File,
      source: String,
      numBuckets: Int,
      bucketCols: Seq[String],
      sortCols: Seq[String] = Nil): Unit = {
    val allBucketFiles = dataDir.listFiles().filterNot(f =>
      f.getName.startsWith(".") || f.getName.startsWith("_")
    )

    for (bucketFile <- allBucketFiles) {
      val bucketId = BucketingUtils.getBucketId(bucketFile.getName).getOrElse {
        fail(s"Unable to find the related bucket files.")
      }

      // Remove the duplicate columns in bucketCols and sortCols;
      // Otherwise, we got analysis errors due to duplicate names
      val selectedColumns = (bucketCols ++ sortCols).distinct
      // We may lose the type information after write(e.g. json format doesn't keep schema
      // information), here we get the types from the original dataframe.
      val types = df.select(selectedColumns.map(col): _*).schema.map(_.dataType)
      val columns = selectedColumns.zip(types).map {
        case (colName, dt) => col(colName).cast(dt)
      }

      // Read the bucket file into a dataframe, so that it's easier to test.
      val readBack = spark.read.format(source)
        .load(bucketFile.getAbsolutePath)
        .select(columns: _*)

      // If we specified sort columns while writing bucket table, make sure the data in this
      // bucket file is already sorted.
      if (sortCols.nonEmpty) {
        checkAnswer(readBack.sort(sortCols.map(col): _*), readBack.collect())
      }

      // Go through all rows in this bucket file, calculate bucket id according to bucket column
      // values, and make sure it equals to the expected bucket id that inferred from file name.
      val qe = readBack.select(bucketCols.map(col): _*).queryExecution
      val rows = qe.toRdd.map(_.copy()).collect()
      val getBucketId = UnsafeProjection.create(
        HashPartitioning(qe.analyzed.output, numBuckets).partitionIdExpression :: Nil,
        qe.analyzed.output)

      for (row <- rows) {
        val actualBucketId = getBucketId(row).getInt(0)
        assert(actualBucketId == bucketId)
      }
    }
  }

  test("write bucketed data") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .partitionBy("i")
          .bucketBy(8, "j", "k")
          .saveAsTable("bucketed_table")

        for (i <- 0 until 5) {
          testBucketing(new File(tableDir, s"i=$i"), source, 8, Seq("j", "k"))
        }
      }
    }
  }

  test("write bucketed data with sortBy") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .partitionBy("i")
          .bucketBy(8, "j")
          .sortBy("k")
          .saveAsTable("bucketed_table")

        for (i <- 0 until 5) {
          testBucketing(new File(tableDir, s"i=$i"), source, 8, Seq("j"), Seq("k"))
        }
      }
    }
  }

  test("write bucketed data with the overlapping bucketBy and partitionBy columns") {
    intercept[AnalysisException](df.write
      .partitionBy("i", "j")
      .bucketBy(8, "j", "k")
      .sortBy("k")
      .saveAsTable("bucketed_table"))
  }

  test("write bucketed data with the identical bucketBy and partitionBy columns") {
    intercept[AnalysisException](df.write
      .partitionBy("i")
      .bucketBy(8, "i")
      .saveAsTable("bucketed_table"))
  }

  test("write bucketed data without partitionBy") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .bucketBy(8, "i", "j")
          .saveAsTable("bucketed_table")

        testBucketing(tableDir, source, 8, Seq("i", "j"))
      }
    }
  }

  test("write bucketed data without partitionBy with sortBy") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .bucketBy(8, "i", "j")
          .sortBy("k")
          .saveAsTable("bucketed_table")

        testBucketing(tableDir, source, 8, Seq("i", "j"), Seq("k"))
      }
    }
  }

  test("write bucketed data with bucketing disabled") {
    // The configuration BUCKETING_ENABLED does not affect the writing path
    withSQLConf(SQLConf.BUCKETING_ENABLED.key -> "false") {
      for (source <- Seq("parquet", "json", "orc")) {
        withTable("bucketed_table") {
          df.write
            .format(source)
            .partitionBy("i")
            .bucketBy(8, "j", "k")
            .saveAsTable("bucketed_table")

          for (i <- 0 until 5) {
            testBucketing(new File(tableDir, s"i=$i"), source, 8, Seq("j", "k"))
          }
        }
      }
    }
  }
}
