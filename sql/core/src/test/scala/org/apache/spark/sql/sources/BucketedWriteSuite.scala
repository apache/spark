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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class BucketedWriteWithoutHiveSupportSuite extends BucketedWriteSuite with SharedSQLContext {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    assert(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
  }

  override protected def fileFormatsToTest: Seq[String] = Seq("parquet", "json")
}

abstract class BucketedWriteSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected def fileFormatsToTest: Seq[String]

  test("bucketed by non-existing column") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "k").saveAsTable("tt"))
  }

  test("numBuckets be greater than 0 but less/eq than default bucketing.maxBuckets (100000)") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    Seq(-1, 0, 100001).foreach(numBuckets => {
      val e = intercept[AnalysisException](df.write.bucketBy(numBuckets, "i").saveAsTable("tt"))
      assert(
        e.getMessage.contains("Number of buckets should be greater than 0 but less than"))
    })
  }

  test("numBuckets be greater than 0 but less/eq than overridden bucketing.maxBuckets (200000)") {
    val maxNrBuckets: Int = 200000
    val catalog = spark.sessionState.catalog

    withSQLConf("spark.sql.sources.bucketing.maxBuckets" -> maxNrBuckets.toString) {
      // within the new limit
      Seq(100001, maxNrBuckets).foreach(numBuckets => {
        withTable("t") {
          df.write.bucketBy(numBuckets, "i").saveAsTable("t")
          val table = catalog.getTableMetadata(TableIdentifier("t"))
          assert(table.bucketSpec == Option(BucketSpec(numBuckets, Seq("i"), Seq())))
        }
      })

      // over the new limit
      withTable("t") {
        val e = intercept[AnalysisException](
          df.write.bucketBy(maxNrBuckets + 1, "i").saveAsTable("t"))
        assert(
          e.getMessage.contains("Number of buckets should be greater than 0 but less than"))
      }
    }
  }

  test("specify sorting columns without bucketing columns") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    val e = intercept[AnalysisException] {
      df.write.sortBy("j").saveAsTable("tt")
    }
    assert(e.getMessage == "sortBy must be used together with bucketBy;")
  }

  test("sorting by non-orderable column") {
    val df = Seq("a" -> Map(1 -> 1), "b" -> Map(2 -> 2)).toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "i").sortBy("j").saveAsTable("tt"))
  }

  test("write bucketed data using save()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").parquet("/tmp/path")
    }
    assert(e.getMessage == "'save' does not support bucketBy right now;")
  }

  test("write bucketed and sorted data using save()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").sortBy("i").parquet("/tmp/path")
    }
    assert(e.getMessage == "'save' does not support bucketBy and sortBy right now;")
  }

  test("write bucketed data using insertInto()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").insertInto("tt")
    }
    assert(e.getMessage == "'insertInto' does not support bucketBy right now;")
  }

  test("write bucketed and sorted data using insertInto()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").sortBy("i").insertInto("tt")
    }
    assert(e.getMessage == "'insertInto' does not support bucketBy and sortBy right now;")
  }

  private lazy val df = {
    (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
  }

  def tableDir: File = {
    val identifier = spark.sessionState.sqlParser.parseTableIdentifier("bucketed_table")
    new File(spark.sessionState.catalog.defaultTablePath(identifier))
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
    for (source <- fileFormatsToTest) {
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
    for (source <- fileFormatsToTest) {
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

  test("write bucketed data with the overlapping bucketBy/sortBy and partitionBy columns") {
    val e1 = intercept[AnalysisException](df.write
      .partitionBy("i", "j")
      .bucketBy(8, "j", "k")
      .sortBy("k")
      .saveAsTable("bucketed_table"))
    assert(e1.message.contains("bucketing column 'j' should not be part of partition columns"))

    val e2 = intercept[AnalysisException](df.write
      .partitionBy("i", "j")
      .bucketBy(8, "k")
      .sortBy("i")
      .saveAsTable("bucketed_table"))
    assert(e2.message.contains("bucket sorting column 'i' should not be part of partition columns"))
  }

  test("write bucketed data without partitionBy") {
    for (source <- fileFormatsToTest) {
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
    for (source <- fileFormatsToTest) {
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
      for (source <- fileFormatsToTest) {
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
