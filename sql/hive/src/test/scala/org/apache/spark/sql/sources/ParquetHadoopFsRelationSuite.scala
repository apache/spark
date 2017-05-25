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

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


class ParquetHadoopFsRelationSuite extends HadoopFsRelationTest {
  import testImplicits._

  override val dataSourceName: String = "parquet"

  // Parquet does not play well with NullType.
  override protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: NullType => false
    case _: CalendarIntervalType => false
    case _ => true
  }

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    withTempDir { file =>
      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(
          CatalogUtils.URIToString(makeQualifiedPath(file.getCanonicalPath)), s"p1=$p1/p2=$p2")
        sparkContext
          .parallelize(for (i <- 1 to 3) yield (i, s"val_$i", p1))
          .toDF("a", "b", "p1")
          .write.parquet(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchemaWithPartition.json)
          .load(file.getCanonicalPath))
    }
  }

  test("SPARK-7868: _temporary directories should be ignored") {
    withTempPath { dir =>
      val df = Seq("a", "b", "c").zipWithIndex.toDF()

      df.write
        .format("parquet")
        .save(dir.getCanonicalPath)

      df.write
        .format("parquet")
        .save(s"${dir.getCanonicalPath}/_temporary")

      checkAnswer(spark.read.format("parquet").load(dir.getCanonicalPath), df.collect())
    }
  }

  test("SPARK-8014: Avoid scanning output directory when SaveMode isn't SaveMode.Append") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val df = Seq(1 -> "a").toDF()

      // Creates an arbitrary file.  If this directory gets scanned, ParquetRelation2 will throw
      // since it's not a valid Parquet file.
      val emptyFile = new File(path, "empty")
      Files.createParentDirs(emptyFile)
      Files.touch(emptyFile)

      // This shouldn't throw anything.
      df.write.format("parquet").mode(SaveMode.Ignore).save(path)

      // This should only complain that the destination directory already exists, rather than file
      // "empty" is not a Parquet file.
      assert {
        intercept[AnalysisException] {
          df.write.format("parquet").mode(SaveMode.ErrorIfExists).save(path)
        }.getMessage.contains("already exists")
      }

      // This shouldn't throw anything.
      df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
      checkAnswer(spark.read.format("parquet").load(path), df)
    }
  }

  test("SPARK-8079: Avoid NPE thrown from BaseWriterContainer.abortJob") {
    withTempPath { dir =>
      intercept[AnalysisException] {
        // Parquet doesn't allow field names with spaces.  Here we are intentionally making an
        // exception thrown from the `ParquetRelation2.prepareForWriteJob()` method to trigger
        // the bug.  Please refer to spark-8079 for more details.
        spark.range(1, 10)
          .withColumnRenamed("id", "a b")
          .write
          .format("parquet")
          .save(dir.getCanonicalPath)
      }
    }
  }

  test("SPARK-8604: Parquet data source should write summary file while doing appending") {
    withSQLConf(
        ParquetOutputFormat.ENABLE_JOB_SUMMARY -> "true",
        SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
          classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName) {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = spark.range(0, 5).toDF()
        df.write.mode(SaveMode.Overwrite).parquet(path)

        val summaryPath = new Path(path, "_metadata")
        val commonSummaryPath = new Path(path, "_common_metadata")

        val fs = summaryPath.getFileSystem(spark.sessionState.newHadoopConf())
        fs.delete(summaryPath, true)
        fs.delete(commonSummaryPath, true)

        df.write.mode(SaveMode.Append).parquet(path)
        checkAnswer(spark.read.parquet(path), df.union(df))

        assert(fs.exists(summaryPath))
        assert(fs.exists(commonSummaryPath))
      }
    }
  }

  test("SPARK-10334 Projections and filters should be kept in physical plan") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      spark.range(2).select('id as 'a, 'id as 'b).write.partitionBy("b").parquet(path)
      val df = spark.read.parquet(path).filter('a === 0).select('b)
      val physicalPlan = df.queryExecution.sparkPlan

      assert(physicalPlan.collect { case p: execution.ProjectExec => p }.length === 1)
      assert(physicalPlan.collect { case p: execution.FilterExec => p }.length === 1)
    }
  }

  test("SPARK-11500: Not deterministic order of columns when using merging schemas.") {
    import testImplicits._
    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true") {
      withTempPath { dir =>
        val pathOne = s"${dir.getCanonicalPath}/part=1"
        Seq(1, 1).zipWithIndex.toDF("a", "b").write.parquet(pathOne)
        val pathTwo = s"${dir.getCanonicalPath}/part=2"
        Seq(1, 1).zipWithIndex.toDF("c", "b").write.parquet(pathTwo)
        val pathThree = s"${dir.getCanonicalPath}/part=3"
        Seq(1, 1).zipWithIndex.toDF("d", "b").write.parquet(pathThree)

        // The schema consists of the leading columns of the first part-file
        // in the lexicographic order.
        assert(spark.read.parquet(dir.getCanonicalPath).schema.map(_.name)
          === Seq("a", "b", "c", "d", "part"))
      }
    }
  }

  test(s"SPARK-13537: Fix readBytes in VectorizedPlainValuesReader") {
    withTempPath { file =>
      val path = file.getCanonicalPath

      val schema = new StructType()
        .add("index", IntegerType, nullable = false)
        .add("col", ByteType, nullable = true)

      val data = Seq(Row(1, -33.toByte), Row(2, 0.toByte), Row(3, -55.toByte), Row(4, 56.toByte),
        Row(5, 127.toByte), Row(6, -44.toByte), Row(7, 23.toByte), Row(8, -95.toByte),
        Row(9, 127.toByte), Row(10, 13.toByte))

      val rdd = spark.sparkContext.parallelize(data)
      val df = spark.createDataFrame(rdd, schema).orderBy("index").coalesce(1)

      df.write
        .mode("overwrite")
        .format(dataSourceName)
        .option("dataSchema", df.schema.json)
        .save(path)

      val loadedDF = spark
        .read
        .format(dataSourceName)
        .option("dataSchema", df.schema.json)
        .schema(df.schema)
        .load(path)
        .orderBy("index")

      checkAnswer(loadedDF, df)
    }
  }

  test("SPARK-13543: Support for specifying compression codec for Parquet via option()") {
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "UNCOMPRESSED") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/table1"
        val df = (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b")
        df.write
          .option("compression", "GzIP")
          .parquet(path)

        val compressedFiles = new File(path).listFiles()
        assert(compressedFiles.exists(_.getName.endsWith(".gz.parquet")))

        val copyDf = spark
          .read
          .parquet(path)
        checkAnswer(df, copyDf)
      }
    }
  }
}
