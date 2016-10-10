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

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.parquet.hadoop.ParquetOutputCommitter

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileScanRDD, HadoopFsRelation, LocalityTestFileSystem, LogicalRelation}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._


abstract class HadoopFsRelationTest extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import spark.implicits._

  val dataSourceName: String

  protected def supportsDataType(dataType: DataType): Boolean = true

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  lazy val testDF = (1 to 3).map(i => (i, s"val_$i")).toDF("a", "b")

  lazy val partitionedTestDF1 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 1, p2)).toDF("a", "b", "p1", "p2")

  lazy val partitionedTestDF2 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 2, p2)).toDF("a", "b", "p1", "p2")

  lazy val partitionedTestDF = partitionedTestDF1.union(partitionedTestDF2)

  def checkQueries(df: DataFrame): Unit = {
    // Selects everything
    checkAnswer(
      df,
      for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))

    // Simple filtering and partition pruning
    checkAnswer(
      df.filter('a > 1 && 'p1 === 2),
      for (i <- 2 to 3; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", 2, p2))

    // Simple projection and filtering
    checkAnswer(
      df.filter('a > 1).select('b, 'a + 1),
      for (i <- 2 to 3; _ <- 1 to 2; _ <- Seq("foo", "bar")) yield Row(s"val_$i", i + 1))

    // Simple projection and partition pruning
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar")) yield Row(s"val_$i", 1))

    // Project many copies of columns with different types (reproduction for SPARK-7858)
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'b, 'b, 'b, 'p1, 'p1, 'p1, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar"))
        yield Row(s"val_$i", s"val_$i", s"val_$i", s"val_$i", 1, 1, 1, 1))

    // Self-join
    df.createOrReplaceTempView("t")
    withTempView("t") {
      checkAnswer(
        sql(
          """SELECT l.a, r.b, l.p1, r.p2
            |FROM t l JOIN t r
            |ON l.a = r.a AND l.p1 = r.p1 AND l.p2 = r.p2
          """.stripMargin),
        for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))
    }
  }

  private val supportedDataTypes = Seq(
    StringType, BinaryType,
    NullType, BooleanType,
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
    DateType, TimestampType,
    ArrayType(IntegerType),
    MapType(StringType, LongType),
    new StructType()
      .add("f1", FloatType, nullable = true)
      .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true),
    new UDT.MyDenseVectorUDT()
  ).filter(supportsDataType)

  for (dataType <- supportedDataTypes) {
    for (parquetDictionaryEncodingEnabled <- Seq(true, false)) {
      test(s"test all data types - $dataType with parquet.enable.dictionary = " +
        s"$parquetDictionaryEncodingEnabled") {

        val extraOptions = Map[String, String](
          "parquet.enable.dictionary" -> parquetDictionaryEncodingEnabled.toString
        )

        withTempPath { file =>
          val path = file.getCanonicalPath

          val dataGenerator = RandomDataGenerator.forType(
            dataType = dataType,
            nullable = true,
            new Random(System.nanoTime())
          ).getOrElse {
            fail(s"Failed to create data generator for schema $dataType")
          }

          // Create a DF for the schema with random data. The index field is used to sort the
          // DataFrame.  This is a workaround for SPARK-10591.
          val schema = new StructType()
            .add("index", IntegerType, nullable = false)
            .add("col", dataType, nullable = true)
          val rdd =
            spark.sparkContext.parallelize((1 to 10).map(i => Row(i, dataGenerator())))
          val df = spark.createDataFrame(rdd, schema).orderBy("index").coalesce(1)

          df.write
            .mode("overwrite")
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .options(extraOptions)
            .save(path)

          val loadedDF = spark
            .read
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .schema(df.schema)
            .options(extraOptions)
            .load(path)
            .orderBy("index")

          checkAnswer(loadedDF, df)
        }
      }
    }
  }

  test("save()/load() - non-partitioned table - Overwrite") {
    withTempPath { file =>
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("path", file.getCanonicalPath)
          .option("dataSchema", dataSchema.json)
          .load(),
        testDF.collect())
    }
  }

  test("save()/load() - non-partitioned table - Append") {
    withTempPath { file =>
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)
      testDF.write.mode(SaveMode.Append).format(dataSourceName).save(file.getCanonicalPath)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath).orderBy("a"),
        testDF.union(testDF).orderBy("a").collect())
    }
  }

  test("save()/load() - non-partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[AnalysisException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists).save(file.getCanonicalPath)
      }
    }
  }

  test("save()/load() - non-partitioned table - Ignore") {
    withTempDir { file =>
      testDF.write.mode(SaveMode.Ignore).format(dataSourceName).save(file.getCanonicalPath)

      val path = new Path(file.getCanonicalPath)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      assert(fs.listStatus(path).isEmpty)
    }
  }

  test("save()/load() - partitioned table - simple queries") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.ErrorIfExists)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkQueries(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath))
    }
  }

  test("save()/load() - partitioned table - Overwrite") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.collect())
    }
  }

  test("save()/load() - partitioned table - Append") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.union(partitionedTestDF).collect())
    }
  }

  test("save()/load() - partitioned table - Append - new partition values") {
    withTempPath { file =>
      partitionedTestDF1.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      partitionedTestDF2.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.collect())
    }
  }

  test("save()/load() - partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[AnalysisException] {
        partitionedTestDF.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .partitionBy("p1", "p2")
          .save(file.getCanonicalPath)
      }
    }
  }

  test("save()/load() - partitioned table - Ignore") {
    withTempDir { file =>
      partitionedTestDF.write
        .format(dataSourceName).mode(SaveMode.Ignore).save(file.getCanonicalPath)

      val path = new Path(file.getCanonicalPath)
      val fs = path.getFileSystem(SparkHadoopUtil.get.conf)
      assert(fs.listStatus(path).isEmpty)
    }
  }

  test("saveAsTable()/load() - non-partitioned table - Overwrite") {
    testDF.write.format(dataSourceName).mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), testDF.collect())
    }
  }

  test("saveAsTable()/load() - non-partitioned table - Append") {
    testDF.write.format(dataSourceName).mode(SaveMode.Overwrite).saveAsTable("t")
    testDF.write.format(dataSourceName).mode(SaveMode.Append).saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), testDF.union(testDF).orderBy("a").collect())
    }
  }

  test("saveAsTable()/load() - non-partitioned table - ErrorIfExists") {
    withTable("t") {
      sql("CREATE TABLE t(i INT) USING parquet")
      intercept[AnalysisException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists).saveAsTable("t")
      }
    }
  }

  test("saveAsTable()/load() - non-partitioned table - Ignore") {
    withTable("t") {
      sql("CREATE TABLE t(i INT) USING parquet")
      testDF.write.format(dataSourceName).mode(SaveMode.Ignore).saveAsTable("t")
      assert(spark.table("t").collect().isEmpty)
    }
  }

  test("saveAsTable()/load() - partitioned table - simple queries") {
    partitionedTestDF.write.format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .saveAsTable("t")

    withTable("t") {
      checkQueries(spark.table("t"))
    }
  }

  test("saveAsTable()/load() - partitioned table - boolean type") {
    spark.range(2)
      .select('id, ('id % 2 === 0).as("b"))
      .write.partitionBy("b").saveAsTable("t")

    withTable("t") {
      checkAnswer(
        spark.table("t").sort('id),
        Row(0, true) :: Row(1, false) :: Nil
      )
    }
  }

  test("saveAsTable()/load() - partitioned table - Overwrite") {
    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), partitionedTestDF.collect())
    }
  }

  test("saveAsTable()/load() - partitioned table - Append") {
    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Append)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), partitionedTestDF.union(partitionedTestDF).collect())
    }
  }

  test("saveAsTable()/load() - partitioned table - Append - new partition values") {
    partitionedTestDF1.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF2.write
      .format(dataSourceName)
      .mode(SaveMode.Append)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), partitionedTestDF.collect())
    }
  }

  test("saveAsTable()/load() - partitioned table - Append - mismatched partition columns") {
    partitionedTestDF1.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    // Using only a subset of all partition columns
    intercept[Throwable] {
      partitionedTestDF2.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .option("dataSchema", dataSchema.json)
        .partitionBy("p1")
        .saveAsTable("t")
    }
  }

  test("saveAsTable()/load() - partitioned table - ErrorIfExists") {
    Seq.empty[(Int, String)].toDF().createOrReplaceTempView("t")

    withTempView("t") {
      intercept[AnalysisException] {
        partitionedTestDF.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .option("dataSchema", dataSchema.json)
          .partitionBy("p1", "p2")
          .saveAsTable("t")
      }
    }
  }

  test("saveAsTable()/load() - partitioned table - Ignore") {
    Seq.empty[(Int, String)].toDF().createOrReplaceTempView("t")

    withTempView("t") {
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Ignore)
        .option("dataSchema", dataSchema.json)
        .partitionBy("p1", "p2")
        .saveAsTable("t")

      assert(spark.table("t").collect().isEmpty)
    }
  }

  test("load() - with directory of unpartitioned data in nested subdirs") {
    withTempPath { dir =>
      val subdir = new File(dir, "subdir")

      val dataInDir = Seq(1, 2, 3).toDF("value")
      val dataInSubdir = Seq(4, 5, 6).toDF("value")

      /*

        Directory structure to be generated

        dir
          |
          |___ [ files of dataInDir ]
          |
          |___ subsubdir
                    |
                    |___ [ files of dataInSubdir ]
      */

      // Generated dataInSubdir, not data in dir
      dataInSubdir.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .save(subdir.getCanonicalPath)

      // Inferring schema should throw error as it should not find any file to infer
      val e = intercept[Exception] {
        spark.read.format(dataSourceName).load(dir.getCanonicalPath)
      }

      e match {
        case _: AnalysisException =>
          assert(e.getMessage.contains("infer"))

        case _: java.util.NoSuchElementException if e.getMessage.contains("dataSchema") =>
          // Ignore error, the source format requires schema to be provided by user
          // This is needed for SimpleTextHadoopFsRelationSuite as SimpleTextSource needs schema

        case _ =>
          fail("Unexpected error trying to infer schema from empty dir", e)
      }

      /** Test whether data is read with the given path matches the expected answer */
      def testWithPath(path: File, expectedAnswer: Seq[Row]): Unit = {
        val df = spark.read
          .format(dataSourceName)
          .schema(dataInDir.schema) // avoid schema inference for any format
          .load(path.getCanonicalPath)
        checkAnswer(df, expectedAnswer)
      }

      // Verify that reading by path 'dir/' gives empty results as there are no files in 'file'
      // and it should not pick up files in 'dir/subdir'
      require(subdir.exists)
      require(subdir.listFiles().exists(!_.isDirectory))
      testWithPath(dir, Seq.empty)

      // Verify that if there is data in dir, then reading by path 'dir/' reads only dataInDir
      dataInDir.write
        .format(dataSourceName)
        .mode(SaveMode.Append)   // append to prevent subdir from being deleted
        .save(dir.getCanonicalPath)
      require(dir.listFiles().exists(!_.isDirectory))
      require(subdir.exists())
      require(subdir.listFiles().exists(!_.isDirectory))
      testWithPath(dir, dataInDir.collect())
    }
  }

  test("Hadoop style globbing - unpartitioned data") {
    withTempPath { file =>

      val dir = file.getCanonicalPath
      val subdir = new File(dir, "subdir")
      val subsubdir = new File(subdir, "subsubdir")
      val anotherSubsubdir =
        new File(new File(dir, "another-subdir"), "another-subsubdir")

      val dataInSubdir = Seq(1, 2, 3).toDF("value")
      val dataInSubsubdir = Seq(4, 5, 6).toDF("value")
      val dataInAnotherSubsubdir = Seq(7, 8, 9).toDF("value")

      dataInSubdir.write
        .format (dataSourceName)
        .mode (SaveMode.Overwrite)
        .save (subdir.getCanonicalPath)

      dataInSubsubdir.write
        .format (dataSourceName)
        .mode (SaveMode.Overwrite)
        .save (subsubdir.getCanonicalPath)

      dataInAnotherSubsubdir.write
        .format (dataSourceName)
        .mode (SaveMode.Overwrite)
        .save (anotherSubsubdir.getCanonicalPath)

      require(subdir.exists)
      require(subdir.listFiles().exists(!_.isDirectory))
      require(subsubdir.exists)
      require(subsubdir.listFiles().exists(!_.isDirectory))
      require(anotherSubsubdir.exists)
      require(anotherSubsubdir.listFiles().exists(!_.isDirectory))

      /*
        Directory structure generated

        dir
          |
          |___ subdir
          |     |
          |     |___ [ files of dataInSubdir ]
          |     |
          |     |___ subsubdir
          |               |
          |               |___ [ files of dataInSubsubdir ]
          |
          |
          |___ anotherSubdir
                |
                |___ anotherSubsubdir
                          |
                          |___ [ files of dataInAnotherSubsubdir ]
       */

      val schema = dataInSubdir.schema

      /** Check whether data is read with the given path matches the expected answer */
      def check(path: String, expectedDf: DataFrame): Unit = {
        val df = spark.read
          .format(dataSourceName)
          .schema(schema) // avoid schema inference for any format, expected to be same format
          .load(path)
        checkAnswer(df, expectedDf)
      }

      check(s"$dir/*/", dataInSubdir)
      check(s"$dir/sub*/*", dataInSubdir.union(dataInSubsubdir))
      check(s"$dir/another*/*", dataInAnotherSubsubdir)
      check(s"$dir/*/another*", dataInAnotherSubsubdir)
      check(s"$dir/*/*", dataInSubdir.union(dataInSubsubdir).union(dataInAnotherSubsubdir))
    }
  }

  test("Hadoop style globbing - partitioned data with schema inference") {

    // Tests the following on partition data
    // - partitions are not discovered with globbing and without base path set.
    // - partitions are discovered with globbing and base path set, though more detailed
    //   tests for this is in ParquetPartitionDiscoverySuite

    withTempPath { path =>
      val dir = path.getCanonicalPath
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(dir)

      def check(
          path: String,
          expectedResult: Either[DataFrame, String],
          basePath: Option[String] = None
        ): Unit = {
        try {
          val reader = spark.read
          basePath.foreach(reader.option("basePath", _))
          val testDf = reader
            .format(dataSourceName)
            .load(path)
          assert(expectedResult.isLeft, s"Error was expected with $path but result found")
          checkAnswer(testDf, expectedResult.left.get)
        } catch {
          case e: java.util.NoSuchElementException if e.getMessage.contains("dataSchema") =>
            // Ignore error, the source format requires schema to be provided by user
            // This is needed for SimpleTextHadoopFsRelationSuite as SimpleTextSource needs schema

          case e: Throwable =>
            assert(expectedResult.isRight, s"Was not expecting error with $path: " + e)
            assert(
              e.getMessage.contains(expectedResult.right.get),
              s"Did not find expected error message wiht $path")
        }
      }

      object Error {
        def apply(msg: String): Either[DataFrame, String] = Right(msg)
      }

      object Result {
        def apply(df: DataFrame): Either[DataFrame, String] = Left(df)
      }

      // ---- Without base path set ----
      // Should find all the data with partitioning columns
      check(s"$dir", Result(partitionedTestDF))

      // Should fail as globbing finds dirs without files, only subdirs in them.
      check(s"$dir/*/", Error("please set \"basePath\""))
      check(s"$dir/p1=*/", Error("please set \"basePath\""))

      // Should not find partition columns as the globs resolve to p2 dirs
      // with files in them
      check(s"$dir/*/*", Result(partitionedTestDF.drop("p1", "p2")))
      check(s"$dir/p1=*/p2=foo", Result(partitionedTestDF.filter("p2 = 'foo'").drop("p1", "p2")))
      check(s"$dir/p1=1/p2=???", Result(partitionedTestDF.filter("p1 = 1").drop("p1", "p2")))

      // Should find all data without the partitioning columns as the globs resolve to the files
      check(s"$dir/*/*/*", Result(partitionedTestDF.drop("p1", "p2")))

      // ---- With base path set ----
      val resultDf = partitionedTestDF.select("a", "b", "p1", "p2")
      check(path = s"$dir/*", Result(resultDf), basePath = Some(dir))
      check(path = s"$dir/*/*", Result(resultDf), basePath = Some(dir))
      check(path = s"$dir/*/*/*", Result(resultDf), basePath = Some(dir))
    }
  }

  test("SPARK-9735 Partition column type casting") {
    withTempPath { file =>
      val df = (for {
        i <- 1 to 3
        p2 <- Seq("foo", "bar")
      } yield (i, s"val_$i", 1.0d, p2, 123, 123.123f)).toDF("a", "b", "p1", "p2", "p3", "f")

      val input = df.select(
        'a,
        'b,
        'p1.cast(StringType).as('ps1),
        'p2,
        'p3.cast(FloatType).as('pf1),
        'f)

      withTempView("t") {
        input
          .write
          .format(dataSourceName)
          .mode(SaveMode.Overwrite)
          .partitionBy("ps1", "p2", "pf1", "f")
          .saveAsTable("t")

        input
          .write
          .format(dataSourceName)
          .mode(SaveMode.Append)
          .partitionBy("ps1", "p2", "pf1", "f")
          .saveAsTable("t")

        val realData = input.collect()

        checkAnswer(spark.table("t"), realData ++ realData)
      }
    }
  }

  test("SPARK-7616: adjust column name order accordingly when saving partitioned table") {
    val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")

    df.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .partitionBy("c", "a")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t").select('b, 'c, 'a), df.select('b, 'c, 'a).collect())
    }
  }

  // NOTE: This test suite is not super deterministic.  On nodes with only relatively few cores
  // (4 or even 1), it's hard to reproduce the data loss issue.  But on nodes with for example 8 or
  // more cores, the issue can be reproduced steadily.  Fortunately our Jenkins builder meets this
  // requirement.  We probably want to move this test case to spark-integration-tests or spark-perf
  // later.
  test("SPARK-8406: Avoids name collision while writing files") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark
        .range(10000)
        .repartition(250)
        .write
        .mode(SaveMode.Overwrite)
        .format(dataSourceName)
        .save(path)

      assertResult(10000) {
        spark
          .read
          .format(dataSourceName)
          .option("dataSchema", StructType(StructField("id", LongType) :: Nil).json)
          .load(path)
          .count()
      }
    }
  }

  test("SPARK-8578 specified custom output committer will not be used to append data") {
    val extraOptions = Map[String, String](
      SQLConf.OUTPUT_COMMITTER_CLASS.key -> classOf[AlwaysFailOutputCommitter].getName,
      // Since Parquet has its own output committer setting, also set it
      // to AlwaysFailParquetOutputCommitter at here.
      "spark.sql.parquet.output.committer.class" ->
        classOf[AlwaysFailParquetOutputCommitter].getName
    )

    val df = spark.range(1, 10).toDF("i")
    withTempPath { dir =>
      df.write.mode("append").format(dataSourceName).save(dir.getCanonicalPath)
      // Because there data already exists,
      // this append should succeed because we will use the output committer associated
      // with file format and AlwaysFailOutputCommitter will not be used.
      df.write.mode("append").format(dataSourceName).save(dir.getCanonicalPath)
      checkAnswer(
        spark.read
          .format(dataSourceName)
          .option("dataSchema", df.schema.json)
          .options(extraOptions)
          .load(dir.getCanonicalPath),
        df.union(df))

      // This will fail because AlwaysFailOutputCommitter is used when we do append.
      intercept[Exception] {
        df.write.mode("overwrite")
          .options(extraOptions).format(dataSourceName).save(dir.getCanonicalPath)
      }
    }
    withTempPath { dir =>
      // Because there is no existing data,
      // this append will fail because AlwaysFailOutputCommitter is used when we do append
      // and there is no existing data.
      intercept[Exception] {
        df.write.mode("append")
          .options(extraOptions)
          .format(dataSourceName)
          .save(dir.getCanonicalPath)
      }
    }
  }

  test("SPARK-8887: Explicitly define which data types can be used as dynamic partition columns") {
    val df = Seq(
      (1, "v1", Array(1, 2, 3), Map("k1" -> "v1"), Tuple2(1, "4")),
      (2, "v2", Array(4, 5, 6), Map("k2" -> "v2"), Tuple2(2, "5")),
      (3, "v3", Array(7, 8, 9), Map("k3" -> "v3"), Tuple2(3, "6"))).toDF("a", "b", "c", "d", "e")
    withTempDir { file =>
      intercept[AnalysisException] {
        df.write.format(dataSourceName).partitionBy("c", "d", "e").save(file.getCanonicalPath)
      }
    }
    intercept[AnalysisException] {
      df.write.format(dataSourceName).partitionBy("c", "d", "e").saveAsTable("t")
    }
  }

  test("Locality support for FileScanRDD") {
    val options = Map[String, String](
      "fs.file.impl" -> classOf[LocalityTestFileSystem].getName,
      "fs.file.impl.disable.cache" -> "true"
    )
    withTempPath { dir =>
      val path = "file://" + dir.getCanonicalPath
      val df1 = spark.range(4)
      df1.coalesce(1).write.mode("overwrite").options(options).format(dataSourceName).save(path)
      df1.coalesce(1).write.mode("append").options(options).format(dataSourceName).save(path)

      def checkLocality(): Unit = {
        val df2 = spark.read
          .format(dataSourceName)
          .option("dataSchema", df1.schema.json)
          .options(options)
          .load(path)

        val Some(fileScanRDD) = df2.queryExecution.executedPlan.collectFirst {
          case scan: DataSourceScanExec if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
            scan.inputRDDs().head.asInstanceOf[FileScanRDD]
        }

        val partitions = fileScanRDD.partitions
        val preferredLocations = partitions.flatMap(fileScanRDD.preferredLocations)

        assert(preferredLocations.distinct.length == 2)
      }

      checkLocality()

      withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "0") {
        checkLocality()
      }
    }
  }
}

// This class is used to test SPARK-8578. We should not use any custom output committer when
// we actually append data to an existing dir.
class AlwaysFailOutputCommitter(
    outputPath: Path,
    context: TaskAttemptContext)
  extends FileOutputCommitter(outputPath, context) {

  override def commitJob(context: JobContext): Unit = {
    sys.error("Intentional job commitment failure for testing purpose.")
  }
}

// This class is used to test SPARK-8578. We should not use any custom output committer when
// we actually append data to an existing dir.
class AlwaysFailParquetOutputCommitter(
    outputPath: Path,
    context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {

  override def commitJob(context: JobContext): Unit = {
    sys.error("Intentional job commitment failure for testing purpose.")
  }
}
