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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

abstract class HadoopFsRelationTest extends QueryTest with SQLTestUtils {
  override val sqlContext: SQLContext = TestHive

  import sqlContext._
  import sqlContext.implicits._

  val dataSourceName = classOf[SimpleTextSource].getCanonicalName

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  val testDF = (1 to 3).map(i => (i, s"val_$i")).toDF("a", "b")

  val partitionedTestDF1 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 1, p2)).toDF("a", "b", "p1", "p2")

  val partitionedTestDF2 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 2, p2)).toDF("a", "b", "p1", "p2")

  val partitionedTestDF = partitionedTestDF1.unionAll(partitionedTestDF2)

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
    df.registerTempTable("t")
    withTempTable("t") {
      checkAnswer(
        sql(
          """SELECT l.a, r.b, l.p1, r.p2
            |FROM t l JOIN t r
            |ON l.a = r.a AND l.p1 = r.p1 AND l.p2 = r.p2
          """.stripMargin),
        for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))
    }
  }

  test("save()/load() - non-partitioned table - Overwrite") {
    withTempPath { file =>
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)

      checkAnswer(
        read.format(dataSourceName)
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
        read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath).orderBy("a"),
        testDF.unionAll(testDF).orderBy("a").collect())
    }
  }

  test("save()/load() - non-partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[RuntimeException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists).save(file.getCanonicalPath)
      }
    }
  }

  test("save()/load() - non-partitioned table - Ignore") {
    withTempDir { file =>
      testDF.write.mode(SaveMode.Ignore).format(dataSourceName).save(file.getCanonicalPath)

      val path = new Path(file.getCanonicalPath)
      val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
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
        read.format(dataSourceName)
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
        read.format(dataSourceName)
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
        read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.unionAll(partitionedTestDF).collect())
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
        read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.collect())
    }
  }

  test("save()/load() - partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[RuntimeException] {
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
      checkAnswer(table("t"), testDF.collect())
    }
  }

  test("saveAsTable()/load() - non-partitioned table - Append") {
    testDF.write.format(dataSourceName).mode(SaveMode.Overwrite).saveAsTable("t")
    testDF.write.format(dataSourceName).mode(SaveMode.Append).saveAsTable("t")

    withTable("t") {
      checkAnswer(table("t"), testDF.unionAll(testDF).orderBy("a").collect())
    }
  }

  test("saveAsTable()/load() - non-partitioned table - ErrorIfExists") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      intercept[AnalysisException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists).saveAsTable("t")
      }
    }
  }

  test("saveAsTable()/load() - non-partitioned table - Ignore") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      testDF.write.format(dataSourceName).mode(SaveMode.Ignore).saveAsTable("t")
      assert(table("t").collect().isEmpty)
    }
  }

  test("saveAsTable()/load() - partitioned table - simple queries") {
    partitionedTestDF.write.format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .saveAsTable("t")

    withTable("t") {
      checkQueries(table("t"))
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
      checkAnswer(table("t"), partitionedTestDF.collect())
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
      checkAnswer(table("t"), partitionedTestDF.unionAll(partitionedTestDF).collect())
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
      checkAnswer(table("t"), partitionedTestDF.collect())
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
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
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
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Ignore)
        .option("dataSchema", dataSchema.json)
        .partitionBy("p1", "p2")
        .saveAsTable("t")

      assert(table("t").collect().isEmpty)
    }
  }

  test("Hadoop style globbing") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      val df = read
        .format(dataSourceName)
        .option("dataSchema", dataSchema.json)
        .load(s"${file.getCanonicalPath}/p1=*/p2=???")

      val expectedPaths = Set(
        s"${file.getCanonicalFile}/p1=1/p2=foo",
        s"${file.getCanonicalFile}/p1=2/p2=foo",
        s"${file.getCanonicalFile}/p1=1/p2=bar",
        s"${file.getCanonicalFile}/p1=2/p2=bar"
      ).map { p =>
        val path = new Path(p)
        val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        path.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
      }

      val actualPaths = df.queryExecution.analyzed.collectFirst {
        case LogicalRelation(relation: HadoopFsRelation) =>
          relation.paths.toSet
      }.getOrElse {
        fail("Expect an FSBasedRelation, but none could be found")
      }

      assert(actualPaths === expectedPaths)
      checkAnswer(df, partitionedTestDF.collect())
    }
  }

  test("Partition column type casting") {
    withTempPath { file =>
      val input = partitionedTestDF.select('a, 'b, 'p1.cast(StringType).as('ps), 'p2)

      input
        .write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("ps", "p2")
        .saveAsTable("t")

      withTempTable("t") {
        checkAnswer(table("t"), input.collect())
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
      checkAnswer(table("t"), df.select('b, 'c, 'a).collect())
    }
  }
}

class SimpleTextHadoopFsRelationSuite extends HadoopFsRelationTest {
  override val dataSourceName: String = classOf[SimpleTextSource].getCanonicalName

  import sqlContext._

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    withTempDir { file =>
      val basePath = new Path(file.getCanonicalPath)
      val fs = basePath.getFileSystem(SparkHadoopUtil.get.conf)
      val qualifiedBasePath = fs.makeQualified(basePath)

      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(qualifiedBasePath, s"p1=$p1/p2=$p2")
        sparkContext
          .parallelize(for (i <- 1 to 3) yield s"$i,val_$i,$p1")
          .saveAsTextFile(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        read.format(dataSourceName)
          .option("dataSchema", dataSchemaWithPartition.json)
          .load(file.getCanonicalPath))
    }
  }
}

class CommitFailureTestRelationSuite extends SparkFunSuite with SQLTestUtils {
  import TestHive.implicits._

  override val sqlContext = TestHive

  val dataSourceName: String = classOf[CommitFailureTestSource].getCanonicalName

  test("SPARK-7684: commitTask() failure should fallback to abortTask()") {
    withTempPath { file =>
      val df = (1 to 3).map(i => i -> s"val_$i").toDF("a", "b")
      intercept[SparkException] {
        df.write.format(dataSourceName).save(file.getCanonicalPath)
      }

      val fs = new Path(file.getCanonicalPath).getFileSystem(SparkHadoopUtil.get.conf)
      assert(!fs.exists(new Path(file.getCanonicalPath, "_temporary")))
    }
  }
}

class ParquetHadoopFsRelationSuite extends HadoopFsRelationTest {
  override val dataSourceName: String = classOf[parquet.DefaultSource].getCanonicalName

  import sqlContext._
  import sqlContext.implicits._

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    withTempDir { file =>
      val basePath = new Path(file.getCanonicalPath)
      val fs = basePath.getFileSystem(SparkHadoopUtil.get.conf)
      val qualifiedBasePath = fs.makeQualified(basePath)

      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(qualifiedBasePath, s"p1=$p1/p2=$p2")
        sparkContext
          .parallelize(for (i <- 1 to 3) yield (i, s"val_$i", p1))
          .toDF("a", "b", "p1")
          .write.parquet(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        read.format(dataSourceName)
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

      checkAnswer(read.format("parquet").load(dir.getCanonicalPath), df.collect())
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
        intercept[RuntimeException] {
          df.write.format("parquet").mode(SaveMode.ErrorIfExists).save(path)
        }.getMessage.contains("already exists")
      }

      // This shouldn't throw anything.
      df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
      checkAnswer(read.format("parquet").load(path), df)
    }
  }

  test("SPARK-8079: Avoid NPE thrown from BaseWriterContainer.abortJob") {
    withTempPath { dir =>
      intercept[AnalysisException] {
        // Parquet doesn't allow field names with spaces.  Here we are intentionally making an
        // exception thrown from the `ParquetRelation2.prepareForWriteJob()` method to trigger
        // the bug.  Please refer to spark-8079 for more details.
        range(1, 10)
          .withColumnRenamed("id", "a b")
          .write
          .format("parquet")
          .save(dir.getCanonicalPath)
      }
    }
  }
}
