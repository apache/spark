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
import java.sql.Timestamp

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

import org.apache.spark.TestUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}
import org.apache.spark.util.Utils

private class OnlyDetectCustomPathFileCommitProtocol(jobId: String, path: String)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path)
    with Serializable with Logging {

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new Exception("there should be no custom partition path")
  }
}

class PartitionedWriteSuite extends QueryTest with SharedSparkSession {
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
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 4)

      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", 2).mode("overwrite").parquet(f.getAbsolutePath)
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 2)

      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", -1).mode("overwrite").parquet(f.getAbsolutePath)
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 1)
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
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 4)
    }
  }

  test("append data to an existing partitioned table without custom partition path") {
    withTable("t") {
      withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[OnlyDetectCustomPathFileCommitProtocol].getName) {
        Seq((1, 2)).toDF("a", "b").write.partitionBy("b").saveAsTable("t")
        // if custom partition path is detected by the task, it will throw an Exception
        // from OnlyDetectCustomPathFileCommitProtocol above.
        Seq((3, 2)).toDF("a", "b").write.mode("append").partitionBy("b").saveAsTable("t")
      }
    }
  }

  private def checkPartitionValues(file: File, expected: String): Unit = {
    val dir = file.getParentFile()
    val value = ExternalCatalogUtils.unescapePathName(
      dir.getName.substring(dir.getName.indexOf("=") + 1))
    assert(value == expected)
  }

  test("timeZone setting in dynamic partition writes") {
    val ts = Timestamp.valueOf("2016-12-01 00:00:00")
    val df = Seq((1, ts)).toDF("i", "ts")
    withTempPath { f =>
      df.write.partitionBy("ts").parquet(f.getAbsolutePath)
      val files = TestUtils.recursiveList(f).filter(_.getAbsolutePath.endsWith("parquet"))
      assert(files.length == 1)
      checkPartitionValues(files.head, "2016-12-01 00:00:00")
    }
    withTempPath { f =>
      df.write.option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
        .partitionBy("ts").parquet(f.getAbsolutePath)
      val files = TestUtils.recursiveList(f).filter(_.getAbsolutePath.endsWith("parquet"))
      assert(files.length == 1)
      // use timeZone option utcTz.getId to format partition value.
      checkPartitionValues(files.head, "2016-12-01 08:00:00")
    }
    withTempPath { f =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        df.write.partitionBy("ts").parquet(f.getAbsolutePath)
        val files = TestUtils.recursiveList(f).filter(_.getAbsolutePath.endsWith("parquet"))
        assert(files.length == 1)
        // if there isn't timeZone option, then use session local timezone.
        checkPartitionValues(files.head, "2016-12-01 08:00:00")
      }
    }
  }

  test("SPARK-31968: duplicate partition columns check") {
    withTempPath { f =>
      checkError(
        exception = intercept[AnalysisException] {
          Seq((3, 2)).toDF("a", "b").write.partitionBy("b", "b").csv(f.getAbsolutePath)
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`b`"))
    }
  }

  test("SPARK-27194 SPARK-29302: Fix commit collision in dynamic partition overwrite mode") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->
      SQLConf.PartitionOverwriteMode.DYNAMIC.toString,
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[PartitionFileExistCommitProtocol].getName) {
      withTempDir { d =>
        withTable("t") {
          sql(
            s"""
               | create table t(c1 int, p1 int) using parquet partitioned by (p1)
               | location '${d.getAbsolutePath}'
            """.stripMargin)

          val df = Seq((1, 2)).toDF("c1", "p1")
          df.write
            .partitionBy("p1")
            .mode("overwrite")
            .saveAsTable("t")
          checkAnswer(sql("select * from t"), df)
        }
      }
    }
  }

  test("SPARK-37231, SPARK-37240: Dynamic writes/reads of ANSI interval partitions") {
    Seq("parquet", "json").foreach { format =>
      Seq(
        "INTERVAL '100' YEAR" -> YearMonthIntervalType(YEAR),
        "INTERVAL '-1-1' YEAR TO MONTH" -> YearMonthIntervalType(YEAR, MONTH),
        "INTERVAL '1000 02:03:04.123' DAY TO SECOND" -> DayTimeIntervalType(DAY, SECOND),
        "INTERVAL '-10' MINUTE" -> DayTimeIntervalType(MINUTE)
      ).foreach { case (intervalStr, intervalType) =>
        withTempPath { f =>
          val df = sql(s"select 0 AS id, $intervalStr AS diff")
          assert(df.schema("diff").dataType === intervalType)
          df.write
            .partitionBy("diff")
            .format(format)
            .save(f.getAbsolutePath)
          val files = TestUtils.recursiveList(f).filter(_.getAbsolutePath.endsWith(format))
          assert(files.length == 1)
          checkPartitionValues(files.head, intervalStr)
          val schema = new StructType()
            .add("id", IntegerType)
            .add("diff", intervalType)
          checkAnswer(spark.read.schema(schema).format(format).load(f.getAbsolutePath), df)
        }
      }
    }
  }
}

/**
 * A file commit protocol with pre-created partition file. when try to overwrite partition dir
 * in dynamic partition mode, FileAlreadyExist exception would raise without SPARK-27194
 */
private class PartitionFileExistCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {
  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)
    val stagingDir = new File(new Path(path).toUri.getPath, s".spark-staging-$jobId")
    stagingDir.mkdirs()
    val stagingPartDir = new File(stagingDir, "p1=2")
    stagingPartDir.mkdirs()
    val conflictTaskFile = new File(stagingPartDir, s"part-00000-$jobId.c000.snappy.parquet")
    conflictTaskFile.createNewFile()
  }
}
