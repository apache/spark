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

package org.apache.spark.sql.execution.datasources

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class FileFormatWriterSuite
  extends QueryTest
  with SharedSparkSession
  with CodegenInterpretedPlanTest {

  import testImplicits._

  test("empty file should be skipped while write to file") {
    withTempPath { path =>
      spark.range(100).repartition(10).where("id = 50").write.parquet(path.toString)
      val partFiles = path.listFiles()
        .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      assert(partFiles.length === 2)
    }
  }

  test("SPARK-22252: FileFormatWriter should respect the input query schema") {
    withTable("t1", "t2", "t3", "t4") {
      spark.range(1).select($"id" as Symbol("col1"), $"id" as Symbol("col2"))
        .write.saveAsTable("t1")
      spark.sql("select COL1, COL2 from t1").write.saveAsTable("t2")
      checkAnswer(spark.table("t2"), Row(0, 0))

      // Test picking part of the columns when writing.
      spark.range(1)
        .select($"id", $"id" as Symbol("col1"), $"id" as Symbol("col2"))
        .write.saveAsTable("t3")
      spark.sql("select COL1, COL2 from t3").write.saveAsTable("t4")
      checkAnswer(spark.table("t4"), Row(0, 0))
    }
  }

  test("Null and '' values should not cause dynamic partition failure of string types") {
    withTable("t1", "t2") {
      Seq((0, None), (1, Some("")), (2, None)).toDF("id", "p")
        .write.partitionBy("p").saveAsTable("t1")
      checkAnswer(spark.table("t1").sort("id"), Seq(Row(0, null), Row(1, null), Row(2, null)))

      sql("create table t2(id long, p string) using parquet partitioned by (p)")
      sql("insert overwrite table t2 partition(p) select id, p from t1")
      checkAnswer(spark.table("t2").sort("id"), Seq(Row(0, null), Row(1, null), Row(2, null)))
    }
  }

  test("SPARK-33904: save and insert into a table in a namespace of spark_catalog") {
    val ns = "spark_catalog.ns"
    withNamespace(ns) {
      spark.sql(s"CREATE NAMESPACE $ns")
      val t = s"$ns.tbl"
      withTable(t) {
        spark.range(1).write.saveAsTable(t)
        Seq(100).toDF().write.insertInto(t)
        checkAnswer(spark.table(t), Seq(Row(0), Row(100)))
      }
    }
  }

  test("SPARK-52978: Customized FileFormatWriter") {
    withSQLConf(SQLConf.FILE_FORMAT_WRITER_CLASS.key ->
      classOf[FileFormatWriterSuite.MyFileFormatWriter].getName) {
      withTable("t1") {
        spark.range(100).write.saveAsTable("t1")
        assert(spark.table("t1").count() == 100)
      }
      assert(FileFormatWriterSuite.MyFileFormatWriter.instantiationCount.get() > 0)
      assert(FileFormatWriterSuite.MyFileFormatWriter.writePlanCount.get() > 0)
    }
  }
}

object FileFormatWriterSuite {
  class MyFileFormatWriter extends FileFormatWriter {
    MyFileFormatWriter.instantiationCount.getAndIncrement()

    // scalastyle:off argcount
    override def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: FileFormatWriter.OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String], numStaticPartitionCols: Int): Set[String] = {
      val partitionPaths = DefaultFileFormatWriter.write(
        sparkSession,
        plan,
        fileFormat,
        committer,
        outputSpec,
        hadoopConf,
        partitionColumns,
        bucketSpec,
        statsTrackers,
        options
      )
      MyFileFormatWriter.writePlanCount.getAndIncrement()
      partitionPaths
    }
    // scalastyle:on argcount

    override private[spark] def executeTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow],
      concurrentOutputWriterSpec: Option[FileFormatWriter.ConcurrentOutputWriterSpec]):
    WriteTaskResult = {
      DefaultFileFormatWriter.executeTask(
        description,
        jobTrackerID,
        sparkStageId,
        sparkPartitionId,
        sparkAttemptNumber,
        committer,
        iterator,
        concurrentOutputWriterSpec
      )
    }
  }

  object MyFileFormatWriter {
    val instantiationCount: AtomicInteger = new AtomicInteger(0)
    val writePlanCount: AtomicInteger = new AtomicInteger(0)
  }
}
