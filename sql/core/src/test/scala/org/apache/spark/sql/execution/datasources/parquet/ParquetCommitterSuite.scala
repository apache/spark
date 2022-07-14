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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat}

import org.apache.spark.{LocalSparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test logic related to choice of output committers.
 */
class ParquetCommitterSuite extends SparkFunSuite with SQLTestUtils
  with LocalSparkContext {

  private val PARQUET_COMMITTER = classOf[ParquetOutputCommitter].getCanonicalName

  protected var spark: SparkSession = _

  /**
   * Create a new [[SparkSession]] running in local-cluster mode with unsafe and codegen enabled.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("testing")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  test("alternative output committer, merge schema") {
    writeDataFrame(MarkingFileOutput.COMMITTER, summary = true, check = true)
  }

  test("alternative output committer, no merge schema") {
    writeDataFrame(MarkingFileOutput.COMMITTER, summary = false, check = true)
  }

  test("Parquet output committer, merge schema") {
    writeDataFrame(PARQUET_COMMITTER, summary = true, check = false)
  }

  test("Parquet output committer, no merge schema") {
    writeDataFrame(PARQUET_COMMITTER, summary = false, check = false)
  }

  /**
   * Write a trivial dataframe as Parquet, using the given committer
   * and job summary option.
   * @param committer committer to use
   * @param summary create a job summary
   * @param check look for a marker file
   * @return if a marker file was sought, it's file status.
   */
  private def writeDataFrame(
      committer: String,
      summary: Boolean,
      check: Boolean): Option[FileStatus] = {
    var result: Option[FileStatus] = None
    val summaryLevel = if (summary) {
      "ALL"
    } else {
      "NONE"
    }
    withSQLConf(
      SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key -> committer,
      ParquetOutputFormat.JOB_SUMMARY_LEVEL -> summaryLevel) {
        withTempPath { dest =>
          val df = spark.createDataFrame(Seq((1, "4"), (2, "2")))
          val destPath = new Path(dest.toURI)
          df.write.format("parquet").save(destPath.toString)
          if (check) {
            result = Some(MarkingFileOutput.checkMarker(
              destPath,
              spark.sessionState.newHadoopConf()))
          }
        }
    }
    result
  }
}

/**
 * A file output committer which explicitly touches a file "marker"; this
 * is how tests can verify that this committer was used.
 * @param outputPath output path
 * @param context task context
 */
private class MarkingFileOutputCommitter(
    outputPath: Path,
    context: TaskAttemptContext) extends FileOutputCommitter(outputPath, context) {

  override def commitJob(context: JobContext): Unit = {
    super.commitJob(context)
    MarkingFileOutput.touch(outputPath, context.getConfiguration)
  }
}

private object MarkingFileOutput {

  val COMMITTER = classOf[MarkingFileOutputCommitter].getCanonicalName

  /**
   * Touch the marker.
   * @param outputPath destination directory
   * @param conf configuration to create the FS with
   */
  def touch(outputPath: Path, conf: Configuration): Unit = {
    outputPath.getFileSystem(conf).create(new Path(outputPath, "marker")).close()
  }

  /**
   * Get the file status of the marker
   *
   * @param outputPath destination directory
   * @param conf configuration to create the FS with
   * @return the status of the marker
   * @throws java.io.FileNotFoundException if the marker is absent
   */
  def checkMarker(outputPath: Path, conf: Configuration): FileStatus = {
    outputPath.getFileSystem(conf).getFileStatus(new Path(outputPath, "marker"))
  }
}
