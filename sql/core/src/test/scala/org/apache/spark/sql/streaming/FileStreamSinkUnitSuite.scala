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

package org.apache.spark.sql.streaming

import java.io.{File, FilenameFilter}

import com.google.common.io.PatternFilenameFilter
import org.apache.commons.io.FileUtils
import org.apache.hadoop.mapreduce.JobContext
import org.scalatest.BeforeAndAfter
import scala.io.Source

import org.apache.spark.SparkException
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.streaming.{FileStreamSink, ManifestFileCommitProtocol, StagingFileCommitProtocol}
import org.apache.spark.sql.internal.SQLConf.STREAMING_FILE_COMMIT_PROTOCOL_CLASS


class FailingManifestFileCommitProtocol(jobId: String, path: String)
  extends StagingFileCommitProtocol(jobId, path) {
  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    logError("Skipping job commit simulating ungraceful shutdown")
  }
}

class ExceptionThrowingManifestFileCommitProtocol(jobId: String, path: String)
  extends StagingFileCommitProtocol(jobId, path) {
  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    throw new IllegalStateException("Simulating exception on job commit")
  }
}


class FileStreamSinkUnitSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  val dir = new File("destination_path")
  val fruits = Seq("apple", "peach", "citron")

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  before {
    FileUtils.deleteQuietly(dir)
  }

  test("add batch results in files") {
    val df = fruits.toDF()
    val sink = new FileStreamSink(spark, dir.getName, new TextFileFormat(), Seq.empty, Map.empty)
    SQLExecution.withNewExecutionId(spark, new QueryExecution(spark, df.logicalPlan)) {
      sink.addBatch(1, df)
    }

    assertResults(dir, fruits)
  }

  test("add same batch again will not duplicate results") {
    val df = fruits.toDF()
    val sink = new FileStreamSink(spark, dir.getName, new TextFileFormat(), Seq.empty, Map.empty)
    SQLExecution.withNewExecutionId(spark, new QueryExecution(spark, df.logicalPlan)) {
      sink.addBatch(1, df)
      sink.addBatch(1, df)
    }

    assertResults(dir, fruits)
  }

  test("adding batch again after job commit failure should not duplicate items") {
    val df = fruits.toDF()
    var sink = new FileStreamSink(spark, dir.getName, new TextFileFormat(), Seq.empty, Map.empty)
    sqlContext.setConf(STREAMING_FILE_COMMIT_PROTOCOL_CLASS.key,
      classOf[FailingManifestFileCommitProtocol].getCanonicalName)
    SQLExecution.withNewExecutionId(spark, new QueryExecution(spark, df.logicalPlan)) {
      sink.addBatch(1, df)
    }

    sqlContext.setConf(STREAMING_FILE_COMMIT_PROTOCOL_CLASS.key,
      STREAMING_FILE_COMMIT_PROTOCOL_CLASS.defaultValueString)
    SQLExecution.withNewExecutionId(spark, new QueryExecution(spark, df.logicalPlan)) {
      sink = new FileStreamSink(spark, dir.getName, new TextFileFormat(), Seq.empty, Map.empty)
      sink.addBatch(1, df)
    }

    assertResults(dir, fruits)
  }

  test("adding batch again after job commit throwing exception should not duplicate items") {
    val df = fruits.toDF()
    var sink = new FileStreamSink(spark, dir.getName, new TextFileFormat(), Seq.empty, Map.empty)
    sqlContext.setConf(STREAMING_FILE_COMMIT_PROTOCOL_CLASS.key,
      classOf[ExceptionThrowingManifestFileCommitProtocol].getCanonicalName)
    intercept[SparkException] {
      SQLExecution.withNewExecutionId(spark, new QueryExecution(spark, df.logicalPlan)) {
        sink.addBatch(1, df)
      }
    }

    sqlContext.setConf(STREAMING_FILE_COMMIT_PROTOCOL_CLASS.key,
      STREAMING_FILE_COMMIT_PROTOCOL_CLASS.defaultValueString)
    SQLExecution.withNewExecutionId(spark, new QueryExecution(spark, df.logicalPlan)) {
      sink = new FileStreamSink(spark, dir.getName, new TextFileFormat(), Seq.empty, Map.empty)
      sink.addBatch(1, df)
    }

    assertResults(dir, fruits)
  }

  private def assertResults(dir: File, fruits: Seq[String]) = {
    val output = dir.listFiles(new PatternFilenameFilter("part-.*")).flatMap {
      file =>
        val source = Source.fromFile(file)
        source.getLines().toSeq
    }.toSeq.sortBy(x => x)

    assert(output == fruits.sortBy(x => x))
  }
}
