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

package org.apache.spark.sql.execution.streaming

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.mockito.{Matchers, Mockito}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.io.FileCommitProtocol.EmptyTaskCommitMessage
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.util.Utils

class StagingFileCommitProtocolSuite extends SparkFunSuite with BeforeAndAfter {
  val array: Array[Byte] = new Array[Byte](1000)
  val taskId = 333
  val taskId2 = 334
  val jobID = 123
  val attemptId: Int = 444
  val attemptIdNext: Int = 445
  val taskAttemptId: TaskAttemptID = new TaskAttemptID("SPARK", jobID, true, taskId, attemptId)
  val taskAttemptId2: TaskAttemptID = new TaskAttemptID("SPARK", jobID, true, taskId2, attemptId)
  val basePath = Utils.createTempDir().getCanonicalFile.toString
  val protocol = newCommitProtocol(jobID)
  val hadoopConf = createConf(0, taskAttemptId)
  val tx = new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  val fs = new Path(basePath).getFileSystem(hadoopConf)
  val ctx: JobContext = Job.getInstance(hadoopConf)

  val session = new TestSparkSession()
  def newCommitProtocol(batchId: Int): StagingFileCommitProtocol = {
    val p = new StagingFileCommitProtocol(batchId.toString, basePath)
    val logClass = classOf[MetadataLog[Array[SinkFileStatus]]]
    val log: MetadataLog[Array[SinkFileStatus]] = Mockito.mock(logClass)
    when(log.add(Matchers.anyInt, Matchers.any(classOf[Array[SinkFileStatus]])))
        .thenReturn(true)

    p.setupManifestOptions(
      log,
      batchId)
    p
  }

  def createConf(partition: Int, taskAttemptId: TaskAttemptID): Configuration = {
    val hc = new Configuration()
    hc.set("mapreduce.job.id", jobID.toString)
    hc.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hc.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hc.setBoolean("mapreduce.task.ismap", true)
    hc.setInt("mapreduce.task.partition", partition)
    session
    hc
  }

  after {
    Utils.deleteRecursively(new File(basePath))
  }

  test("file is generated on job commit") {
    protocol.setupJob(ctx)

    protocol.setupTask(tx)
    val fileName = protocol.newTaskTempFile(tx, None, "ext")
    writeToFile(fileName, "data")

    protocol.onTaskCommit(EmptyTaskCommitMessage)

    protocol.commitJob(ctx, Seq(EmptyTaskCommitMessage))

    assert(fileContents == Set("data"))
  }

  test("file is generated into partition subdirectory") {
    protocol.setupJob(ctx)

    protocol.setupTask(tx)
    val fileName = protocol.newTaskTempFile(tx, Some("subdir"), "ext")
    writeToFile(fileName, "data")

    protocol.onTaskCommit(EmptyTaskCommitMessage)

    protocol.commitJob(ctx, Seq(EmptyTaskCommitMessage))

    assert(fileContents(new Path(basePath, "subdir")) == Set("data"))
  }

  test("before job commit file is not visible") {
    protocol.setupJob(ctx)

    protocol.setupTask(tx)
    val fileName = protocol.newTaskTempFile(tx, None, "ext")
    writeToFile(fileName, "data")

    protocol.onTaskCommit(EmptyTaskCommitMessage)

    assert(fileContents == Set())

  }

  test("2 tasks can write in the same job") {
    protocol.setupJob(ctx)
    protocol.setupTask(tx)

    val protocol2 = newCommitProtocol(jobID)
    val tx2 = new TaskAttemptContextImpl(createConf(1, taskAttemptId2), taskAttemptId2)
    protocol2.setupTask(tx2)

    val fileName = protocol.newTaskTempFile(tx, None, "ext")
    writeToFile(fileName, "data0")

    val fileName2 = protocol2.newTaskTempFile(tx2, None, "ext")
    writeToFile(fileName2, "data1")

    protocol.onTaskCommit(EmptyTaskCommitMessage)
    protocol2.onTaskCommit(EmptyTaskCommitMessage)

    protocol.commitJob(ctx, Seq())
    assert(fileContents == Set("data0", "data1"))
  }

  test("same task can be executed twice") {
    protocol.setupJob(ctx)
    protocol.setupTask(tx)

    val fileName = protocol.newTaskTempFile(tx, None, "ext")
    writeToFile(fileName, "data")

    protocol.onTaskCommit(EmptyTaskCommitMessage)

    protocol.commitJob(ctx, Seq())

    val protocol2 = newCommitProtocol(jobID)
    val attempt = new TaskAttemptID("SPARK", jobID, true, taskId, attemptIdNext)
    hadoopConf.set("mapreduce.task.attempt.id", attempt.toString)
    val tx2 = new TaskAttemptContextImpl(hadoopConf, attempt)
    protocol2.setupJob(tx2)
    protocol2.setupTask(tx2)

    val fileName2 = protocol2.newTaskTempFile(tx2, None, "ext")
    writeToFile(fileName2, "data")
    protocol2.onTaskCommit(EmptyTaskCommitMessage)
    protocol2.commitJob(tx2, Seq())

    assert(fileContents == Set("data"))

  }

  test("task without job commit can be restarted") {
    protocol.setupJob(ctx)
    protocol.setupTask(tx)

    val fileName = protocol.newTaskTempFile(tx, None, "ext")
    writeToFile(fileName, "data")

    protocol.onTaskCommit(EmptyTaskCommitMessage)

    val protocol2 = newCommitProtocol(jobID)
    val attempt = new TaskAttemptID("SPARK", jobID, true, taskId, attemptIdNext)
    hadoopConf.set("mapreduce.task.attempt.id", attempt.toString)
    val tx2 = new TaskAttemptContextImpl(hadoopConf, attempt)
    protocol2.setupJob(tx2)
    protocol2.setupTask(tx2)

    val fileName2 = protocol2.newTaskTempFile(tx2, None, "ext")
    writeToFile(fileName2, "data")
    protocol2.onTaskCommit(EmptyTaskCommitMessage)
    protocol2.commitJob(tx2, Seq())

    assert(fileContents == Set("data"))

  }


  test("multiple files can be generated by same task") {
    protocol.setupJob(ctx)

    protocol.setupTask(tx)
    val fileName = protocol.newTaskTempFile(tx, None, "ext")
    writeToFile(fileName, "data0")

    val fileName2 = protocol.newTaskTempFile(tx, None, "ext")
    writeToFile(fileName2, "data1")

    protocol.onTaskCommit(EmptyTaskCommitMessage)

    protocol.commitJob(ctx, Seq(EmptyTaskCommitMessage))

    assert(fileContents == Set("data0", "data1"))
  }



  private def fileContents: Set[String] = {
    val path = new Path(basePath)
    fileContents(path)
  }

  private def fileContents(path: Path): Set[String] = {
    val files: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, false)

    val fileList = Seq.newBuilder[LocatedFileStatus]
    while (files.hasNext) {
      fileList += files.next()
    }
    val allData = fileList.result().map(f => {
      val stream = fs.open(f.getPath)
      val length = stream.read(array)
      array.slice(0, length).map(_.toChar).mkString
    })
    allData.toSet
  }

  private def writeToFile(fileName: String, data: String) = {
    val file = new Path(fileName)
    val fs = file.getFileSystem(hadoopConf)
    val os = fs.create(file)
    os.write(data.getBytes())
    os.close()
  }

}
