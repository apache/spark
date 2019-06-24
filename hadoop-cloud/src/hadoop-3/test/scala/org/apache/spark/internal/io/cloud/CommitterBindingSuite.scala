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

package org.apache.spark.internal.io.cloud

import java.io.{File, FileInputStream, FileOutputStream, IOException, ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.InvocationTargetException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, JobStatus, MRJobConfig, TaskAttemptID}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.io.{FileCommitProtocol, cloud}
import org.apache.spark.internal.io.cloud.PathCommitterConstants._

/**
 * Test committer binding logic.
 */
class CommitterBindingSuite extends SparkFunSuite {

  private val jobId = "2007071202143_0101"
  private val taskAttempt0 = "attempt_" + jobId + "_m_000000_0"
  private val taskAttemptId0 = TaskAttemptID.forName(taskAttempt0)

  /**
   * Does the
   * [[BindingParquetOutputCommitter]] committer bind to the schema-specific
   * committer declared for the destination path?
   */
  test("BindingParquetOutputCommitter lifecycle") {
    val path = new Path("http://example/data")
    val job = newJob(path)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)

    StubPathOutputCommitterFactory.bind(conf, "http")
    val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
    val parquet = new BindingParquetOutputCommitter(path, tContext)
    val inner = parquet.boundCommitter().asInstanceOf[StubPathOutputCommitter]
    parquet.setupJob(tContext)
    assert(inner.jobSetup, s"$inner job not setup")
    parquet.setupTask(tContext)
    assert(inner.taskSetup, s"$inner task not setup")
    assert(parquet.needsTaskCommit(tContext), "needsTaskCommit false")
    inner.needsTaskCommit = false
    assert(!parquet.needsTaskCommit(tContext), "needsTaskCommit true")
    parquet.commitTask(tContext)
    assert(inner.taskCommitted, s"$inner task not committed")
    parquet.abortTask(tContext)
    assert(inner.taskAborted, s"$inner task not aborted")
    parquet.commitJob(tContext)
    assert(inner.jobCommitted, s"$inner job not committed")
    parquet.abortJob(tContext, JobStatus.State.RUNNING)
    assert(inner.jobAborted, s"$inner job not aborted")
  }

  test("cloud binding to SparkConf") {
    val sc = new SparkConf()
    cloud.bind(sc)
  }

  /**
   * Create a a new job. Sets the task attempt ID.
   *
   * @return the new job
   * @throws IOException failure
   */
  @throws[IOException]
  def newJob(outDir: Path): Job = {
    val job = Job.getInstance(new Configuration())
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
    conf.setBoolean(CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    FileOutputFormat.setOutputPath(job, outDir)
    job
  }

  /**
   * Verify that the committer protocol can be serialized and that
   * round trips work.
   */
  test("CommitterSerialization") {
    val tempDir = File.createTempFile("ser", ".bin")

    tempDir.delete();
    val committer = new PathOutputCommitProtocol(jobId, tempDir.toURI.toString,false)

    val serData = File.createTempFile("ser", ".bin")
    var out: ObjectOutputStream = null
    var in: ObjectInputStream = null

    try {
      out = new ObjectOutputStream(new FileOutputStream(serData))
      out.writeObject(committer)
      out.close
      in = new ObjectInputStream(new FileInputStream(serData))
      val result = in.readObject()

      val committer2 = result.asInstanceOf[PathOutputCommitProtocol]

      assert(committer.getDestination() === committer2.getDestination,
        "destination mismatch on round trip")
      assert(committer.destPath === committer2.destPath,
        "destPath mismatch on round trip")
    } finally {
      IOUtils.closeStreams(out, in)
      serData.delete()
    }
  }

  test("Instantiate") {
    val instance = FileCommitProtocol.instantiate(
      cloud.PATH_COMMIT_PROTOCOL_CLASSNAME,
      jobId, "file:///tmp", false)

    val protocol = instance.asInstanceOf[PathOutputCommitProtocol]
    assert("file:///tmp"=== protocol.getDestination())
  }

  test("InstantiateNoDynamicPartitioning") {
    val ex = intercept[InvocationTargetException] {
      FileCommitProtocol.instantiate(
        cloud.PATH_COMMIT_PROTOCOL_CLASSNAME,
        jobId, "file:///tmp", true)
    }
    val cause = ex.getCause
    if (cause == null || !cause.isInstanceOf[IOException]) {
      // very unexpected: throw the exception and its full stack.
      throw ex
    }
    assert(cause.getMessage.contains(PathOutputCommitProtocol.UNSUPPORTED))
  }

}
