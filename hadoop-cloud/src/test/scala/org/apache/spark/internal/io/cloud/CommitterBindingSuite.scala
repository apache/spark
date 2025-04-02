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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, StreamCapabilities}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.mapreduce.{Job, JobStatus, MRJobConfig, TaskAttemptContext, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.output.{BindingPathOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec}
import org.apache.spark.internal.io.cloud.PathOutputCommitProtocol.{CAPABILITY_DYNAMIC_PARTITIONING, OUTPUTCOMMITTER_FACTORY_SCHEME}

class CommitterBindingSuite extends SparkFunSuite {

  private val jobId = "2007071202143_0101"
  private val taskAttempt0 = "attempt_" + jobId + "_m_000000_0"
  private val taskAttemptId0 = TaskAttemptID.forName(taskAttempt0)

  /**
   * The classname to use when referring to the path output committer.
   */
  private val pathCommitProtocolClassname: String = classOf[PathOutputCommitProtocol].getName

  /** hadoop-mapreduce option to enable the _SUCCESS marker. */
  private val successMarker = "mapreduce.fileoutputcommitter.marksuccessfuljobs"

  /**
   * Does the
   * [[BindingParquetOutputCommitter]] committer bind to the schema-specific
   * committer declared for the destination path? And that lifecycle events
   * are correctly propagated?
   * This only works with a hadoop build where BindingPathOutputCommitter
   * does passthrough of stream capabilities, so check that first.
   */
  test("BindingParquetOutputCommitter binds to the inner committer") {

    val path = new Path("http://example/data")
    val job = newJob(path)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)
    StubPathOutputCommitterBinding.bindWithDynamicPartitioning(conf, "http")
    val tContext: TaskAttemptContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
    val parquet = new BindingParquetOutputCommitter(path, tContext)
    val inner = parquet.boundCommitter().asInstanceOf[StubPathOutputCommitterWithDynamicPartioning]
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

    val binding = new BindingPathOutputCommitter(path, tContext)
    // MAPREDUCE-7403 only arrived after hadoop 3.3.4; this test case
    // is designed to work with versions with and without the feature.
    if (binding.isInstanceOf[StreamCapabilities]) {
      // this version of hadoop does support hasCapability probes
      // through the BindingPathOutputCommitter used by the
      // parquet committer, so verify that it goes through
      // to the stub committer.
      assert(parquet.hasCapability(CAPABILITY_DYNAMIC_PARTITIONING),
        s"committer $parquet does not declare dynamic partition support")
    }
  }

  /**
   * Create a a new job. Sets the task attempt ID.
   *
   * @return the new job
   */
  def newJob(outDir: Path): Job = {
    val job = Job.getInstance(new Configuration())
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
    conf.setBoolean(successMarker, true)
    FileOutputFormat.setOutputPath(job, outDir)
    job
  }

  test("committer protocol can be serialized and deserialized") {
    val tempDir = File.createTempFile("ser", ".bin")

    tempDir.delete()
    val committer = new PathOutputCommitProtocol(jobId, tempDir.toURI.toString, false)

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

      assert(committer.destination === committer2.destination,
        "destination mismatch on round trip")
      assert(committer.destPath === committer2.destPath,
        "destPath mismatch on round trip")
    } finally {
      IOUtils.closeStreams(out, in)
      serData.delete()
    }
  }

  test("local filesystem instantiation") {
    val instance = FileCommitProtocol.instantiate(
      pathCommitProtocolClassname,
      jobId, "file:///tmp", false)

    val protocol = instance.asInstanceOf[PathOutputCommitProtocol]
    assert("file:///tmp" === protocol.destination)
  }

  /*
   * Bind a job to a committer which doesn't support dynamic partitioning.
   * Job setup must fail, and calling `newTaskTempFileAbsPath()` must
   * raise `UnsupportedOperationException`.
   */
  test("reject dynamic partitioning if not supported") {
    val path = new Path("http://example/data")
    val job = newJob(path)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)
    StubPathOutputCommitterBinding.bind(conf, "http")
    val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
    val committer = FileCommitProtocol.instantiate(
      pathCommitProtocolClassname,
      jobId,
      path.toUri.toString,
      true)
    val ioe = intercept[IOException] {
      committer.setupJob(tContext)
    }
    if (!ioe.getMessage.contains(PathOutputCommitProtocol.UNSUPPORTED)) {
      throw ioe
    }

    // calls to newTaskTempFileAbsPath() will be rejected
    intercept[UnsupportedOperationException] {
      verifyAbsTempFileWorks(tContext, committer)
    }
  }

  /*
   * Bind to a committer with dynamic partitioning support,
   * verify that job and task setup works, and that
   * `newTaskTempFileAbsPath()` creates a temp file which
   * can be moved to an absolute path later.
   */
  test("permit dynamic partitioning if the committer says it works") {
    val path = new Path("http://example/data")
    val job = newJob(path)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)
    StubPathOutputCommitterBinding.bindWithDynamicPartitioning(conf, "http")
    val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
    val committer: PathOutputCommitProtocol = FileCommitProtocol.instantiate(
      pathCommitProtocolClassname,
      jobId,
      path.toUri.toString,
      true).asInstanceOf[PathOutputCommitProtocol]
    committer.setupJob(tContext)
    committer.setupTask(tContext)
    verifyAbsTempFileWorks(tContext, committer)
  }

  /*
   * Create a FileOutputCommitter through the PathOutputCommitProtocol
   * using the relevant factory in hadoop-mapreduce-core JAR.
   */
  test("FileOutputCommitter through PathOutputCommitProtocol") {
    // temp path; use a unique filename
    val jobCommitDir = File.createTempFile(
      "FileOutputCommitter-through-PathOutputCommitProtocol",
      "")
    try {
      // delete the temp file and create a temp dir.
      jobCommitDir.delete();
      val jobUri = jobCommitDir.toURI
      // hadoop path of the job
      val path = new Path(jobUri)
      val job = newJob(path)
      val conf = job.getConfiguration
      conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
      conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)
      bindToFileOutputCommitterFactory(conf, "file")
      val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
      val committer: PathOutputCommitProtocol = FileCommitProtocol.instantiate(
        pathCommitProtocolClassname,
        jobId,
        jobUri.toString,
        true).asInstanceOf[PathOutputCommitProtocol]
      committer.setupJob(tContext)
      committer.setupTask(tContext)
      verifyAbsTempFileWorks(tContext, committer)
    } finally {
      jobCommitDir.delete();
    }
  }

  /**
   * Verify that a committer supports `newTaskTempFileAbsPath()`.
   *
   * @param tContext task context
   * @param committer committer
   */
  private def verifyAbsTempFileWorks(
    tContext: TaskAttemptContextImpl,
    committer: FileCommitProtocol): Unit = {
    val spec = FileNameSpec(".lotus.", ".123")
    val absPath = committer.newTaskTempFileAbsPath(
      tContext,
      "/tmp",
      spec)
    assert(absPath.endsWith(".123"), s"wrong suffix in $absPath")
    assert(absPath.contains("lotus"), s"wrong prefix in $absPath")
  }

  /**
   * Given a hadoop configuration, explicitly set up the factory binding for the scheme
   * to a committer factory which always creates FileOutputCommitters.
   *
   * @param conf   config to patch
   * @param scheme filesystem scheme.
   */
  def bindToFileOutputCommitterFactory(conf: Configuration, scheme: String): Unit = {
    conf.set(OUTPUTCOMMITTER_FACTORY_SCHEME + "." + scheme,
      "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory")
  }

}

