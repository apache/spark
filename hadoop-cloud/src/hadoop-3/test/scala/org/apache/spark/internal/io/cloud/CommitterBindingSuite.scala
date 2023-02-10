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

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, StreamCapabilities}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.mapreduce.{Job, JobStatus, MRJobConfig, TaskAttemptContext, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.output.{BindingPathOutputCommitter, FileOutputFormat, PathOutputCommitterFactory}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.cloud.PathOutputCommitProtocol._
import org.apache.spark.sql.internal.SQLConf

class CommitterBindingSuite extends SparkFunSuite {

  private val jobId = "2007071202143_0101"
  private val taskAttempt0 = "attempt_" + jobId + "_m_000000_0"
  private val taskAttemptId0 = TaskAttemptID.forName(taskAttempt0)

  /**
   * The classname to use when referring to the path output committer.
   */
  private val pathCommitProtocolClassname: String = classOf[PathOutputCommitProtocol]
    .getName

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
    val conf = newJob(path).getConfiguration
    StubPathOutputCommitterBinding.bindWithDynamicPartitioning(conf, "http")
    val tContext: TaskAttemptContext = new TaskAttemptContextImpl(conf,
      taskAttemptId0)
    val parquet = new BindingParquetOutputCommitter(path, tContext)
    val inner = parquet.boundCommitter
      .asInstanceOf[StubPathOutputCommitterWithDynamicPartioning]
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
    // MAPREDUCE-7403 only arrived in hadoop 3.3.3; this test case
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
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)
    conf.setBoolean(successMarker, true)
    FileOutputFormat.setOutputPath(job, outDir)
    job
  }

  test("committer protocol can be serialized and deserialized") {
    val tempDir = File.createTempFile("ser", ".bin")

    tempDir.delete()
    val committer = new PathOutputCommitProtocol(jobId, tempDir.toURI.toString,
      false)

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
   * Bind a job to a committer which doesn't support dynamic partitioning,
   * but request dynamic partitioning in the protocol instantiation.
   * This works, though a warning will have appeared in the log and
   * the performance of the job commit is unknown and potentially slow.
   */
  test("permit dynamic partitioning even not declared as supported") {
    val path = new Path("http://example/dir1/dir2/dir3")
    val conf = newJob(path).getConfiguration
    StubPathOutputCommitterBinding.bind(conf, "http")
    val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
    val committer = instantiateCommitter(path, true)
    committer.setupTask(tContext)
    assert(committer.getPartitions.isDefined,
      "committer partition list should be defined")

    val file1 = new Path(
      committer.newTaskTempFile(tContext, Option("part=1"), ".csv"))
    assert(file1.getName.endsWith(".csv"), s"wrong suffix in $file1")
    assert(file1.getParent.getName === "part=1", s"wrong parent dir in $file1")
    val partionSet1 = committer.getPartitions.get
    assert(partionSet1 === Set("part=1"))

    val file2 = new Path(
      committer.newTaskTempFile(tContext, Option("part=2"),
        FileNameSpec("prefix", ".csv")))
    assert(file2.getName.endsWith(".csv"), s"wrong suffix in $file1")
    assert(file2.getName.startsWith("prefix"), s"wrong prefix in $file1")

    val partionSet2 = committer.getPartitions.get
    assert(partionSet2 === Set("part=1", "part=2"))

    // calls to newTaskTempFileAbsPath() will be accepted
    verifyAbsTempFileWorks(tContext, committer)
  }

  /*
   * Bind a job to a committer which doesn't support dynamic partitioning,
   * but request dynamic partitioning in the protocol instantiation.
   * This works, though a warning will have appeared in the log and
   * the performance of the job commit is unknown and potentially slow.
   */
  test("basic committer") {
    val path = new Path("http://example/dir1/dir2/dir3")
    val conf = newJob(path).getConfiguration
    StubPathOutputCommitterBinding.bind(conf, "http")
    val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
    val committer = instantiateCommitter(path, false)
    committer.setupTask(tContext)
    assert(committer.getPartitions.isDefined,
      "committer partition list should be defined")

    val file1 = new Path(
      committer.newTaskTempFile(tContext, Option("part=1"), ".csv"))
    assert(file1.getName.endsWith(".csv"), s"wrong suffix in $file1")
    assert(file1.getParent.getName === "part=1", s"wrong parent dir in $file1")

    assert(committer.getPartitions.get.isEmpty,
      "partitions are being collected in a non-dynamic job")

    // calls to newTaskTempFileAbsPath() will be accepted
    verifyAbsTempFileWorks(tContext, committer)
  }

  /**
   * Instantiate a committer.
   *
   * @param path path to bind to
   * @param dynamic use dynamicPartitionOverwrite
   * @return the committer
   */
  private def instantiateCommitter(path: Path,
      dynamic: Boolean): PathOutputCommitProtocol = {
    FileCommitProtocol.instantiate(
      pathCommitProtocolClassname,
      jobId,
      path.toUri.toString,
      dynamic).asInstanceOf[PathOutputCommitProtocol]
  }

  /*
   * Bind to a committer with dynamic partitioning support,
   * verify that job and task setup works, and that
   * `newTaskTempFileAbsPath()` creates a temp file which
   * can be moved to an absolute path later.
   */
  test("permit dynamic partitioning if the committer says it works") {
    val path = new Path("http://example/dir1/dir2/dir3")
    val job = newJob(path)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt0)
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)
    StubPathOutputCommitterBinding.bindWithDynamicPartitioning(conf, "http")
    val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
    val committer: PathOutputCommitProtocol = instantiateCommitter(path, true)
    committer.setupJob(tContext)
    committer.setupTask(tContext)
    verifyAbsTempFileWorks(tContext, committer)

    // attempt to create files in directories above the job
    // dir, which in dynamic partitioning will result in the delete
    // of the parent dir, hence loss of the job.
    /// for safety, this is forbidden.
    List("/dir1", "/dir1/dir2", "/dir1/dir2/dir3", "", "/").foreach { d =>
      intercept[IllegalArgumentException] {
        committer.newTaskTempFileAbsPath(tContext, d, ".ext")
      }
    }
    // "adjacent" paths and child paths are valid.
    List("/d", "/dir12", "/dir1/dir2/dir30", "/dir1/dir2/dir3/dir4")
      .foreach {
        committer.newTaskTempFileAbsPath(tContext, _, ".ext")
      }
  }

  /*
   * Create a FileOutputCommitter through the PathOutputCommitProtocol
   * using the relevant factory in hadoop-mapreduce-core JAR.
   */
  test("Dynamic FileOutputCommitter through PathOutputCommitProtocol") {
    // temp path; use a unique filename
    val jobCommitDir = File.createTempFile(
      "FileOutputCommitter-through-PathOutputCommitProtocol",
      "")
    try {
      // delete the temp file and create a temp dir.
      jobCommitDir.delete()
      // hadoop path of the job
      val path = new Path(jobCommitDir.toURI)
      val conf = newJob(path).getConfiguration
      bindToFileOutputCommitterFactory(conf, "file")
      val tContext = new TaskAttemptContextImpl(conf, taskAttemptId0)
      val committer = instantiateCommitter(path, true)
      committer.setupJob(tContext)
      // unless/until setupTask() is invoked. the partition list is not created,
      // this means that the job manager instance will return None
      // on a call to getPartitions.
      assert(committer.getPartitions.isEmpty,
        "committer partition list should be empty")
      committer.setupTask(tContext)
      verifyAbsTempFileWorks(tContext, committer)
    } finally {
      jobCommitDir.delete()
    }
  }

  /**
   * When the FileOutputCommitter has been forcibly disabled,
   * attempting to create it will raise an exception.
   */
  test("FileOutputCommitter disabled") {
    // temp path; use a unique filename
    val jobCommitDir = File.createTempFile(
      "FileOutputCommitter-disabled",
      "")
    try {
      // delete the temp file and create a temp dir.
      jobCommitDir.delete()
      // hadoop path of the job
      val path = new Path(jobCommitDir.toURI)
      val conf = newJob(path).getConfiguration
      bindToFileOutputCommitterFactory(conf, "file")
      conf.setBoolean(REJECT_FILE_OUTPUT, true)
      intercept[IllegalArgumentException] {
        instantiateCommitter(path, true)
          .setupJob(new TaskAttemptContextImpl(conf, taskAttemptId0))
      }
      // the committer never created the destination directory
      assert(!jobCommitDir.exists(),
        s"job commit dir $jobCommitDir should not have been created")
    } finally {
      jobCommitDir.delete()
    }
  }

  /**
   * Verify that a committer supports `newTaskTempFileAbsPath()`,
   * returning a new file under /tmp.
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
   * @param conf config to patch
   * @param scheme filesystem scheme.
   */
  def bindToFileOutputCommitterFactory(conf: Configuration,
      scheme: String): Unit = {

    conf.set(OUTPUTCOMMITTER_FACTORY_SCHEME + "." + scheme,
      CommitterBindingSuite.FILE_OUTPUT_COMMITTER_FACTORY)
  }

}

/**
 * Constants for this and related test suites
 */
private[cloud] object CommitterBindingSuite extends Logging {
  val FILE_OUTPUT_COMMITTER_FACTORY: String = "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory"

  val PATH_OUTPUT_COMMITTER_NAME: String = "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"

  val BINDING_PARQUET_OUTPUT_COMMITTER_CLASS: String =
    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"

  /**
   * Options to bind to the path committer through SQL and parquet.
   */
  val BIND_TO_PATH_COMMITTER: Map[String, String] = Map(
    SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key ->
      BINDING_PARQUET_OUTPUT_COMMITTER_CLASS,
    SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key -> PATH_OUTPUT_COMMITTER_NAME,
  )

  /**
   * Prefix to use for manifest committer options.
   */
  val MANIFEST_OPT_PREFIX = "spark.hadoop.mapreduce.manifest.committer."

  /**
   * Directory for saving job summary reports.
   * These are the `_SUCCESS` files, but are saved even on
   * job failures.
   */
  val OPT_SUMMARY_REPORT_DIR: String = MANIFEST_OPT_PREFIX +
    "summary.report.directory"

  /**
   * Directory under target/ for reports.
   */
  val JOB_REPORTS_DIR = "./target/reports/"

  /**
   * Subdir for collected IOStatistics.
   */
  val IOSTATS_SUBDIR = "iostats"

  /**
   * Subdir for manifest committer _SUMMARY files.
   */
  val SUMMARY_SUBDIR = "summary"

  /**
   * Enable the path committer in a spark configuration, including optionally
   * the manifest committer if the test is run on a hadoop build with it.
   * This committer, which works on file:// repositories
   * scales better on azure and google cloud stores, and
   * collects and reports IOStatistics from task commit IO as well
   * as Job commit operations.
   * @param conf the configuration to modify
   * @param tryToUseManifest should the manifest be probed for and enabled if found?
   * @return (is the manifest in use, report directory)
   */
  def enablePathCommitter(conf: SparkConf,
      tryToUseManifest: Boolean): (Boolean, File) = {
    val reportsDir = new File(JOB_REPORTS_DIR).getCanonicalFile
    val statisticsDir = new File(reportsDir, IOSTATS_SUBDIR).getCanonicalFile
    conf.setAll(BIND_TO_PATH_COMMITTER)
      .set(REPORT_DIR,
        statisticsDir.toURI.toString)

    if (!tryToUseManifest) {
      // no need to look for the manifest.
      return (false, reportsDir)
    }
    // look for the manifest committer exactly once.
    val loader = getClass.getClassLoader

    var usingManifest = try {
      loader.loadClass(PathOutputCommitProtocol.MANIFEST_COMMITTER_FACTORY)
      // manifest committer class was found so bind to and configure it.
      logInfo("Using Manifest Committer")
      conf.set(PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS,
        MANIFEST_COMMITTER_FACTORY)
      // save full _SUCCESS files for the curious; this includes timings
      // of operations in task as well as job commit.
      conf.set(OPT_SUMMARY_REPORT_DIR,
        new File(reportsDir, SUMMARY_SUBDIR).getCanonicalFile.toURI.toString)
      true
    } catch {
      case _: ClassNotFoundException =>
        val mapredJarUrl = loader.getResource(
          "org/apache/hadoop/mapreduce/lib/output/PathOutputCommitterFactory.class")
        logInfo(
          s"Manifest Committer not found in JAR $mapredJarUrl; using FileOutputCommitter")
        false
    }
    (usingManifest, reportsDir)
  }
}



