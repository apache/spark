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

import java.io.IOException

import org.apache.hadoop.fs.{Path, StreamCapabilities}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, PathOutputCommitter, PathOutputCommitterFactory}

import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol

/**
 * Spark Commit protocol for Path Output Committers.
 * This committer will work with the `FileOutputCommitter` and subclasses.
 * All implementations *must* be serializable.
 *
 * Rather than ask the `FileOutputFormat` for a committer, it uses the
 * `org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory` factory
 * API to create the committer.
 *
 * In `setupCommitter` the factory is identified and instantiated;
 * this factory then creates the actual committer implementation.
 *
 * Dynamic Partition support will be determined once the committer is
 * instantiated in the setupJob/setupTask methods. If this
 * class was instantiated with `dynamicPartitionOverwrite` set to true,
 * then the instantiated committer must either be an instance of
 * `FileOutputCommitter` or it must implement the `StreamCapabilities`
 * interface and declare that it has the capability
 * `mapreduce.job.committer.dynamic.partitioning`.
 * That feature is available on Hadoop releases with the Intermediate
 * Manifest Committer for GCS and ABFS; it is not supported by the
 * S3A committers.
 * @constructor Instantiate.
 * @param jobId                     job
 * @param dest                      destination
 * @param dynamicPartitionOverwrite does the caller want support for dynamic
 *                                  partition overwrite?
 */
class PathOutputCommitProtocol(
    jobId: String,
    dest: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, dest, dynamicPartitionOverwrite)
    with Serializable {

  /** The committer created. */
  @transient private var committer: PathOutputCommitter = _

  require(dest != null, "Null destination specified")

  private[cloud] val destination: String = dest

  /** The destination path. This is serializable in Hadoop 3. */
  private[cloud] val destPath: Path = new Path(destination)

  logTrace(s"Instantiated committer with job ID=$jobId;" +
    s" destination=$destPath;" +
    s" dynamicPartitionOverwrite=$dynamicPartitionOverwrite")

  import PathOutputCommitProtocol._

  /**
   * Set up the committer.
   * This creates it by talking directly to the Hadoop factories, instead
   * of the V1 `mapred.FileOutputFormat` methods.
   * @param context task attempt
   * @return the committer to use. This will always be a subclass of
   *         `PathOutputCommitter`.
   */
  override protected def setupCommitter(context: TaskAttemptContext): PathOutputCommitter = {
    logTrace(s"Setting up committer for path $destination")
    committer = PathOutputCommitterFactory.createCommitter(destPath, context)

    // Special feature to force out the FileOutputCommitter, so as to guarantee
    // that the binding is working properly.
    val rejectFileOutput = context.getConfiguration
      .getBoolean(REJECT_FILE_OUTPUT, REJECT_FILE_OUTPUT_DEFVAL)
    if (rejectFileOutput && committer.isInstanceOf[FileOutputCommitter]) {
      // the output format returned a file output format committer, which
      // is exactly what we do not want. So switch back to the factory.
      val factory = PathOutputCommitterFactory.getCommitterFactory(
        destPath,
        context.getConfiguration)
      logTrace(s"Using committer factory $factory")
      committer = factory.createOutputCommitter(destPath, context)
    }

    logTrace(s"Using committer ${committer.getClass}")
    logTrace(s"Committer details: $committer")
    if (committer.isInstanceOf[FileOutputCommitter]) {
      require(!rejectFileOutput,
        s"Committer created is the FileOutputCommitter $committer")

      if (committer.isCommitJobRepeatable(context)) {
        // If FileOutputCommitter says its job commit is repeatable, it means
        // it is using the v2 algorithm, which is not safe for task commit
        // failures. Warn
        logTrace(s"Committer $committer may not be tolerant of task commit failures")
      }
    } else {
      // if required other committers need to be checked for dynamic partition
      // compatibility through a StreamCapabilities probe.
      if (dynamicPartitionOverwrite) {
        if (supportsDynamicPartitions) {
          logDebug(
            s"Committer $committer has declared compatibility with dynamic partition overwrite")
        } else {
          throw new IOException(PathOutputCommitProtocol.UNSUPPORTED + ": " + committer)
        }
      }
    }
    committer
  }


  /**
   * Does the instantiated committer support dynamic partitions?
   * @return true if the committer declares itself compatible.
   */
  private def supportsDynamicPartitions = {
    committer.isInstanceOf[FileOutputCommitter] ||
      (committer.isInstanceOf[StreamCapabilities] &&
        committer.asInstanceOf[StreamCapabilities]
          .hasCapability(CAPABILITY_DYNAMIC_PARTITIONING))
  }

  /**
   * Create a temporary file for a task.
   *
   * @param taskContext task context
   * @param dir         optional subdirectory
   * @param spec        file naming specification
   * @return a path as a string
   */
  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String = {

    val workDir = committer.getWorkPath
    val parent = dir.map {
      d => new Path(workDir, d)
    }.getOrElse(workDir)
    val file = new Path(parent, getFilename(taskContext, spec))
    logTrace(s"Creating task file $file for dir $dir and spec $spec")
    file.toString
  }

  /**
   * Reject any requests for an absolute path file on a committer which
   * is not compatible with it.
   *
   * @param taskContext task context
   * @param absoluteDir final directory
   * @param spec output filename
   * @return a path string
   * @throws UnsupportedOperationException if incompatible
   */
  override def newTaskTempFileAbsPath(
    taskContext: TaskAttemptContext,
    absoluteDir: String,
    spec: FileNameSpec): String = {

    if (supportsDynamicPartitions) {
      super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    } else {
      throw new UnsupportedOperationException(s"Absolute output locations not supported" +
        s" by committer $committer")
    }
  }
}

object PathOutputCommitProtocol {

  /**
   * Hadoop configuration option.
   * Fail fast if the committer is using the path output protocol.
   * This option can be used to catch configuration issues early.
   *
   * It's mostly relevant when testing/diagnostics, as it can be used to
   * enforce that schema-specific options are triggering a switch
   * to a new committer.
   */
  val REJECT_FILE_OUTPUT = "pathoutputcommit.reject.fileoutput"

  /**
   * Default behavior: accept the file output.
   */
  val REJECT_FILE_OUTPUT_DEFVAL = false

  /** Error string for tests. */
  private[cloud] val UNSUPPORTED: String = "PathOutputCommitter does not support" +
    " dynamicPartitionOverwrite"

  /**
   * Stream Capabilities probe for spark dynamic partitioning compatibility.
   */
  private[cloud] val CAPABILITY_DYNAMIC_PARTITIONING =
    "mapreduce.job.committer.dynamic.partitioning"

  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  private[cloud] val OUTPUTCOMMITTER_FACTORY_SCHEME = "mapreduce.outputcommitter.factory.scheme"
}
