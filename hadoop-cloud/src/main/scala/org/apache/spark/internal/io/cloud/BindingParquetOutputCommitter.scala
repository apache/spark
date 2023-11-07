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
import org.apache.hadoop.mapreduce.{JobContext, JobStatus, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{BindingPathOutputCommitter, PathOutputCommitter}
import org.apache.parquet.hadoop.ParquetOutputCommitter

import org.apache.spark.internal.Logging

/**
 * This Parquet Committer subclass dynamically binds to the factory-configured
 * output committer, and is intended to allow callers to use any 'PathOutputCommitter',
 * even if not a subclass of 'ParquetOutputCommitter'.
 *
 * The Parquet `parquet.enable.summary-metadata` option will only be supported
 * if the instantiated committer itself supports it.
 */
class BindingParquetOutputCommitter(
    path: Path,
    context: TaskAttemptContext)
  extends ParquetOutputCommitter(path, context) with Logging with StreamCapabilities {

  logTrace(s"${this.getClass.getName} binding to configured PathOutputCommitter and dest $path")

  private val committer = new BindingPathOutputCommitter(path, context)

  /**
   * This is the committer ultimately bound to.
   * @return the committer instantiated by the factory.
   */
  private[cloud] def boundCommitter(): PathOutputCommitter = {
    committer.getCommitter
  }

  override def getWorkPath(): Path = {
    committer.getWorkPath()
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.setupTask(taskAttemptContext)
  }

  override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.commitTask(taskAttemptContext)
  }

  override def abortTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.abortTask(taskAttemptContext)
  }

  override def setupJob(jobContext: JobContext): Unit = {
    committer.setupJob(jobContext)
  }

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    committer.needsTaskCommit(taskAttemptContext)
  }

  override def cleanupJob(jobContext: JobContext): Unit = {
    committer.cleanupJob(jobContext)
  }

  override def isCommitJobRepeatable(jobContext: JobContext): Boolean = {
    committer.isCommitJobRepeatable(jobContext)
  }

  override def commitJob(jobContext: JobContext): Unit = {
    committer.commitJob(jobContext)
  }

  override def recoverTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.recoverTask(taskAttemptContext)
  }

  /**
   * Abort the job; log and ignore any IO exception thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param jobContext job context
   * @param state final state of the job
   */
  override def abortJob(jobContext: JobContext, state: JobStatus.State): Unit = {
    try {
      committer.abortJob(jobContext, state)
    } catch {
      case e: IOException =>
        // swallow exception to avoid problems when called within exception
        // handlers
        logWarning("Abort job failed", e)
    }
  }

  override def isRecoverySupported: Boolean = {
    committer.isRecoverySupported()
  }

  override def isRecoverySupported(jobContext: JobContext): Boolean = {
    committer.isRecoverySupported(jobContext)
  }

  override def toString: String = s"BindingParquetOutputCommitter($committer)"

  override def hasCapability(capability: String): Boolean =
    committer.isInstanceOf[StreamCapabilities] &&
      committer.asInstanceOf[StreamCapabilities].hasCapability(capability)
}
