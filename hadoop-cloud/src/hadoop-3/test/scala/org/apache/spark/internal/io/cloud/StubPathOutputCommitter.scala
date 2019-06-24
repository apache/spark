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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{PathOutputCommitter, PathOutputCommitterFactory}
import org.apache.hadoop.mapreduce.{JobContext, JobStatus, TaskAttemptContext}

/**
 * A local path output committer which tracks its state, for use in
 * tests.
 * @param outputPath final destination.
 * @param workPath work path
 * @param context task/job attempt.
 */
class StubPathOutputCommitter(
    outputPath: Path,
    workPath: Path,
    context: TaskAttemptContext) extends PathOutputCommitter(workPath, context) {

  var jobSetup: Boolean = false
  var jobCommitted: Boolean = false
  var jobAborted: Boolean = false

  var taskSetup: Boolean = false
  var taskCommitted: Boolean = false
  var taskAborted: Boolean = false
  var needsTaskCommit: Boolean = true

  override def getOutputPath: Path = outputPath

  override def getWorkPath: Path = {
    workPath
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    taskSetup = true
  }

  override def abortTask(taskAttemptContext: TaskAttemptContext): Unit = {
    taskAborted = true
  }

  override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
    taskCommitted = true
  }

  override def setupJob(jobContext: JobContext): Unit = {
    jobSetup = true
  }

  override def commitJob(jobContext: JobContext): Unit = {
    jobCommitted = true
  }

  override def abortJob(
      jobContext: JobContext,
      state: JobStatus.State): Unit = {
    jobAborted = true
  }

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    needsTaskCommit
  }

  override def toString(): String  = s"StubPathOutputCommitter(setup=$jobSetup," +
    s" committed=$jobCommitted, aborted=$jobAborted)"
}

/**
 * Factory.
 */
class StubPathOutputCommitterFactory extends PathOutputCommitterFactory {

  override def createOutputCommitter(
      outputPath: Path,
      context: TaskAttemptContext): PathOutputCommitter = {
    new StubPathOutputCommitter(outputPath, workPath(outputPath), context)
  }


  private def workPath(out: Path): Path = new Path(out, PathCommitterConstants.TEMP_DIR_NAME)
}

object StubPathOutputCommitterFactory {
  val Name: String = "org.apache.spark.internal.io.cloud.StubPathOutputCommitterFactory"

  /**
   * Given a hadoop configuration, set up the factory binding for the scheme.
   * @param conf config to patch
   * @param scheme filesystem scheme.
   */
  def bind(conf: Configuration, scheme: String): Unit = {
    val key = String.format(
      PathCommitterConstants.OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN, scheme)
    conf.set(key, Name)
  }

}
