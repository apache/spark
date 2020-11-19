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

package org.apache.spark.internal.io

import org.apache.hadoop.mapreduce.TaskAttemptContext

abstract class FileCommitProtocolV2 extends FileCommitProtocol {

  @deprecated("use newTaskTempFileV2", "3.1.0")
  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String

  @deprecated("use newTaskTempFileAbsPathV2", "3.1.0")
  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String

  /**
   * Notifies the commit protocol to add a new file, and gets back the full path that should be
   * used. Must be called on the executors when running tasks.
   *
   * Note that the returned temp file may have an arbitrary path. The commit protocol only
   * promises that the file will be at the location specified by the arguments after job commit.
   *
   * A full file path consists of the following parts:
   *  1. the base path
   *  2. the relative file path
   *
   * The "relativeFilePath" parameter specifies 2. The base path is left to the commit protocol
   * implementation to decide.
   *
   * Important: it is the caller's responsibility to add uniquely identifying content to
   * "relativeFilePath" if a task is going to write out multiple files to the same dir. The file
   * commit protocol only guarantees that files written by different tasks will not conflict.
   */
  def newTaskTempFileV2(taskContext: TaskAttemptContext, relativeFilePath: String): String

  /**
   * Similar to newTaskTempFileV2(), but allows files to committed to an absolute output location.
   * Depending on the implementation, there may be weaker guarantees around adding files this way.
   *
   * Important: it is the caller's responsibility to add uniquely identifying content to
   * "absoluteFilePath" if a task is going to write out multiple files to the same dir. The file
   * commit protocol only guarantees that files written by different tasks will not conflict.
   */
  def newTaskTempFileAbsPathV2(taskContext: TaskAttemptContext, absoluteFilePath: String): String
}

object FileCommitProtocolV2 {

  final def getFilename(
      taskContext: TaskAttemptContext, jobId: String, prefix: String, ext: String): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"${prefix}part-$split%05d-$jobId$ext"
  }
}
