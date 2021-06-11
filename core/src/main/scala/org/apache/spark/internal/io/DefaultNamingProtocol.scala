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

/**
 * An [[FileNamingProtocol]] implementation by default for custom [[FileCommitProtocol]]
 * implementations.
 *
 * This delegates to call [[FileCommitProtocol.newTaskTempFile]] and
 * [[FileCommitProtocol.newTaskTempFileAbsPath()]] to be backward compatible with
 * custom [[FileCommitProtocol]] implementation before Spark 3.2.0. Newer implementation of
 * [[FileCommitProtocol]] after Spark 3.2.0 should create own implementation of
 * [[FileNamingProtocol]], instead of using this.
 */
class DefaultNamingProtocol(
    jobId: String,
    path: String,
    commitProtocol: FileCommitProtocol)
  extends FileNamingProtocol with Serializable {

  override def getTaskStagingPath(
      taskContext: TaskAttemptContext, fileContext: FileContext): String = {
    fileContext.absoluteDir match {
      case Some(dir) => commitProtocol.newTaskTempFileAbsPath(
        taskContext, dir, fileContext.ext)
      case _ => commitProtocol.newTaskTempFile(
        taskContext, fileContext.relativeDir, fileContext.ext)
    }
  }

  override def getTaskFinalPath(
      taskContext: TaskAttemptContext, fileContext: FileContext): String = {
    ""
  }
}
