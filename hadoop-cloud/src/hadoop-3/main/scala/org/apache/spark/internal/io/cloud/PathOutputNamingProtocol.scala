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

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, FileContext, HadoopMapReduceNamingProtocol}

/**
 * An [[FileNamingProtocol]] implementation for [[PathOutputCommitProtocol]].
 */
class PathOutputNamingProtocol(
    jobId: String,
    dest: String,
    commitProtocol: FileCommitProtocol)
  extends HadoopMapReduceNamingProtocol(jobId, dest, commitProtocol) with Logging {

  require(commitProtocol.isInstanceOf[PathOutputCommitProtocol])

  private val pathOutputCommitProtocol = commitProtocol.asInstanceOf[PathOutputCommitProtocol]

  override def getTaskStagingPath(
      taskContext: TaskAttemptContext, fileContext: FileContext): String = {
    val filename = getFilename(taskContext, fileContext)
    fileContext.absoluteDir match {
      case Some(_) =>
        new Path(pathOutputCommitProtocol.stagingDir, UUID.randomUUID().toString + "-" + filename)
          .toString
      case _ =>
        val workDir = pathOutputCommitProtocol.getCommitter.getWorkPath
        val parent = fileContext.relativeDir.map {
          d => new Path(workDir, d)
        }.getOrElse(workDir)
        val file = new Path(parent, filename)
        logTrace(s"Creating task file $file for dir ${fileContext.relativeDir} and ext " +
          s"${fileContext.ext}")
        file.toString
    }
  }
}
