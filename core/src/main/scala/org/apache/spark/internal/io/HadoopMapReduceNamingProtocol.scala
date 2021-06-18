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

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

/**
 * An [[FileNamingProtocol]] implementation for [[HadoopMapReduceCommitProtocol]].
 */
class HadoopMapReduceNamingProtocol(
    jobId: String,
    path: String,
    commitProtocol: FileCommitProtocol)
  extends FileNamingProtocol with Serializable {

  require(commitProtocol.isInstanceOf[HadoopMapReduceCommitProtocol])

  private val hadoopMRCommitProtocol = commitProtocol.asInstanceOf[HadoopMapReduceCommitProtocol]

  override def getTaskStagingPath(
      taskContext: TaskAttemptContext, fileContext: FileContext): String = {
    val filename = getFilename(taskContext, fileContext)
    fileContext.absoluteDir match {
      case Some(_) =>
        new Path(hadoopMRCommitProtocol.stagingDir, UUID.randomUUID().toString + "-" + filename)
          .toString
      case _ =>
        val stagingDir: Path = hadoopMRCommitProtocol.getCommitter match {
          // For FileOutputCommitter it has its own staging path called "work path".
          case f: FileOutputCommitter =>
            new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
          case _ => new Path(path)
        }

        fileContext.relativeDir.map { d =>
          new Path(new Path(stagingDir, d), filename).toString
        }.getOrElse {
          new Path(stagingDir, filename).toString
        }
    }
  }

  override def getTaskFinalPath(
      taskContext: TaskAttemptContext, fileContext: FileContext): String = {
    require(fileContext.absoluteDir.isDefined)
    val filename = getFilename(taskContext, fileContext)
    new Path(fileContext.absoluteDir.get, filename).toString
  }

  protected def getFilename(taskContext: TaskAttemptContext, fileContext: FileContext): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val prefix = fileContext.prefix.getOrElse("")
    val ext = fileContext.ext
    f"${prefix}part-$split%05d-$jobId$ext"
  }
}
