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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{OutputCommitter, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileNameSpec, HadoopMapReduceCommitProtocol}

/**
 * A variant of [[HadoopMapReduceCommitProtocol]] that allows specifying the actual
 * Hadoop output committer using an option specified in SQLConf.
 */
class SQLPathHadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    stagingDir: Path,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, path, stagingDir, dynamicPartitionOverwrite)
    with Serializable with Logging {

  val sqlPathOutputCommitter: SQLPathOutputCommitter =
    committer.asInstanceOf[SQLPathOutputCommitter]

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val committer = new SQLPathOutputCommitter(stagingDir, new Path(path), context)
    logInfo(s"Using output committer class ${committer.getClass.getCanonicalName}")
    committer
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String = {
    val filename = getFilename(taskContext, spec)
    dir.map { d =>
      new Path(
        new Path(sqlPathOutputCommitter.getTaskAttemptPath(taskContext), d), filename).toString
    }.getOrElse {
      new Path(sqlPathOutputCommitter.getTaskAttemptPath(taskContext), filename).toString
    }
  }
}
