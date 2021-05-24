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
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.sql.internal.SQLConf

/**
 * A variant of [[HadoopMapReduceCommitProtocol]] that allows specifying the actual
 * Hadoop output committer using an option specified in SQLConf.
 */
class SQLHadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite)
    with Serializable with Logging {

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    var committer = super.setupCommitter(context)

    val configuration = context.getConfiguration
    val clazz =
      configuration.getClass(SQLConf.OUTPUT_COMMITTER_CLASS.key, null, classOf[OutputCommitter])

    if (clazz != null) {
      logInfo(s"Using user defined output committer class ${clazz.getCanonicalName}")

      // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
      // has an associated output committer. To override this output committer,
      // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
      // If a data source needs to override the output committer, it needs to set the
      // output committer in prepareForWrite method.
      if (classOf[FileOutputCommitter].isAssignableFrom(clazz)) {
        // The specified output committer is a FileOutputCommitter.
        // So, we will use the FileOutputCommitter-specified constructor.
        val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
        val committerOutputPath = if (dynamicPartitionOverwrite) stagingDir else new Path(path)
        committer = ctor.newInstance(committerOutputPath, context)
      } else {
        // The specified output committer is just an OutputCommitter.
        // So, we will use the no-argument constructor.
        val ctor = clazz.getDeclaredConstructor()
        committer = ctor.newInstance()
      }
    }
    logInfo(s"Using output committer class ${committer.getClass.getCanonicalName}")
    committer
  }
}
