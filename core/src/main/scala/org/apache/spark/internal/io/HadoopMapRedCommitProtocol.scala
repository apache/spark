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

import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.{TaskAttemptContext => NewTaskAttemptContext}

import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.MDC

/**
 * An [[FileCommitProtocol]] implementation backed by an underlying Hadoop OutputCommitter
 * (from the old mapred API).
 *
 * Unlike Hadoop's OutputCommitter, this implementation is serializable.
 */
class HadoopMapRedCommitProtocol(jobId: String, path: String)
  extends HadoopMapReduceCommitProtocol(jobId, path) {

  override def setupCommitter(context: NewTaskAttemptContext): OutputCommitter = {
    val config = context.getConfiguration.asInstanceOf[JobConf]
    val committer = config.getOutputCommitter
    logInfo(log"Using output committer class" +
      log" ${MDC(LogKeys.CLASS_NAME, committer.getClass.getCanonicalName)}")
    committer
  }
}
