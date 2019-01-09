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

import scala.reflect.ClassTag

import org.apache.hadoop.mapreduce._

import org.apache.spark.SparkConf

/**
 * Interface for create output format/committer/writer used during saving an RDD using a Hadoop
 * OutputFormat (both from the old mapred API and the new mapreduce API)
 *
 * Notes:
 * 1. Implementations should throw [[IllegalArgumentException]] when wrong hadoop API is
 *    referenced;
 * 2. Implementations must be serializable, as the instance instantiated on the driver
 *    will be used for tasks on executors;
 * 3. Implementations should have a constructor with exactly one argument:
 *    (conf: SerializableConfiguration) or (conf: SerializableJobConf).
 */
abstract class HadoopWriteConfigUtil[K, V: ClassTag] extends Serializable {

  // --------------------------------------------------------------------------
  // Create JobContext/TaskAttemptContext
  // --------------------------------------------------------------------------

  def createJobContext(jobTrackerId: String, jobId: Int): JobContext

  def createTaskAttemptContext(
      jobTrackerId: String,
      jobId: Int,
      splitId: Int,
      taskAttemptId: Int): TaskAttemptContext

  // --------------------------------------------------------------------------
  // Create committer
  // --------------------------------------------------------------------------

  def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol

  // --------------------------------------------------------------------------
  // Create writer
  // --------------------------------------------------------------------------

  def initWriter(taskContext: TaskAttemptContext, splitId: Int): Unit

  def write(pair: (K, V)): Unit

  def closeWriter(taskContext: TaskAttemptContext): Unit

  // --------------------------------------------------------------------------
  // Create OutputFormat
  // --------------------------------------------------------------------------

  def initOutputFormat(jobContext: JobContext): Unit

  // --------------------------------------------------------------------------
  // Verify hadoop config
  // --------------------------------------------------------------------------

  def assertConf(jobContext: JobContext, conf: SparkConf): Unit
}
