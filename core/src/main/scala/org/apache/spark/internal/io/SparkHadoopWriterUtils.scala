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

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import scala.util.{DynamicVariable, Random}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, JobID}

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.OutputMetrics

/**
 * A helper object that provide common utils used during saving an RDD using a Hadoop OutputFormat
 * (both from the old mapred API and the new mapreduce API)
 */
private[spark]
object SparkHadoopWriterUtils {

  private val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256
  private val RAND = new Random()

  // For job tracker IDs
  private val DATE_TIME_FORMATTER =
    DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss", Locale.US)
      .withZone(ZoneId.systemDefault())

  /**
   * Create a job ID.
   *
   * @param time (current) time
   * @param id job number
   * @return a job ID
   */
  def createJobID(time: Date, id: Int): JobID = {
    val jobTrackerID = createJobTrackerID(time)
    createJobID(jobTrackerID, id)
  }

  /**
   * Create a job ID.
   *
   * @param jobTrackerID unique job track id
   * @param id job number
   * @return a job ID
   */
  def createJobID(jobTrackerID: String, id: Int): JobID = {
    if (id < 0) {
      throw new IllegalArgumentException("Job number is negative")
    }
    new JobID(jobTrackerID, id)
  }

  /**
   * Generate an ID for a job tracker.
   * @param time (current) time
   * @return a string for a job ID
   */
  def createJobTrackerID(time: Date): String = {
    val base = DATE_TIME_FORMATTER.format(time.toInstant)
    var l1 = RAND.nextLong()
    if (l1 < 0) {
      l1 = -l1
    }
    base + l1
  }

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  // Note: this needs to be a function instead of a 'val' so that the disableOutputSpecValidation
  // setting can take effect:
  def isOutputSpecValidationEnabled(conf: SparkConf): Boolean = {
    val validationDisabled = disableOutputSpecValidation.value
    val enabledInConf = conf.getBoolean("spark.hadoop.validateOutputSpecs", true)
    enabledInConf && !validationDisabled
  }

  // TODO: these don't seem like the right abstractions.
  // We should abstract the duplicate code in a less awkward way.

  def initHadoopOutputMetrics(context: TaskContext): (OutputMetrics, () => Long) = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    (context.taskMetrics().outputMetrics, bytesWrittenCallback)
  }

  def maybeUpdateOutputMetrics(
      outputMetrics: OutputMetrics,
      callback: () => Long,
      recordsWritten: Long): Unit = {
    if (recordsWritten % RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      outputMetrics.setBytesWritten(callback())
      outputMetrics.setRecordsWritten(recordsWritten)
    }
  }

  /**
   * Allows for the `spark.hadoop.validateOutputSpecs` checks to be disabled on a case-by-case
   * basis; see SPARK-4835 for more details.
   */
  val disableOutputSpecValidation: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
}
