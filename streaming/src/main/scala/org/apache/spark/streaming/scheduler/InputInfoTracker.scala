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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{StreamingContext, Time}

/**
 * :: DeveloperApi ::
 * Track the information of input stream at specified batch time.
 *
 * @param inputStreamId the input stream id
 * @param numRecords the number of records in a batch
 * @param metadata metadata for this batch. It should contain at least one standard field named
 *                 "Description" which maps to the content that will be shown in the UI.
 */
@DeveloperApi
case class StreamInputInfo(
    inputStreamId: Int, numRecords: Long, metadata: Map[String, Any] = Map.empty) {
  require(numRecords >= 0, "numRecords must not be negative")

  def metadataDescription: Option[String] =
    metadata.get(StreamInputInfo.METADATA_KEY_DESCRIPTION).map(_.toString)
}

@DeveloperApi
object StreamInputInfo {

  /**
   * The key for description in `StreamInputInfo.metadata`.
   */
  val METADATA_KEY_DESCRIPTION: String = "Description"
}

/**
 * This class manages all the input streams as well as their input data statistics. The information
 * will be exposed through StreamingListener for monitoring.
 */
private[streaming] class InputInfoTracker(ssc: StreamingContext) extends Logging {

  // Map to track all the InputInfo related to specific batch time and input stream.
  private val batchTimeToInputInfos =
    new mutable.HashMap[Time, mutable.HashMap[Int, StreamInputInfo]]

  /** Report the input information with batch time to the tracker */
  def reportInfo(batchTime: Time, inputInfo: StreamInputInfo): Unit = synchronized {
    val inputInfos = batchTimeToInputInfos.getOrElseUpdate(batchTime,
      new mutable.HashMap[Int, StreamInputInfo]())

    if (inputInfos.contains(inputInfo.inputStreamId)) {
      throw new IllegalStateException(s"Input stream ${inputInfo.inputStreamId} for batch" +
        s"$batchTime is already added into InputInfoTracker, this is a illegal state")
    }
    inputInfos += ((inputInfo.inputStreamId, inputInfo))
  }

  /** Get the all the input stream's information of specified batch time */
  def getInfo(batchTime: Time): Map[Int, StreamInputInfo] = synchronized {
    val inputInfos = batchTimeToInputInfos.get(batchTime)
    // Convert mutable HashMap to immutable Map for the caller
    inputInfos.map(_.toMap).getOrElse(Map[Int, StreamInputInfo]())
  }

  /** Cleanup the tracked input information older than threshold batch time */
  def cleanup(batchThreshTime: Time): Unit = synchronized {
    val timesToCleanup = batchTimeToInputInfos.keys.filter(_ < batchThreshTime)
    logInfo(s"remove old batch metadata: ${timesToCleanup.mkString(" ")}")
    batchTimeToInputInfos --= timesToCleanup
  }
}
