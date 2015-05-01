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

import org.apache.spark.Logging
import org.apache.spark.streaming.{Time, StreamingContext}

/** To track the information of input stream at specified batch time. */
case class InputInfo(batchTime: Time, inputStreamId: Int, numRecords: Long)

/**
 * This class manages all the input streams as well as their input data statistics. The information
 * will output to StreamingListener for better monitoring.
 */
private[streaming] class InputInfoTracker(ssc: StreamingContext) extends Logging {

  // Map to track all the InputInfo related to specific batch time and input stream.
  private val batchTimeToInputInfos = new mutable.HashMap[Time, mutable.HashMap[Int, InputInfo]]

  /** Report the input information with batch time to the tracker */
  def reportInfo(batchTime: Time, inputInfo: InputInfo): Unit = synchronized {
    val inputInfos = batchTimeToInputInfos.getOrElseUpdate(batchTime,
      new mutable.HashMap[Int, InputInfo]())

    if (inputInfos.contains(inputInfo.inputStreamId)) {
      throw new IllegalStateException(s"Input stream ${inputInfo.inputStreamId}} for batch" +
        s"$batchTime is already added into InputInfoTracker, this is a illegal state")
    }
    inputInfos += ((inputInfo.inputStreamId, inputInfo))
  }

  /** Get the all the input stream's information of specified batch time */
  def getInfo(batchTime: Time): Map[Int, InputInfo] = synchronized {
    val inputInfos = batchTimeToInputInfos.get(batchTime)
    // Convert mutable HashMap to immutable Map for the caller
    inputInfos.map(_.toMap).getOrElse(Map[Int, InputInfo]())
  }

  /** Get the input information of specified batch time and input stream id */
  def getInfoOfBatchAndStream(batchTime: Time, inputStreamId: Int
    ): Option[InputInfo] = synchronized {
    batchTimeToInputInfos.get(batchTime).map(_.get(inputStreamId)).flatMap(x => x)
  }

  /** Cleanup the tracked input information older than threshold batch time */
  def cleanup(batchThreshTime: Time): Unit = synchronized {
    val timesToCleanup = batchTimeToInputInfos.keys.filter(_ < batchThreshTime)
    logInfo(s"remove old batch metadata: ${timesToCleanup.mkString(" ")}")

    batchTimeToInputInfos --= timesToCleanup
  }
}
