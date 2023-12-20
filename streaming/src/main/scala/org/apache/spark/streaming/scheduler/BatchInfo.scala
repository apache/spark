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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Time

/**
 * :: DeveloperApi ::
 * Class having information on completed batches.
 * @param batchTime   Time of the batch
 * @param streamIdToInputInfo A map of input stream id to its input info
 * @param submissionTime  Clock time of when jobs of this batch was submitted to
 *                        the streaming scheduler queue
 * @param processingStartTime Clock time of when the first job of this batch started processing
 * @param processingEndTime Clock time of when the last job of this batch finished processing
 * @param outputOperationInfos The output operations in this batch
 */
@DeveloperApi
case class BatchInfo(
    batchTime: Time,
    streamIdToInputInfo: Map[Int, StreamInputInfo],
    submissionTime: Long,
    processingStartTime: Option[Long],
    processingEndTime: Option[Long],
    outputOperationInfos: Map[Int, OutputOperationInfo]
  ) {

  /**
   * Time taken for the first job of this batch to start processing from the time this batch
   * was submitted to the streaming scheduler. Essentially, it is
   * `processingStartTime` - `submissionTime`.
   */
  def schedulingDelay: Option[Long] = processingStartTime.map(_ - submissionTime)

  /**
   * Time taken for the all jobs of this batch to finish processing from the time they started
   * processing. Essentially, it is `processingEndTime` - `processingStartTime`.
   */
  def processingDelay: Option[Long] = processingEndTime.zip(processingStartTime)
    .map(x => x._1 - x._2)

  /**
   * Time taken for all the jobs of this batch to finish processing from the time they
   * were submitted.  Essentially, it is `processingDelay` + `schedulingDelay`.
   */
  def totalDelay: Option[Long] = schedulingDelay.zip(processingDelay)
    .map(x => x._1 + x._2)

  /**
   * The number of recorders received by the receivers in this batch.
   */
  def numRecords: Long = streamIdToInputInfo.values.map(_.numRecords).sum

}
