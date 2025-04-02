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
 * Class having information on output operations.
 * @param batchTime Time of the batch
 * @param id Id of this output operation. Different output operations have different ids in a batch.
 * @param name The name of this output operation.
 * @param description The description of this output operation.
 * @param startTime Clock time of when the output operation started processing
 * @param endTime Clock time of when the output operation started processing
 * @param failureReason Failure reason if this output operation fails
 */
@DeveloperApi
case class OutputOperationInfo(
    batchTime: Time,
    id: Int,
    name: String,
    description: String,
    startTime: Option[Long],
    endTime: Option[Long],
    failureReason: Option[String]) {

  /**
   * Return the duration of this output operation.
   */
  def duration: Option[Long] = for (s <- startTime; e <- endTime) yield e - s
}
