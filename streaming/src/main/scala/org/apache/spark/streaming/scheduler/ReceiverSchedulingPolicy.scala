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

import scala.collection.Map
import scala.collection.mutable

/**
 * A ReceiverScheduler trying to balance executors' load. Here is the approach to schedule executors
 * for a receiver.
 * <ol>
 *   <li>
 *     If preferredLocation is set, preferredLocation should be one of the candidate executors.
 *   </li>
 *   <li>
 *     Every executor will be assigned to a weight according to the receivers running or scheduling
 *     on it.
 *     <ul>
 *       <li>
 *         If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
 *       </li>
 *       <li>
 *         If a receiver is scheduled to an executor but has not yet run, it contributes
 *         `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.</li>
 *     </ul>
 *     At last, if there are more than 3 idle executors (weight = 0), returns all idle executors.
 *     Otherwise, we only return 3 best options according to the weights.
 *   </li>
 * </ol>
 *
 */
private[streaming] class ReceiverSchedulingPolicy {

  /**
   * Return a list of candidate executors to run the receiver. If the list is empty, the caller can
   * run this receiver in arbitrary executor.
   */
  def scheduleReceiver(
      receiverId: Int,
      preferredLocation: Option[String],
      receiverTrackingInfoMap: Map[Int, ReceiverTrackingInfo],
      executors: Seq[String]): Seq[String] = {
    if (executors.isEmpty) {
      return Seq.empty
    }

    // Always try to schedule to the preferred locations
    val locations = mutable.Set[String]()
    locations ++= preferredLocation

    val executorWeights = receiverTrackingInfoMap.values.flatMap { receiverTrackingInfo =>
      receiverTrackingInfo.state match {
        case ReceiverState.INACTIVE => Nil
        case ReceiverState.SCHEDULED =>
          val scheduledLocations = receiverTrackingInfo.scheduledLocations.get
          // The probability that a scheduled receiver will run in an executor is
          // 1.0 / scheduledLocations.size
          scheduledLocations.map(location => location -> (1.0 / scheduledLocations.size))
        case ReceiverState.ACTIVE => Seq(receiverTrackingInfo.runningLocation.get -> 1.0)
      }
    }.groupBy(_._1).mapValues(_.map(_._2).sum) // Sum weights for each executor

    val idleExecutors = (executors.toSet -- executorWeights.keys).toSeq
    if (idleExecutors.size >= 3) {
      // If there are more than 3 idle executors, return all of them
      locations ++= idleExecutors
    } else {
      // If there are less than 3 idle executors, return 3 best options
      locations ++= idleExecutors
      val sortedExecutors = executorWeights.toSeq.sortBy(_._2).map(_._1)
      locations ++= (idleExecutors ++ sortedExecutors).take(3)
    }
    locations.toSeq
  }
}
