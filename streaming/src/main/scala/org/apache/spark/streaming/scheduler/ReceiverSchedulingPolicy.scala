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

import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.receiver.Receiver

/**
 * A class that tries to schedule receivers with evenly distributed. There are two phases for
 * scheduling receivers.
 *
 *  - The first phase is global scheduling when ReceiverTracker is starting and we need to schedule
 *    all receivers at the same time. ReceiverTracker will call `scheduleReceivers` at this phase.
 *    It will try to schedule receivers such that they are evenly distributed. ReceiverTracker
 *    should update its `receiverTrackingInfoMap` according to the results of `scheduleReceivers`.
 *    `ReceiverTrackingInfo.scheduledLocations` for each receiver should be set to a location list
 *    that contains the scheduled locations. Then when a receiver is starting, it will send a
 *    register request and `ReceiverTracker.registerReceiver` will be called. In
 *    `ReceiverTracker.registerReceiver`, if a receiver's scheduled locations is set, it should
 *    check if the location of this receiver is one of the scheduled locations, if not, the register
 *    will be rejected.
 *  - The second phase is local scheduling when a receiver is restarting. There are two cases of
 *    receiver restarting:
 *    - If a receiver is restarting because it's rejected due to the real location and the scheduled
 *      locations mismatching, in other words, it fails to start in one of the locations that
 *      `scheduleReceivers` suggested, `ReceiverTracker` should firstly choose the executors that
 *      are still alive in the list of scheduled locations, then use them to launch the receiver
 *      job.
 *    - If a receiver is restarting without a scheduled locations list, or the executors in the list
 *      are dead, `ReceiverTracker` should call `rescheduleReceiver`. If so, `ReceiverTracker`
 *      should not set `ReceiverTrackingInfo.scheduledLocations` for this receiver, instead, it
 *      should clear it. Then when this receiver is registering, we can know this is a local
 *      scheduling, and `ReceiverTrackingInfo` should call `rescheduleReceiver` again to check if
 *      the launching location is matching.
 *
 * In conclusion, we should make a global schedule, try to achieve that exactly as long as possible,
 * otherwise do local scheduling.
 */
private[streaming] class ReceiverSchedulingPolicy {

  /**
   * Try our best to schedule receivers with evenly distributed. However, if the
   * `preferredLocation`s of receivers are not even, we may not be able to schedule them evenly
   * because we have to respect them.
   *
   * Here is the approach to schedule executors:
   * <ol>
   *   <li>First, schedule all the receivers with preferred locations (hosts), evenly among the
   *       executors running on those host.</li>
   *   <li>Then, schedule all other receivers evenly among all the executors such that overall
   *       distribution over all the receivers is even.</li>
   * </ol>
   *
   * This method is called when we start to launch receivers at the first time.
   *
   * @return a map for receivers and their scheduled locations
   */
  def scheduleReceivers(
      receivers: Seq[Receiver[_]],
      executors: Seq[ExecutorCacheTaskLocation]): Map[Int, Seq[TaskLocation]] = {
    if (receivers.isEmpty) {
      return Map.empty
    }

    if (executors.isEmpty) {
      return receivers.map(_.streamId -> Seq.empty).toMap
    }

    val hostToExecutors = executors.groupBy(_.host)
    val scheduledLocations = Array.fill(receivers.length)(new mutable.ArrayBuffer[TaskLocation])
    val numReceiversOnExecutor = mutable.HashMap[ExecutorCacheTaskLocation, Int]()
    // Set the initial value to 0
    executors.foreach(e => numReceiversOnExecutor(e) = 0)

    // Firstly, we need to respect "preferredLocation". So if a receiver has "preferredLocation",
    // we need to make sure the "preferredLocation" is in the candidate scheduled executor list.
    for (i <- receivers.indices) {
      // Note: preferredLocation is host but executors are host_executorId
      receivers(i).preferredLocation.foreach { host =>
        hostToExecutors.get(host) match {
          case Some(executorsOnHost) =>
            // preferredLocation is a known host. Select an executor that has the least receivers in
            // this host
            val leastScheduledExecutor =
              executorsOnHost.minBy(executor => numReceiversOnExecutor(executor))
            scheduledLocations(i) += leastScheduledExecutor
            numReceiversOnExecutor(leastScheduledExecutor) =
              numReceiversOnExecutor(leastScheduledExecutor) + 1
          case None =>
            // preferredLocation is an unknown host.
            // Note: There are two cases:
            // 1. This executor is not up. But it may be up later.
            // 2. This executor is dead, or it's not a host in the cluster.
            // Currently, simply add host to the scheduled executors.

            // Note: host could be `HDFSCacheTaskLocation`, so use `TaskLocation.apply` to handle
            // this case
            scheduledLocations(i) += TaskLocation(host)
        }
      }
    }

    // For those receivers that don't have preferredLocation, make sure we assign at least one
    // executor to them.
    for (scheduledLocationsForOneReceiver <- scheduledLocations.filter(_.isEmpty)) {
      // Select the executor that has the least receivers
      val (leastScheduledExecutor, numReceivers) = numReceiversOnExecutor.minBy(_._2)
      scheduledLocationsForOneReceiver += leastScheduledExecutor
      numReceiversOnExecutor(leastScheduledExecutor) = numReceivers + 1
    }

    // Assign idle executors to receivers that have less executors
    val idleExecutors = numReceiversOnExecutor.filter(_._2 == 0).keys
    for (executor <- idleExecutors) {
      // Assign an idle executor to the receiver that has least candidate executors.
      val leastScheduledExecutors = scheduledLocations.minBy(_.size)
      leastScheduledExecutors += executor
    }

    receivers.map(_.streamId).zip(scheduledLocations.map(_.toSeq)).toMap
  }

  /**
   * Return a list of candidate locations to run the receiver. If the list is empty, the caller can
   * run this receiver in arbitrary executor.
   *
   * This method tries to balance executors' load. Here is the approach to schedule executors
   * for a receiver.
   * <ol>
   *   <li>
   *     If preferredLocation is set, preferredLocation should be one of the candidate locations.
   *   </li>
   *   <li>
   *     Every executor will be assigned to a weight according to the receivers running or
   *     scheduling on it.
   *     <ul>
   *       <li>
   *         If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
   *       </li>
   *       <li>
   *         If a receiver is scheduled to an executor but has not yet run, it contributes
   *         `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.</li>
   *     </ul>
   *     At last, if there are any idle executors (weight = 0), returns all idle executors.
   *     Otherwise, returns the executors that have the minimum weight.
   *   </li>
   * </ol>
   *
   * This method is called when a receiver is registering with ReceiverTracker or is restarting.
   */
  def rescheduleReceiver(
      receiverId: Int,
      preferredLocation: Option[String],
      receiverTrackingInfoMap: Map[Int, ReceiverTrackingInfo],
      executors: Seq[ExecutorCacheTaskLocation]): Seq[TaskLocation] = {
    if (executors.isEmpty) {
      return Seq.empty
    }

    // Always try to schedule to the preferred locations
    val scheduledLocations = mutable.Set[TaskLocation]()
    // Note: preferredLocation could be `HDFSCacheTaskLocation`, so use `TaskLocation.apply` to
    // handle this case
    scheduledLocations ++= preferredLocation.map(TaskLocation(_))

    val executorWeights: Map[ExecutorCacheTaskLocation, Double] = {
      receiverTrackingInfoMap.values.flatMap(convertReceiverTrackingInfoToExecutorWeights)
        .groupBy(_._1).mapValues(_.map(_._2).sum).toMap // Sum weights for each executor
    }

    val idleExecutors = executors.toSet -- executorWeights.keys
    if (idleExecutors.nonEmpty) {
      scheduledLocations ++= idleExecutors
    } else {
      // There is no idle executor. So select all executors that have the minimum weight.
      val sortedExecutors = executorWeights.toSeq.sortBy(_._2)
      if (sortedExecutors.nonEmpty) {
        val minWeight = sortedExecutors(0)._2
        scheduledLocations ++= sortedExecutors.takeWhile(_._2 == minWeight).map(_._1)
      } else {
        // This should not happen since "executors" is not empty
      }
    }
    scheduledLocations.toSeq
  }

  /**
   * This method tries to convert a receiver tracking info to executor weights. Every executor will
   * be assigned to a weight according to the receivers running or scheduling on it:
   *
   * - If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
   * - If a receiver is scheduled to an executor but has not yet run, it contributes
   * `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.
   */
  private def convertReceiverTrackingInfoToExecutorWeights(
      receiverTrackingInfo: ReceiverTrackingInfo): Seq[(ExecutorCacheTaskLocation, Double)] = {
    receiverTrackingInfo.state match {
      case ReceiverState.INACTIVE => Nil
      case ReceiverState.SCHEDULED =>
        val scheduledLocations = receiverTrackingInfo.scheduledLocations.get
        // The probability that a scheduled receiver will run in an executor is
        // 1.0 / scheduledLocations.size
        scheduledLocations.filter(_.isInstanceOf[ExecutorCacheTaskLocation]).map { location =>
          location.asInstanceOf[ExecutorCacheTaskLocation] -> (1.0 / scheduledLocations.size)
        }
      case ReceiverState.ACTIVE => Seq(receiverTrackingInfo.runningExecutor.get -> 1.0)
    }
  }
}
