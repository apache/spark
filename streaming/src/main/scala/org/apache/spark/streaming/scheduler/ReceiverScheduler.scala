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
import scala.util.Random

import org.apache.spark.streaming.scheduler.ReceiverState._

private[streaming] case class ReceiverTrackingInfo(
    receiverId: Int,
    state: ReceiverState,
    scheduledLocations: Option[Seq[String]],
    runningLocation: Option[String])

private[streaming] trait ReceiverScheduler {

  /**
   * Return a candidate executor list to run the receiver. If the list is empty, the caller can run
   * this receiver in arbitrary executor.
   */
  def scheduleReceiver(
      receiverId: Int,
      preferredLocation: Option[String],
      receiverTrackingInfoMap: Map[Int, ReceiverTrackingInfo],
      executors: Seq[String]): Seq[String]
}

/**
 * A ReceiverScheduler trying to balance executors' load.
 */
private[streaming] class LoadBalanceReceiverSchedulerImpl extends ReceiverScheduler {

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

    val executorWeights = receiverTrackingInfoMap.filter { case (id, _) =>
      // Ignore the receiver to be scheduled. It may be still running.
      id != receiverId
    }.values.flatMap { receiverTrackingInfo =>
      receiverTrackingInfo.state match {
        case ReceiverState.INACTIVE => Nil
        case ReceiverState.SCHEDULED =>
          val scheduledLocations = receiverTrackingInfo.scheduledLocations.get
          // The probability that a scheduled receiver will run in an executor is
          // 1.0 / scheduledLocations.size
          scheduledLocations.map(location => (location, 1.0 / scheduledLocations.size))
        case ReceiverState.ACTIVE => Seq(receiverTrackingInfo.runningLocation.get -> 1.0)
      }
    }.groupBy(_._1).mapValues(_.map(_._2).sum) // Sum weights for each executor

    val idleExecutors = (executors.toSet -- executorWeights.keys).toSeq
    if (idleExecutors.nonEmpty) {
      // If there are idle executors, randomly select one
      locations += idleExecutors(Random.nextInt(idleExecutors.size))
    } else {
      // Use the executor that runs the least receivers
      locations += executorWeights.minBy(_._2)._1
    }
    locations.toSeq
  }
}
