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

package org.apache.spark.deploy.yarn

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.util.RackResolver

import org.apache.spark.SparkConf

private[yarn] trait ContainerPlacementStrategy {
  type Locality = Array[String]

  /**
   * Calculate each container's node locality and rack locality
   * @param numContainer number of containers to calculate
   *
   * @return node localities and rack localities, each locality is an array of string,
   *         the length of localities is the same as number of containers
   */
  def localityOfRequestedContainers(numContainer: Int, numLocalityAwarePendingTasks: Int,
      preferredLocalityToCount: Map[String, Int]): (Array[Locality], Array[Locality])
}

/**
 * This strategy calculates the preferred localities by considering the node ratio of pending
 * tasks, number of required cores/containers and current existed containers. For example,
 * if we have 20 tasks which require (host1, host2, host3) and 10 tasks which require (host1,
 * host2, host4), besides each container has 2 cores and cpus per task is 1,
 * so the required container number is 15, and host ratio is (host1: 30, host2: 30, host3: 20,
 * host4: 10).
 *
 * 1. If requested container number (18) is more than the required container number (15):
 *
 * requests for 5 containers with nodes: (host1, host2, host3, host4)
 * requests for 5 containers with nodes: (host1, host2, host3)
 * requests for 5 containers with nodes: (host1, host2)
 * requests for 3 containers with no locality preferences.
 *
 * The placement ratio is 3 : 3 : 2 : 1, and set the additional containers with no locality
 * preferences.
 *
 * 2. If requested container number (10) is less than or equal to the required container number
 * (15):
 *
 * requests for 4 containers with nodes: (host1, host2, host3, host4)
 * requests for 3 containers with nodes: (host1, host2, host3)
 * requests for 3 containers with nodes: (host1, host2)
 *
 * The placement ratio is 10 : 10 : 7 : 4, close to expected ratio (3 : 3 : 2 : 1)
 *
 * 3. If containers are existed but no matching localities, follow the method of 1 and 2.
 *
 * 4. If containers are existed and some localities are matched. For example if we have 1
 * containers on each node (host1: 1, host2: 1: host3: 1, host4: 1), and the expected containers
 * on each node would be (host1: 5, host2: 5, host3: 4, host4: 2),
 * so the newly requested containers on each node would be updated to (host1: 4, host2: 4,
 * host3: 3, host4: 1), 12 containers by total.
 *
 *   4.1 If requested container number (18) is more than newly required containers (12). Follow
 *   method 1 with updated ratio 4 : 4 : 3 : 1.
 *
 *   4.2 If request container number (10) is more than newly required containers (12). Follow
 *   method 2 with updated ratio 4 : 4 : 3 : 1.
 *
 * 5. If containers are existed and existing localities can fully cover the requested localities.
 * For example if we have 5 containers on each node (host1: 5, host2: 5, host3: 5, host4: 5),
 * which could cover the current requested localities. This algorithm will allocate all the
 * requested containers with no localities.
 */
private[yarn] class LocalityPreferredContainerPlacementStrategy(
    val sparkConf: SparkConf,
    val yarnConf: Configuration,
    val yarnAllocator: YarnAllocator) extends ContainerPlacementStrategy {

  // Number of CPUs per task
  private val CPUS_PER_TASK = sparkConf.getInt("spark.task.cpus", 1)

  // Get the required cores of locality aware task
  private def localityAwareTaskCores(localityAwarePendingTasks: Int): Int = {
    localityAwarePendingTasks * CPUS_PER_TASK
  }

  // Get the expected number of locality aware containers
  private def expectedLocalityAwareContainerNum(localityAwarePendingTasks: Int): Int = {
    (localityAwareTaskCores(localityAwarePendingTasks) +
      yarnAllocator.resource.getVirtualCores - 1) /
        yarnAllocator.resource.getVirtualCores
  }

  // Update the expected locality distribution by considering the existing allocated container
  // host distributions.
  private def updateExpectedLocalityToCounts(localityAwarePendingTasks: Int,
      preferredLocalityToCounts: Map[String, Int]): Map[String, Int] = {
    val totalPreferredLocalities = preferredLocalityToCounts.values.sum
    preferredLocalityToCounts.map { case (host, count) =>
      val expectedCount =
        count.toDouble * expectedLocalityAwareContainerNum(localityAwarePendingTasks) /
          totalPreferredLocalities
      val existedCount = yarnAllocator.allocatedHostToContainersMap.get(host)
        .map(_.size)
        .getOrElse(0)

      if (expectedCount > existedCount) {
        // Get the actual container number if existing container can not fully satisfy the
        // expected number of container
        (host, (expectedCount - existedCount).ceil.toInt)
      } else {
        // If the current existed container number can fully satisfy the expected number of
        // containers, set the required containers to be 0
        (host, 0)
      }
    }
  }

  def localityOfRequestedContainers(numContainer: Int, numLocalityAwarePendingTasks: Int,
      preferredLocalityToCounts: Map[String, Int]): (Array[Locality], Array[Locality]) = {
    val updatedLocalityToCounts =
      updateExpectedLocalityToCounts(numLocalityAwarePendingTasks, preferredLocalityToCounts)
    val updatedLocalityAwareContainerNum = updatedLocalityToCounts.values.sum

    // The number of containers to allocate, divided into two groups, one with preferred locality,
    // and the other without locality preference.
    var requiredLocalityFreeContainerNum: Int = 0
    var requiredLocalityAwareContainerNum: Int = 0

    if (updatedLocalityAwareContainerNum == 0) {
      // If the current existed containers can satisfy all the locality preferred tasks,
      // allocate the new container with no locality preference
      requiredLocalityFreeContainerNum = numContainer
    } else {
      if (updatedLocalityAwareContainerNum >= numContainer) {
        // If newly requested containers cannot satisfy the locality preferred tasks,
        // allocate all the new container with locality preference
        requiredLocalityAwareContainerNum = numContainer
      } else {
        // If part of newly requested can satisfy the locality preferred tasks, allocate part of
        // the containers with locality preference, and another part with no locality preference
        requiredLocalityAwareContainerNum = updatedLocalityAwareContainerNum
        requiredLocalityFreeContainerNum = numContainer - updatedLocalityAwareContainerNum
      }
    }

    val preferredNodeLocalities = ArrayBuffer[Locality]()
    if (requiredLocalityFreeContainerNum > 0) {
      for (i <- 0 until requiredLocalityFreeContainerNum) {
        preferredNodeLocalities += null.asInstanceOf[Locality]
      }
    }

    if (requiredLocalityAwareContainerNum > 0) {
      val largestRatio = updatedLocalityToCounts.values.max
      // Round the ratio of preferred locality to the number of locality required container
      // number, which is used for locality preferred host calculating.
      var preferredLocalityRatio = updatedLocalityToCounts.mapValues { ratio =>
        val adjustedRatio = ratio.toDouble * requiredLocalityAwareContainerNum / largestRatio
        adjustedRatio.ceil.toInt
      }

      for (i <- 0 until requiredLocalityAwareContainerNum) {
        // Only filter out the ratio which is larger than 0, which means the current host can
        // still be allocated with new container request.
        val hosts = preferredLocalityRatio.filter(_._2 > 0).keys.toArray
        preferredNodeLocalities += hosts
        // Each time when the host is used, subtract 1. When the current ratio is 0,
        // which means all the required ratio is satisfied, this host will not be allocated again.
        preferredLocalityRatio = preferredLocalityRatio.mapValues(_ - 1)
      }
    }

    val preferredRackLocalities = preferredNodeLocalities.map { hosts =>
      if (hosts == null) {
        null.asInstanceOf[Locality]
      } else {
        val racks = hosts.map { h =>
          RackResolver.resolve(yarnConf, h).getNetworkLocation
        }.toSet
        racks.toArray
      }
    }

    (preferredNodeLocalities.toArray, preferredRackLocalities.toArray)
  }
}
