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

package org.apache.spark.scheduler

import scala.collection.mutable.HashMap

import org.apache.spark.SparkException
import org.apache.spark.resource.{ResourceAmountUtils, ResourceProfile}
import org.apache.spark.resource.ResourceAmountUtils.ONE_ENTIRE_RESOURCE

/**
 * Class to hold information about a series of resources belonging to an executor.
 * A resource could be a GPU, FPGA, etc. And it is used as a temporary
 * class to calculate the resources amounts when offering resources to
 * the tasks in the [[TaskSchedulerImpl]]
 *
 * One example is GPUs, where the addresses would be the indices of the GPUs
 *
 * @param resources The executor available resources and amount. eg,
 *                  Map("gpu" -> Map("0" -> ResourceAmountUtils.toInternalResource(0.2),
 *                                   "1" -> ResourceAmountUtils.toInternalResource(1.0)),
 *                  "fpga" -> Map("a" -> ResourceAmountUtils.toInternalResource(0.3),
 *                                "b" -> ResourceAmountUtils.toInternalResource(0.9))
 *                  )
 */
private[spark] class ExecutorResourcesAmounts(
    private val resources: Map[String, Map[String, Long]]) extends Serializable {

  /**
   * convert the resources to be mutable HashMap
   */
  private val internalResources: Map[String, HashMap[String, Long]] = {
    resources.map { case (rName, addressAmounts) =>
      rName -> HashMap(addressAmounts.toSeq: _*)
    }
  }

  /**
   * The total address count of each resource. Eg,
   * Map("gpu" -> Map("0" -> ResourceAmountUtils.toInternalResource(0.5),
   *                  "1" -> ResourceAmountUtils.toInternalResource(0.5),
   *                  "2" -> ResourceAmountUtils.toInternalResource(0.5)),
   *     "fpga" -> Map("a" -> ResourceAmountUtils.toInternalResource(0.5),
   *                   "b" -> ResourceAmountUtils.toInternalResource(0.5)))
   * the resourceAmount will be Map("gpu" -> 3, "fpga" -> 2)
   */
  lazy val resourceAddressAmount: Map[String, Int] = internalResources.map {
    case (rName, addressMap) => rName -> addressMap.size
  }

  /**
   * For testing purpose. convert internal resources back to the "fraction" resources.
   */
  private[spark] def availableResources: Map[String, Map[String, Double]] = {
    internalResources.map { case (rName, addressMap) =>
      rName -> addressMap.map { case (address, amount) =>
        address -> ResourceAmountUtils.toFractionalResource(amount)
      }.toMap
    }
  }

  /**
   * Acquire the resource.
   * @param assignedResource the assigned resource information
   */
  def acquire(assignedResource: Map[String, Map[String, Long]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = internalResources.getOrElse(rName,
        throw new SparkException(s"Try to acquire an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prevInternalTotalAmount = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to acquire an address that doesn't exist. $rName " +
            s"address $address doesn't exist."))

        val left = prevInternalTotalAmount - amount
        if (left < 0) {
          throw new SparkException(s"The total amount " +
            s"${ResourceAmountUtils.toFractionalResource(left)} " +
            s"after acquiring $rName address $address should be >= 0")
        }
        internalResources(rName)(address) = left
      }
    }
  }

  /**
   * Release the assigned resources to the resource pool
   * @param assignedResource resource to be released
   */
  def release(assignedResource: Map[String, Map[String, Long]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = internalResources.getOrElse(rName,
        throw new SparkException(s"Try to release an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prevInternalTotalAmount = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to release an address that is not assigned. $rName " +
            s"address $address is not assigned."))
        val total = prevInternalTotalAmount + amount
        if (total > ONE_ENTIRE_RESOURCE) {
          throw new SparkException(s"The total amount " +
            s"${ResourceAmountUtils.toFractionalResource(total)} " +
            s"after releasing $rName address $address should be <= 1.0")
        }
        internalResources(rName)(address) = total
      }
    }
  }

  /**
   * Try to assign the addresses according to the task requirement. This function always goes
   * through the available resources starting from the "small" address. If the resources amount
   * of the address is matching the task requirement, we will assign this address to this task.
   * Eg, assuming the available resources are {"gpu" -&gt; {"0"-&gt; 0.7, "1" -&gt; 1.0}) and the
   * task requirement is 0.5, this function will return Some(Map("gpu" -&gt; {"0" -&gt; 0.5})).
   *
   * TODO: as we consistently allocate addresses beginning from the "small" address, it can
   * potentially result in an undesired consequence where a portion of the resource is being wasted.
   * Eg, assuming the available resources are {"gpu" -&gt; {"0"-&gt; 1.0, "1" -&gt; 0.5}) and the
   * task amount requirement is 0.5, this function will return
   * Some(Map("gpu" -&gt; {"0" -&gt; 0.5})), and the left available resource will be
   * {"gpu" -&gt; {"0"-&gt; 0.5, "1" -&gt; 0.5}) which can't assign to the task that
   * requires &gt; 0.5 any more.
   *
   * @param taskSetProf assign resources based on which resource profile
   * @return the optional assigned resources amounts. returns None if any
   *         of the task requests for resources aren't met.
   */
  def assignAddressesCustomResources(taskSetProf: ResourceProfile):
      Option[Map[String, Map[String, Long]]] = {
    // only look at the resource other than cpus
    val tsResources = taskSetProf.getCustomTaskResources()
    if (tsResources.isEmpty) {
      return Some(Map.empty)
    }

    val allocatedAddresses = HashMap[String, Map[String, Long]]()

    // Go through all resources here so that we can make sure they match and also get what the
    // assignments are for the next task
    for ((rName, taskReqs) <- tsResources) {
      // TaskResourceRequest checks the task amount should be in (0, 1] or a whole number
      var taskAmount = taskReqs.amount

      internalResources.get(rName) match {
        case Some(addressesAmountMap) =>
          val allocatedAddressesMap = HashMap[String, Long]()

          // Always sort the addresses
          val addresses = addressesAmountMap.keys.toSeq.sorted

          // task.amount is a whole number
          if (taskAmount >= 1.0) {
            for (address <- addresses if taskAmount > 0) {
              // The address is still a whole resource
              if (ResourceAmountUtils.isOneEntireResource(addressesAmountMap(address))) {
                taskAmount -= 1.0
                // Assign the full resource of the address
                allocatedAddressesMap(address) = ONE_ENTIRE_RESOURCE
              }
            }
          } else if (taskAmount > 0.0) { // 0 < task.amount < 1.0
            val internalTaskAmount = ResourceAmountUtils.toInternalResource(taskAmount)
            for (address <- addresses if taskAmount > 0) {
              if (addressesAmountMap(address) >= internalTaskAmount) {
                // Assign the part of the address.
                allocatedAddressesMap(address) = internalTaskAmount
                taskAmount = 0
              }
            }
          }

          if (taskAmount == 0 && allocatedAddressesMap.size > 0) {
            allocatedAddresses.put(rName, allocatedAddressesMap.toMap)
          } else {
            return None
          }

        case None => return None
      }
    }
    Some(allocatedAddresses.toMap)
  }

}

private[spark] object ExecutorResourcesAmounts {

  /**
   * Create an empty ExecutorResourcesAmounts
   */
  def empty: ExecutorResourcesAmounts = new ExecutorResourcesAmounts(Map.empty)

  /**
   * Converts executor infos to ExecutorResourcesAmounts
   */
  def apply(executorInfos: Map[String, ExecutorResourceInfo]): ExecutorResourcesAmounts = {
    new ExecutorResourcesAmounts(
      executorInfos.map { case (rName, rInfo) => rName -> rInfo.resourcesAmounts }
    )
  }

}
