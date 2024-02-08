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

import scala.collection.mutable
import scala.math.BigDecimal.RoundingMode

import org.apache.spark.SparkException
import org.apache.spark.resource.{ResourceAmountUtils, ResourceProfile}
import org.apache.spark.resource.ResourceAmountUtils.{ONE_ENTIRE_RESOURCE, ZERO_RESOURCE}

/**
 * Class to hold information about a series of resources belonging to an executor.
 * A resource could be a GPU, FPGA, etc. And it is used as a temporary
 * class to calculate the resources amounts when offering resources to
 * the tasks in the [[TaskSchedulerImpl]]
 *
 * One example is GPUs, where the addresses would be the indices of the GPUs
 *
 * @param resources The executor available resources and amount. eg,
 *                  Map("gpu" -> Map("0" -> BigDecimal(0.2),
 *                                   "1" -> BigDecimal(1.0)),
 *                  "fpga" -> Map("a" -> BigDecimal(0.3),
 *                                "b" -> BigDecimal(0.9))
 *                  )
 */
private[spark] class ExecutorResourcesAmounts(
    private val resources: Map[String, Map[String, BigDecimal]]) extends Serializable {

  /**
   * The current available resources.
   */
  private[spark] val availableResources: Map[String, mutable.HashMap[String, BigDecimal]] = {
    resources.map { case (rName, addressAmounts) =>
      rName -> mutable.HashMap(addressAmounts.toSeq: _*)
    }
  }

  /**
   * The total address count of each resource. Eg, assume availableResources is
   * Map("gpu" -> Map("0" -> BigDecimal(0.5),
   *                  "1" -> BigDecimal(0.5),
   *                  "2" -> BigDecimal(0.5)),
   *     "fpga" -> Map("a" -> BigDecimal(0.5),
   *                   "b" -> BigDecimal(0.5))),
   * the resourceAmount will be Map("gpu" -> 3, "fpga" -> 2)
   */
  private[spark] lazy val resourceAddressAmount: Map[String, Int] = availableResources.map {
    case (rName, addressMap) => rName -> addressMap.size
  }

  /**
   * Acquire the resource.
   * @param assignedResource the assigned resource information
   */
  def acquire(assignedResource: Map[String, Map[String, BigDecimal]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = availableResources.getOrElse(rName,
        throw new SparkException(s"Try to acquire an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prev = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to acquire an address that doesn't exist. $rName " +
            s"address $address doesn't exist."))

        val left = prev - amount
        if (left < ZERO_RESOURCE) {
          throw new SparkException(s"The total amount " +
            s"${left.toDouble} after acquiring $rName address $address should be >= 0")
        }
        availableResources(rName)(address) = left
      }
    }
  }

  /**
   * Release the assigned resources to the resource pool
   * @param assignedResource resource to be released
   */
  def release(assignedResource: Map[String, Map[String, BigDecimal]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = availableResources.getOrElse(rName,
        throw new SparkException(s"Try to release an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prev = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to release an address that is not assigned. $rName " +
            s"address $address is not assigned."))
        val total = prev + amount
        if (total > ONE_ENTIRE_RESOURCE) {
          throw new SparkException(s"The total amount " +
            s"${total.toDouble} after releasing $rName address $address should be <= 1.0")
        }
        availableResources(rName)(address) = total
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
      Option[Map[String, Map[String, BigDecimal]]] = {
    // only look at the resource other than cpus
    val tsResources = taskSetProf.getCustomTaskResources()
    if (tsResources.isEmpty) {
      return Some(Map.empty)
    }

    val allocatedAddresses = mutable.HashMap[String, Map[String, BigDecimal]]()

    // Go through all resources here so that we can make sure they match and also get what the
    // assignments are for the next task
    for ((rName, taskReqs) <- tsResources) {
      // taskReqs.amount must be in (0, 1] or a whole number
      var taskAmount = BigDecimal(taskReqs.amount)
        .setScale(ResourceAmountUtils.SCALE, RoundingMode.DOWN)

      availableResources.get(rName) match {
        case Some(addressesAmountMap) =>
          val allocatedAddressesMap = mutable.HashMap[String, BigDecimal]()

          // Always sort the addresses
          val addresses = addressesAmountMap.keys.toSeq.sorted

          // task.amount is a whole number
          if (taskAmount >= ONE_ENTIRE_RESOURCE) {
            for (address <- addresses if taskAmount > ZERO_RESOURCE) {
              // The address is still a whole resource
              if (addressesAmountMap(address) == ONE_ENTIRE_RESOURCE) {
                taskAmount -= ONE_ENTIRE_RESOURCE
                // Assign the full resource of the address
                allocatedAddressesMap(address) = ONE_ENTIRE_RESOURCE
              }
            }
          } else if (taskAmount > ZERO_RESOURCE) { // 0 < task.amount < 1.0
            for (address <- addresses if taskAmount > ZERO_RESOURCE) {
              if (addressesAmountMap(address) >= taskAmount) {
                // Assign the part of the address.
                allocatedAddressesMap(address) = taskAmount
                taskAmount = ZERO_RESOURCE
              }
            }
          }

          if (taskAmount == ZERO_RESOURCE && allocatedAddressesMap.nonEmpty) {
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
