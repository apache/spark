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
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.resource.ResourceAmountUtils.RESOURCE_TOTAL_AMOUNT

/**
 * Class to hold information about a series of resources belonging to an executor.
 * A resource could be a GPU, FPGA, etc. And it is used as a temporary
 * class to calculate the resources amounts when offering resources to
 * the tasks in the [[TaskSchedulerImpl]]
 *
 * One example is GPUs, where the addresses would be the indices of the GPUs
 *
 * @param resources The executor available resources and amount. eg,
 *                  Map("gpu" -> mutable.Map("0" -> 0.2, "1" -> 1.0),
 *                  "fpga" -> mutable.Map("a" -> 0.3, "b" -> 0.9)
 *                  )
 */
private[spark] class ExecutorResourcesAmounts(
    private val resources: Map[String, Map[String, Double]]) extends Serializable {

  /**
   * Multiply the RESOURCE_TOTAL_AMOUNT to avoid using double directly.
   * and convert the addressesAmounts to be mutable.HashMap
   */
  private val internalResources: Map[String, HashMap[String, Long]] = {
    resources.map { case (rName, addressAmounts) =>
      rName -> HashMap(addressAmounts.map { case (address, amount) =>
        address -> (amount * RESOURCE_TOTAL_AMOUNT).toLong
      }.toSeq: _*)
    }
  }

  /**
   * The total address count of each resource. Eg,
   * Map("gpu" -> Map("0" -> 0.5, "1" -> 0.5, "2" -> 0.5),
   *     "fpga" -> Map("a" -> 0.5, "b" -> 0.5))
   * the resourceAmount will be Map("gpu" -> 3, "fpga" -> 2)
   */
  lazy val resourceAmount: Map[String, Int] = internalResources.map { case (rName, addressMap) =>
    rName -> addressMap.size
  }

  /**
   * For testing purpose. convert internal resources back to the "fraction" resources.
   */
  private[spark] def availableResources: Map[String, Map[String, Double]] = {
    internalResources.map { case (rName, addressMap) =>
      rName -> addressMap.map { case (address, amount) =>
        address -> amount.toDouble / RESOURCE_TOTAL_AMOUNT
      }.toMap
    }
  }

  /**
   * Acquire the resource and update the resource
   * @param assignedResource the assigned resource information
   */
  def acquire(assignedResource: Map[String, Map[String, Double]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = internalResources.getOrElse(rName,
        throw new SparkException(s"Try to acquire an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prevInternalTotalAmount = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to acquire an address that doesn't exist. $rName " +
            s"address $address doesn't exist."))

        val internalTaskAmount = (amount * RESOURCE_TOTAL_AMOUNT).toLong
        val internalLeft = prevInternalTotalAmount - internalTaskAmount
        val realLeft = internalLeft.toDouble / RESOURCE_TOTAL_AMOUNT
        if (realLeft < 0) {
          throw new SparkException(s"The total amount ${realLeft} " +
            s"after acquiring $rName address $address should be >= 0")
        }
        internalResources(rName)(address) = internalLeft
      }
    }
  }

  /**
   * Release the assigned resources to the resource pool
   * @param assignedResource resource to be released
   */
  def release(assignedResource: Map[String, Map[String, Double]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = internalResources.getOrElse(rName,
        throw new SparkException(s"Try to release an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prevInternalTotalAmount = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to release an address that is not assigned. $rName " +
            s"address $address is not assigned."))
        val internalTaskAmount = (amount * RESOURCE_TOTAL_AMOUNT).toLong
        val internalTotal = prevInternalTotalAmount + internalTaskAmount
        if (internalTotal > RESOURCE_TOTAL_AMOUNT) {
          throw new SparkException(s"The total amount " +
            s"${internalTotal.toDouble / RESOURCE_TOTAL_AMOUNT} " +
            s"after releasing $rName address $address should be <= 1.0")
        }
        internalResources(rName)(address) = internalTotal
      }
    }
  }

  /**
   * Try to assign the address according to the task requirement.
   * Please note that this function will not update the values.
   *
   * @param taskSetProf assign resources based on which resource profile
   * @return the resource
   */
  def assignResources(taskSetProf: ResourceProfile):
      Option[(Map[String, ResourceInformation], Map[String, Map[String, Double]])] = {

    // only look at the resource other than cpus
    val tsResources = taskSetProf.getCustomTaskResources()
    if (tsResources.isEmpty) {
      return Some(Map.empty, Map.empty)
    }

    val localTaskReqAssign = HashMap[String, ResourceInformation]()
    val allocatedAddresses = HashMap[String, Map[String, Double]]()

    // we go through all resources here so that we can make sure they match and also get what the
    // assignments are for the next task
    for ((rName, taskReqs) <- tsResources) {
      // TaskResourceRequest checks the task amount should be in (0, 1] or a whole number
      var taskAmount = taskReqs.amount

      internalResources.get(rName) match {
        case Some(addressesAmountMap) =>
          val allocatedAddressesMap = HashMap[String, Double]()

          // always sort the addresses
          val addresses = addressesAmountMap.keys.toSeq.sorted

          // task.amount is a whole number
          if (taskAmount >= 1.0) {
            for (address <- addresses if taskAmount > 0) {
              // The address is still a whole resource
              if (addressesAmountMap(address) == RESOURCE_TOTAL_AMOUNT) {
                taskAmount -= 1.0
                // Assign the full resource of the address
                allocatedAddressesMap(address) = 1.0
              }
            }
          } else if (taskAmount > 0.0) { // 0 < task.amount < 1.0
            val internalTaskAmount = taskAmount * RESOURCE_TOTAL_AMOUNT
            for (address <- addresses if taskAmount > 0) {
              if (addressesAmountMap(address) >= internalTaskAmount) {
                // Assign the part of the address.
                allocatedAddressesMap(address) = taskAmount
                taskAmount = 0
              }
            }
          }

          if (taskAmount == 0 && allocatedAddressesMap.size > 0) {
            localTaskReqAssign.put(rName, new ResourceInformation(rName,
              allocatedAddressesMap.keys.toArray))
            allocatedAddresses.put(rName, allocatedAddressesMap.toMap)
          } else return None

        case None => return None
      }
    }
    Some(localTaskReqAssign.toMap, allocatedAddresses.toMap)
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
