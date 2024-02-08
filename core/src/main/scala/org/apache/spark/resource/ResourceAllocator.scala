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

package org.apache.spark.resource

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.resource.ResourceAmountUtils.{ONE_ENTIRE_RESOURCE, ZERO_RESOURCE}

private[spark] object ResourceAmountUtils {

  final val SCALE = 14

  final val ONE_ENTIRE_RESOURCE: BigDecimal = BigDecimal(1.0).setScale(SCALE)

  final val ZERO_RESOURCE: BigDecimal = BigDecimal(0).setScale(SCALE)
}

/**
 * Trait used to help executor/worker allocate resources.
 * Please note that this is intended to be used in a single thread.
 */
private[spark] trait ResourceAllocator {

  protected def resourceName: String
  protected def resourceAddresses: Seq[String]

  /**
   * Map from an address to its availability default to 1.0 (we multiply ONE_ENTIRE_RESOURCE
   * to avoid precision error), a value &gt; 0 means the address is available, while value of
   * 0 means the address is fully assigned.
   */
  private lazy val addressAvailabilityMap = {
    mutable.HashMap(resourceAddresses.map(address => address -> ONE_ENTIRE_RESOURCE): _*)
  }

  /**
   * Get the amounts of resources that have been multiplied by ONE_ENTIRE_RESOURCE.
   * @return the resources amounts
   */
  def resourcesAmounts: Map[String, BigDecimal] = addressAvailabilityMap.toMap

  /**
   * Sequence of currently available resource addresses which are not fully assigned.
   */
  def availableAddrs: Seq[String] = addressAvailabilityMap
    .filter(addresses => addresses._2 > ZERO_RESOURCE).keys.toSeq.sorted

  /**
   * Sequence of currently assigned resource addresses.
   */
  private[spark] def assignedAddrs: Seq[String] = addressAvailabilityMap
    .filter(addresses => addresses._2 < ONE_ENTIRE_RESOURCE).keys.toSeq.sorted

  /**
   * Acquire a sequence of resource addresses (to a launched task), these addresses must be
   * available. When the task finishes, it will return the acquired resource addresses.
   * Throw an Exception if an address is not available or doesn't exist.
   */
  def acquire(addressesAmounts: Map[String, BigDecimal]): Unit = {
    addressesAmounts.foreach { case (address, amount) =>
      val prev = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
          s"address $address doesn't exist."))

      val left = prev - amount

      if (left < ZERO_RESOURCE) {
        throw new SparkException(s"Try to acquire $resourceName address $address " +
          s"amount: ${amount}, but only ${prev} left.")
      } else {
        addressAvailabilityMap(address) = left
      }
    }
  }

  /**
   * Release a sequence of resource addresses, these addresses must have been assigned. Resource
   * addresses are released when a task has finished.
   * Throw an Exception if an address is not assigned or doesn't exist.
   */
  def release(addressesAmounts: Map[String, BigDecimal]): Unit = {
    addressesAmounts.foreach { case (address, amount) =>
      val prev = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to release an address that doesn't exist. $resourceName " +
          s"address $address doesn't exist."))

      val total = prev + amount

      if (total > ONE_ENTIRE_RESOURCE) {
        throw new SparkException(s"Try to release $resourceName address $address " +
          s"amount: ${amount}. But the total amount: ${total} after release should be <= 1")
      } else {
        addressAvailabilityMap(address) = total
      }
    }
  }
}
