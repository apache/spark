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
import org.apache.spark.resource.ResourceAmountUtils.RESOURCE_TOTAL_AMOUNT

private[spark] object ResourceAmountUtils {
  /**
   * Using "double" to do the resource calculation may encounter a problem of precision loss. Eg
   *
   * scala> val taskAmount = 1.0 / 9
   * taskAmount: Double = 0.1111111111111111
   *
   * scala> var total = 1.0
   * total: Double = 1.0
   *
   * scala> for (i <- 1 to 9 ) {
   * |   if (total >= taskAmount) {
   * |           total -= taskAmount
   * |           println(s"assign $taskAmount for task $i, total left: $total")
   * |   } else {
   * |           println(s"ERROR Can't assign $taskAmount for task $i, total left: $total")
   * |   }
   * | }
   * assign 0.1111111111111111 for task 1, total left: 0.8888888888888888
   * assign 0.1111111111111111 for task 2, total left: 0.7777777777777777
   * assign 0.1111111111111111 for task 3, total left: 0.6666666666666665
   * assign 0.1111111111111111 for task 4, total left: 0.5555555555555554
   * assign 0.1111111111111111 for task 5, total left: 0.44444444444444425
   * assign 0.1111111111111111 for task 6, total left: 0.33333333333333315
   * assign 0.1111111111111111 for task 7, total left: 0.22222222222222204
   * assign 0.1111111111111111 for task 8, total left: 0.11111111111111094
   * ERROR Can't assign 0.1111111111111111 for task 9, total left: 0.11111111111111094
   *
   * So we multiply RESOURCE_TOTAL_AMOUNT to convert the double to long to avoid this limitation.
   * Double can display up to 16 decimal places, so we set the factor to
   * 10, 000, 000, 000, 000, 000L.
   */
  final val RESOURCE_TOTAL_AMOUNT: Long = 10000000000000000L
}

/**
 * Trait used to help executor/worker allocate resources.
 * Please note that this is intended to be used in a single thread.
 */
private[spark] trait ResourceAllocator {

  protected def resourceName: String
  protected def resourceAddresses: Seq[String]

  /**
   * Map from an address to its availability default to 1.0 (we multiply RESOURCE_TOTAL_AMOUNT
   * to avoid precision error), a value > 0 means the address is available, while value of
   * 0 means the address is fully assigned.
   */
  private lazy val addressAvailabilityMap = {
    mutable.HashMap(resourceAddresses.map(address => address -> RESOURCE_TOTAL_AMOUNT): _*)
  }

  /**
   * Get the amounts of resources that have been multiplied by RESOURCE_TOTAL_AMOUNT.
   * @return the resources amounts
   */
  def resourcesAmounts: Map[String, Long] = addressAvailabilityMap.toMap

  /**
   * Sequence of currently available resource addresses which are not fully assigned.
   */
  def availableAddrs: Seq[String] = addressAvailabilityMap
    .filter(addresses => addresses._2 > 0).keys.toSeq.sorted

  /**
   * Sequence of currently assigned resource addresses.
   */
  private[spark] def assignedAddrs: Seq[String] = addressAvailabilityMap
    .filter(addresses => addresses._2 < RESOURCE_TOTAL_AMOUNT).keys.toSeq.sorted

  /**
   * Acquire a sequence of resource addresses (to a launched task), these addresses must be
   * available. When the task finishes, it will return the acquired resource addresses.
   * Throw an Exception if an address is not available or doesn't exist.
   */
  def acquire(addressesAmounts: Map[String, Long]): Unit = {
    addressesAmounts.foreach { case (address, amount) =>
      val prevAmount = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
          s"address $address doesn't exist."))

      val left = addressAvailabilityMap(address) - amount

      if (left < 0) {
        throw new SparkException(s"Try to acquire $resourceName address $address " +
          s"amount: ${amount.toDouble / RESOURCE_TOTAL_AMOUNT}, but only " +
          s"${prevAmount.toDouble / RESOURCE_TOTAL_AMOUNT} left.")
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
  def release (addressesAmounts: Map[String, Long]): Unit = {
    addressesAmounts.foreach { case (address, amount) =>
      val prevAmount = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to release an address that doesn't exist. $resourceName " +
          s"address $address doesn't exist."))

      val total = prevAmount + amount

      if (total > RESOURCE_TOTAL_AMOUNT) {
        throw new SparkException(s"Try to release $resourceName address $address " +
          s"amount: ${amount.toDouble / RESOURCE_TOTAL_AMOUNT}. But the total amount: " +
          s"${total.toDouble / RESOURCE_TOTAL_AMOUNT} " +
          s"after release should be <= 1")
      } else {
        addressAvailabilityMap(address) = total
      }
    }
  }
}
