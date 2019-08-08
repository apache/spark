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

import org.apache.spark.SparkException
import org.apache.spark.util.collection.OpenHashMap

/**
 * Class to hold information about a type of Resource on an Executor. This information is managed
 * by SchedulerBackend, and TaskScheduler shall schedule tasks on idle Executors based on the
 * information.
 * Please note that this class is intended to be used in a single thread.
 * @param name Resource name
 * @param addresses Resource addresses provided by the executor
 */
private[spark] class ExecutorResourceInfo(
    val name: String,
    addresses: Seq[String]) extends Serializable {

  /**
   * Map from an address to its availability, the value `true` means the address is available,
   * while value `false` means the address is assigned.
   * TODO Use [[OpenHashMap]] instead to gain better performance.
   */
  private val addressAvailabilityMap = mutable.HashMap(addresses.map(_ -> true): _*)

  /**
   * Sequence of currently available resource addresses.
   */
  def availableAddrs: Seq[String] = addressAvailabilityMap.flatMap { case (addr, available) =>
    if (available) Some(addr) else None
  }.toSeq

  /**
   * Sequence of currently assigned resource addresses.
   * Exposed for testing only.
   */
  private[scheduler] def assignedAddrs: Seq[String] = addressAvailabilityMap
    .flatMap { case (addr, available) =>
      if (!available) Some(addr) else None
    }.toSeq

  /**
   * Acquire a sequence of resource addresses (to a launched task), these addresses must be
   * available. When the task finishes, it will return the acquired resource addresses.
   * Throw an Exception if an address is not available or doesn't exist.
   */
  def acquire(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      if (!addressAvailabilityMap.contains(address)) {
        throw new SparkException(s"Try to acquire an address that doesn't exist. $name address " +
          s"$address doesn't exist.")
      }
      val isAvailable = addressAvailabilityMap(address)
      if (isAvailable) {
        addressAvailabilityMap(address) = false
      } else {
        throw new SparkException(s"Try to acquire an address that is not available. $name " +
          s"address $address is not available.")
      }
    }
  }

  /**
   * Release a sequence of resource addresses, these addresses must have been assigned. Resource
   * addresses are released when a task has finished.
   * Throw an Exception if an address is not assigned or doesn't exist.
   */
  def release(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      if (!addressAvailabilityMap.contains(address)) {
        throw new SparkException(s"Try to release an address that doesn't exist. $name address " +
          s"$address doesn't exist.")
      }
      val isAvailable = addressAvailabilityMap(address)
      if (!isAvailable) {
        addressAvailabilityMap(address) = true
      } else {
        throw new SparkException(s"Try to release an address that is not assigned. $name " +
          s"address $address is not assigned.")
      }
    }
  }
}
