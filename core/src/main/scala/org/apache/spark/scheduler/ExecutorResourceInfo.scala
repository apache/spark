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

/**
 * Class to hold information about a type of Resource on an Executor. This information is managed
 * by SchedulerBackend, and TaskScheduler shall schedule tasks on idle Executors based on the
 * information.
 * @param name Resource name
 * @param addresses Resource addresses provided by the executor
 */
private[spark] class ExecutorResourceInfo(
    val name: String,
    private val addresses: Seq[String]) extends Serializable {

  private val addressesMap = new HashMap[String, Boolean]()
  addresses.foreach(addressesMap.put(_, true))

  /**
   * Sequence of currently available resource addresses.
   */
  def availableAddrs: Seq[String] = addressesMap.toList.filter(_._2 == true).map(_._1)

  /**
   * Sequence of currently assigned resource addresses.
   * Exposed for testing only.
   */
  private[scheduler] def assignedAddrs: Seq[String] =
    addressesMap.toList.filter(_._2 == false).map(_._1)

  /**
   * Acquire a sequence of resource addresses (to a launched task), these addresses must be
   * available. When the task finishes, it will return the acquired resource addresses.
   */
  def acquire(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      val isAvailable = addressesMap.getOrElse(address, false)
      if (isAvailable) {
        addressesMap(address) = false
      } else {
        throw new SparkException(s"Try to acquire address that is not available. $name address " +
          s"$address is not available.")
      }
    }
  }

  /**
   * Release a sequence of resource addresses, these addresses must have been assigned. Resource
   * addresses are released when a task has finished.
   */
  def release(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      val isAssigned = addressesMap.getOrElse(address, true)
      if (!isAssigned) {
        addressesMap(address) = true
      } else {
        throw new SparkException(s"Try to release address that is not assigned. $name address " +
          s"$address is not assigned.")
      }
    }
  }
}
