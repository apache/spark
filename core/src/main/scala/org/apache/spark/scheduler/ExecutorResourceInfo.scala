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

import scala.collection.mutable.ArrayBuffer

/**
 * Class to hold information about a type of Resource on an Executor. This information is managed
 * by SchedulerBackend, and TaskScheduler shall schedule tasks on idle Executors based on the
 * information.
 */
private[spark] class ExecutorResourceInfo(
    private val name: String,
    private[scheduler] val addresses: ArrayBuffer[String]) extends Serializable {

  // Addresses of resource that has not been acquired.
  // Exposed for testing only.
  private[scheduler] var idleAddresses: ArrayBuffer[String] = addresses.clone()

  // Addresses of resource that has been assigned to running tasks.
  // Exposed for testing only.
  private[scheduler] val allocatedAddresses: ArrayBuffer[String] = ArrayBuffer.empty

  def getName(): String = name

  // Number of resource addresses that can be acquired.
  def getNumOfIdleResources(): Int = idleAddresses.size

  // Reserve given number of resource addresses, these addresses can be assigned to a future
  // launched task.
  def acquireAddresses(num: Int): Seq[String] = {
    assert(num <= idleAddresses.size, "Required to take more addresses than available. " +
      s"Required $num $name addresses, but only ${idleAddresses.size} available.")
    val addrs = idleAddresses.take(num)
    idleAddresses --= addrs
    addrs
  }

  // Give back a sequence of resource addresses, these addresses must have been assigned. Resource
  // addresses are released when a task has finished.
  def releaseAddresses(addrs: Array[String]): Unit = {
    addrs.foreach { address =>
      assert(allocatedAddresses.contains(address), "Try to release address that is not " +
        s"allocated. $name address $address is not allocated.")
      addresses += address
      idleAddresses += address
      allocatedAddresses -= address
    }
  }

  // Assign a sequence of resource addresses (to a launched task), these addresses must be
  // available.
  def assignAddresses(addrs: Array[String]): Unit = {
    addrs.foreach { address =>
      assert(addresses.contains(address), "Try to assign address that is not available. " +
        s"$name address $address is not available.")
      allocatedAddresses += address
      addresses -= address
    }
  }

  // Reset the resource addresses that has not been acquired.
  def resetIdleAddresses(): Unit = {
    idleAddresses = addresses.clone()
  }
}
