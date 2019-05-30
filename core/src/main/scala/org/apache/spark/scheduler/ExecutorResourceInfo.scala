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
    private val addresses: Seq[String]) extends Serializable {

  // Addresses of resources that has not been assigned or reserved.
  // Exposed for testing only.
  private[scheduler] val idleAddresses: ArrayBuffer[String] = addresses.to[ArrayBuffer]

  // Addresses of resources that has been assigned to running tasks.
  // Exposed for testing only.
  private[scheduler] val allocatedAddresses: ArrayBuffer[String] = ArrayBuffer.empty

  // Addresses of resources that has been reserved but not assigned out yet.
  // Exposed for testing only.
  private[scheduler] val reservedAddresses: ArrayBuffer[String] = ArrayBuffer.empty

  def getName(): String = name

  def getNumOfIdleResources(): Int = idleAddresses.size

  // Reserve given number of resource addresses, these addresses can be assigned to a future
  // launched task.
  def acquireAddresses(num: Int): Seq[String] = {
    assert(num <= idleAddresses.size, "Required to take more addresses than available. " +
      s"Required $num $name addresses, but only ${idleAddresses.size} available.")
    val addrs = idleAddresses.take(num)
    idleAddresses --= addrs
    reservedAddresses ++= addrs
    addrs
  }

  // Give back a sequence of resource addresses, these addresses must have been reserved or
  // assigned. Resource addresses are released when a task has finished, or the task launch is
  // skipped.
  def releaseAddresses(addrs: Array[String]): Unit = {
    addrs.foreach { address =>
      assert((allocatedAddresses ++ reservedAddresses).contains(address), "Try to release " +
        s"address that is not reserved or allocated. $name address $address is not allocated.")
      idleAddresses += address
      if (allocatedAddresses.contains(address)) {
        allocatedAddresses -= address
      } else if (reservedAddresses.contains(address)) {
        reservedAddresses -= address
      }
    }
  }

  // Assign a sequence of resource addresses (to a launched task), these addresses must have been
  // reserved.
  def assignAddresses(addrs: Array[String]): Unit = {
    addrs.foreach { address =>
      assert(reservedAddresses.contains(address), "Try to assign address that is not reserved. " +
        s"$name address $address is not reserved.")
      allocatedAddresses += address
      reservedAddresses -= address
    }
  }
}
