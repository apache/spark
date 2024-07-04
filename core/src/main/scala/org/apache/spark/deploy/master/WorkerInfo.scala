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

package org.apache.spark.deploy.master

import scala.collection.mutable

import org.apache.spark.resource.{ResourceAllocator, ResourceInformation, ResourceRequirement}
import org.apache.spark.resource.ResourceAmountUtils.ONE_ENTIRE_RESOURCE
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

private[spark] case class WorkerResourceInfo(name: String, addresses: Seq[String])
  extends Serializable with ResourceAllocator {

  override protected def resourceName = this.name
  override protected def resourceAddresses = this.addresses

  /**
   * Acquire the resources.
   * @param amount How many addresses are requesting.
   * @return ResourceInformation
   */
  def acquire(amount: Int): ResourceInformation = {
    // Any available address from availableAddrs must be a whole resource
    // since worker needs to do full resources to the executors.
    val addresses = availableAddrs.take(amount)
    assert(addresses.length == amount)

    acquire(addresses.map(addr => addr -> ONE_ENTIRE_RESOURCE).toMap)
    new ResourceInformation(resourceName, addresses.toArray)
  }
}

private[spark] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val cores: Int,
    val memory: Int,
    val endpoint: RpcEndpointRef,
    val webUiAddress: String,
    val resources: Map[String, WorkerResourceInfo])
  extends Serializable {

  Utils.checkHost(host)
  assert (port > 0)

  @transient var executors: mutable.HashMap[String, ExecutorDesc] = _ // executorId => info
  @transient var drivers: mutable.HashMap[String, DriverInfo] = _ // driverId => info
  @transient var state: WorkerState.Value = _
  @transient var coresUsed: Int = _
  @transient var memoryUsed: Int = _

  @transient var lastHeartbeat: Long = _

  init()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed
  def resourcesAmountFree: Map[String, Int] = {
    resources.map { case (rName, rInfo) =>
      rName -> rInfo.availableAddrs.length
    }
  }

  def resourcesInfo: Map[String, ResourceInformation] = {
    resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.addresses.toArray)
    }
  }

  def resourcesInfoFree: Map[String, ResourceInformation] = {
    resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.availableAddrs.toArray)
    }
  }

  def resourcesInfoUsed: Map[String, ResourceInformation] = {
    resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.assignedAddrs.toArray)
    }
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init(): Unit = {
    executors = new mutable.HashMap
    drivers = new mutable.HashMap
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def addExecutor(exec: ExecutorDesc): Unit = {
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
  }

  def removeExecutor(exec: ExecutorDesc): Unit = {
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
      releaseResources(exec.resources)
    }
  }

  def hasExecutor(app: ApplicationInfo): Boolean = {
    executors.values.exists(_.application == app)
  }

  def addDriver(driver: DriverInfo): Unit = {
    drivers(driver.id) = driver
    memoryUsed += driver.desc.mem
    coresUsed += driver.desc.cores
  }

  def removeDriver(driver: DriverInfo): Unit = {
    drivers -= driver.id
    memoryUsed -= driver.desc.mem
    coresUsed -= driver.desc.cores
    releaseResources(driver.resources)
  }

  def setState(state: WorkerState.Value): Unit = {
    this.state = state
  }

  def isAlive(): Boolean = this.state == WorkerState.ALIVE

  /**
   * acquire specified amount resources for driver/executor from the worker
   * @param resourceReqs the resources requirement from driver/executor
   */
  def acquireResources(resourceReqs: Seq[ResourceRequirement])
    : Map[String, ResourceInformation] = {
    resourceReqs.map { req =>
      val rName = req.resourceName
      val amount = req.amount
      rName -> resources(rName).acquire(amount)
    }.toMap
  }

  /**
   * used during master recovery
   */
  def recoverResources(expected: Map[String, ResourceInformation]): Unit = {
    expected.foreach { case (rName, rInfo) =>
      resources(rName).acquire(rInfo.addresses.map(addr => addr -> ONE_ENTIRE_RESOURCE).toMap)
    }
  }

  /**
   * release resources to worker from the driver/executor
   * @param allocated the resources which allocated to driver/executor previously
   */
  private def releaseResources(allocated: Map[String, ResourceInformation]): Unit = {
    allocated.foreach { case (rName, rInfo) =>
      resources(rName).release(rInfo.addresses.map(addrs => addrs -> ONE_ENTIRE_RESOURCE).toMap)
    }
  }
}
