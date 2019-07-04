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

import java.io.{File, RandomAccessFile}
import java.nio.channels.{FileLock, OverlappingFileLockException}
import java.nio.file.{Files, Paths}

import scala.util.Random
import scala.util.control.NonFatal

import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Worker.SPARK_WORKER_PREFIX
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Resource identifier.
 * @param componentName spark.driver / spark.executor / spark.task
 * @param resourceName  gpu, fpga, etc
 */
private[spark] case class ResourceID(componentName: String, resourceName: String) {
  def confPrefix: String = s"$componentName.resource.$resourceName." // with ending dot
  def amountConf: String = s"$confPrefix${ResourceUtils.AMOUNT}"
  def discoveryScriptConf: String = s"$confPrefix${ResourceUtils.DISCOVERY_SCRIPT}"
  def vendorConf: String = s"$confPrefix${ResourceUtils.VENDOR}"
}

private[spark] case class ResourceRequest(
    id: ResourceID,
    amount: Int,
    discoveryScript: Option[String],
    vendor: Option[String])

private[spark] case class TaskResourceRequirement(resourceName: String, amount: Int)

/**
 * Case class representing allocated resource addresses for a specific resource.
 * Cluster manager uses the JSON serialization of this case class to pass allocated resource info to
 * driver and executors. See the ``--resourcesFile`` option there.
 */
private[spark] case class ResourceAllocation(id: ResourceID, addresses: Seq[String]) {
  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(id.resourceName, addresses.toArray)
  }
}

private[spark] object ResourceUtils extends Logging {
  // These directory/files are used to coordinate the resources between
  // the drivers/workers on the host in Spark Standalone.
  val SPARK_RESOURCES_DIRECTORY = "spark-resources"
  val ALLOCATED_RESOURCES_FILE = "__allocated_resources__.json"
  val RESOURCES_LOCK_FILE = "__allocated_resources__.lock"

  // config suffixes
  val DISCOVERY_SCRIPT = "discoveryScript"
  val VENDOR = "vendor"
  // user facing configs use .amount to allow to extend in the future,
  // internally we currently only support addresses, so its just an integer count
  val AMOUNT = "amount"

  /**
   * Assign resources to workers from the same host to avoid address conflict.
   * @param resources the resources found by worker on the host
   * @param resourceRequests map resource name to the request amount by the worker
   * @return allocated resources for the worker/driver or throws exception if can't
   *         meet worker/driver's requirement
   */
  def acquireResources(
      resources: Map[String, ResourceInformation],
      resourceRequests: Map[String, Int])
  : Map[String, ResourceInformation] = {
    if (resourceRequests.isEmpty) {
      return Map.empty
    }
    val lock = acquireLock()
    val resourcesFile = new File(getOrCreateResourcesDir(), ALLOCATED_RESOURCES_FILE)
    val allocated = {
      if (resourcesFile.exists()) {
        val allocated = parseAllocatedFromJsonFile(resourcesFile.getPath)
        allocated.map { allocation => allocation.id.resourceName -> allocation.addresses.toArray}
      } else {
        Map.empty
      }
    }.toMap

    val newAssigned = {
      resourceRequests.map{ case (rName, amount) =>
        val assigned = allocated.getOrElse(rName, Array.empty)
        val available = resources(rName).addresses.diff(assigned)
        val newAssigned = {
          if (available.length >= amount) {
            available.take(amount)
          } else {
            throw new SparkException(s"No more resources available since they've already" +
              s" assigned to other workers/drivers.")
          }
        }
        rName -> new ResourceInformation(rName, newAssigned)
      }
    }

    val newAllocated = {
      val newResources = newAssigned.keys.partition(allocated.keySet.contains)._2.toSet
      val allResources = newResources ++ allocated.keys
      allResources.map { rName =>
        val oldAddrs = allocated.getOrElse(rName, Array.empty)
        val newAddrs = Option(newAssigned.getOrElse(rName, null)).map(_.addresses)
          .getOrElse(Array.empty)
        rName -> new ResourceInformation(rName, Array.concat(oldAddrs, newAddrs))
      }.toMap
    }
    writeResourceAllocationJson(SPARK_WORKER_PREFIX, newAllocated, resourcesFile)
    releaseLock(lock)
    newAssigned
  }

  /**
   * Free the indicated resources to make those resources be available for other
   * workers on the same host.
   * @param toRelease the resources expected to release
   */
  def releaseResources(toRelease: Map[String, ResourceInformation]): Unit = {
    if (toRelease.nonEmpty) {
      val lock = acquireLock()
      val resourcesFile = new File(getOrCreateResourcesDir(), ALLOCATED_RESOURCES_FILE)
      if (resourcesFile.exists()) {
        val allocated = {
          val allocated = parseAllocatedFromJsonFile(resourcesFile.getPath)
          allocated.map { allocation => allocation.id.resourceName -> allocation.addresses.toArray}
        }.toMap
        val newAllocated = {
          allocated.map { case (rName, addresses) =>
            val retained = addresses.diff(Option(toRelease.getOrElse(rName, null))
              .map(_.addresses).getOrElse(Array.empty))
            rName -> new ResourceInformation(rName, retained)
          }.filter(_._2.addresses.nonEmpty)
        }
        if (newAllocated.nonEmpty) {
          writeResourceAllocationJson(SPARK_WORKER_PREFIX, newAllocated, resourcesFile)
        } else {
          if (!resourcesFile.delete()) {
            logWarning(s"Failed to delete $ALLOCATED_RESOURCES_FILE.")
          }
        }
      }
      releaseLock(lock)
    }
  }

  private def acquireLock(): FileLock = {
    val resourcesDir = getOrCreateResourcesDir()
    val lockFile = new File(resourcesDir, RESOURCES_LOCK_FILE)
    val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel
    var keepTry = true
    var lock: FileLock = null
    while(keepTry) {
      try {
        lock = lockFileChannel.lock()
        logInfo(s"Acquired lock on $RESOURCES_LOCK_FILE.")
        keepTry = false
      } catch {
        case e: OverlappingFileLockException =>
          // This exception throws when we're in LocalSparkCluster mode. FileLock is designed
          // to be used across JVMs, but our LocalSparkCluster is designed to launch multiple
          // workers in the same JVM. As a result, when an worker in LocalSparkCluster try to
          // acquire the lock on `resources.lock` which already locked by other worker, we'll
          // hit this exception. So, we should manually control it.
          keepTry = true
          // there may be multiple workers race for the lock,
          // so, sleep for a random time to avoid possible conflict
          val duration = Random.nextInt(1000) + 1000
          Thread.sleep(duration)
      }
    }
    assert(lock != null, s"Acquired null lock on $RESOURCES_LOCK_FILE.")
    lock
  }

  private def releaseLock(lock: FileLock): Unit = {
    try {
      lock.release()
      logInfo(s"Released lock on $RESOURCES_LOCK_FILE.")
    } catch {
      case e: Exception =>
        logWarning(s"Error while releasing lock on $RESOURCES_LOCK_FILE.", e)
    }
  }

  private def getOrCreateResourcesDir(): File = {
    val sparkHome = if (Utils.isTesting) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.getOrElse("SPARK_HOME", "."))
    }
    val resourceDir = new File(sparkHome, SPARK_RESOURCES_DIRECTORY)
    if (!resourceDir.exists()) {
      Utils.createDirectory(resourceDir)
    }
    resourceDir
  }

  def writeResourceAllocationJson(
      componentName: String,
      resources: Map[String, ResourceInformation],
      jsonFile: File): Unit = {
    implicit val formats = DefaultFormats
    val allocation = resources.map { case (rName, rInfo) =>
      ResourceAllocation(ResourceID(componentName, rName), rInfo.addresses)
    }.toSeq
    val allocationJson = Extraction.decompose(allocation)
    Files.write(jsonFile.toPath, compact(render(allocationJson)).getBytes())
  }

  def prepareResourceFile(
      componentName: String,
      resources: Map[String, ResourceInformation],
      dir: File): File = {
    val compShortName = componentName.substring(componentName.lastIndexOf(".") + 1)
    val tmpFile = Utils.tempFileWith(dir)
    try {
      writeResourceAllocationJson(componentName, resources, tmpFile)
    } catch {
      case NonFatal(e) =>
        val errMsg = s"Exception threw while preparing resource file for $compShortName"
        logError(errMsg, e)
        throw new SparkException(errMsg, e)
    }
    val resourceFile = File.createTempFile(s"resource-${compShortName}-", ".json", dir)
    tmpFile.renameTo(resourceFile)
    resourceFile
  }

  def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest = {
    val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
    val amount = settings.getOrElse(AMOUNT,
      throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}")
    ).toInt
    val discoveryScript = settings.get(DISCOVERY_SCRIPT)
    val vendor = settings.get(VENDOR)
    ResourceRequest(resourceId, amount, discoveryScript, vendor)
  }

  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.resource.").map { case (key, _) =>
      key.substring(0, key.indexOf('.'))
    }.toSet.toSeq.map(name => ResourceID(componentName, name))
  }

  def parseAllResourceRequests(
      sparkConf: SparkConf,
      componentName: String): Seq[ResourceRequest] = {
    listResourceIds(sparkConf, componentName).map { id =>
      parseResourceRequest(sparkConf, id)
    }
  }

  def parseTaskResourceRequirements(sparkConf: SparkConf): Seq[TaskResourceRequirement] = {
    parseAllResourceRequests(sparkConf, SPARK_TASK_PREFIX).map { request =>
      TaskResourceRequirement(request.id.resourceName, request.amount)
    }
  }

  def parseResourceRequirements(sparkConf: SparkConf, componentName: String): Map[String, Int] = {
    parseAllResourceRequests(sparkConf, componentName).map { request =>
      request.id.resourceName -> request.amount
    }.toMap
  }

  def parseAllocatedFromJsonFile(resourcesFile: String): Seq[ResourceAllocation] = {
    implicit val formats = DefaultFormats
    val json = new String(Files.readAllBytes(Paths.get(resourcesFile)))
    try {
      parse(json).extract[Seq[ResourceAllocation]]
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing resources file $resourcesFile", e)
    }
  }

  private def parseAllocatedOrDiscoverResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Seq[ResourceAllocation] = {
    val allocated = resourcesFileOpt.toSeq.flatMap(parseAllocatedFromJsonFile)
      .filter(_.id.componentName == componentName)
    val otherResourceIds = listResourceIds(sparkConf, componentName).diff(allocated.map(_.id))
    allocated ++ otherResourceIds.map { id =>
      val request = parseResourceRequest(sparkConf, id)
      ResourceAllocation(id, discoverResource(request).addresses)
    }
  }

  private def assertResourceAllocationMeetsRequest(
      allocation: ResourceAllocation,
      request: ResourceRequest): Unit = {
    require(allocation.id == request.id && allocation.addresses.size >= request.amount,
      s"Resource: ${allocation.id.resourceName}, with addresses: " +
      s"${allocation.addresses.mkString(",")} " +
      s"is less than what the user requested: ${request.amount})")
  }

  private def assertAllResourceAllocationsMeetRequests(
      allocations: Seq[ResourceAllocation],
      requests: Seq[ResourceRequest]): Unit = {
    val allocated = allocations.map(x => x.id -> x).toMap
    requests.foreach(r => assertResourceAllocationMeetsRequest(allocated(r.id), r))
  }

  /**
   * Gets all allocated resource information for the input component from input resources file and
   * discover the remaining via discovery scripts.
   * It also verifies the resource allocation meets required amount for each resource.
   * @return a map from resource name to resource info
   */
  def getOrDiscoverAllResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    val requests = parseAllResourceRequests(sparkConf, componentName)
    val allocations = parseAllocatedOrDiscoverResources(sparkConf, componentName, resourcesFileOpt)
    assertAllResourceAllocationsMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
    logInfo("==============================================================")
    logInfo(s"Resources for $componentName:\n${resourceInfoMap.mkString("\n")}")
    logInfo("==============================================================")
    resourceInfoMap
  }

  // visible for test
  private[spark] def discoverResource(resourceRequest: ResourceRequest): ResourceInformation = {
    val resourceName = resourceRequest.id.resourceName
    val script = resourceRequest.discoveryScript
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        val output = executeAndGetOutput(Seq(script.get), new File("."))
        ResourceInformation.parseJson(output)
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
          "doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use resource: $resourceName, but " +
        "didn't specify a discovery script!")
    }
    if (!result.name.equals(resourceName)) {
      throw new SparkException(s"Error running the resource discovery script ${script.get}: " +
        s"script returned resource name ${result.name} and we were expecting $resourceName.")
    }
    result
  }

  // known types of resources
  final val GPU: String = "gpu"
  final val FPGA: String = "fpga"
}
