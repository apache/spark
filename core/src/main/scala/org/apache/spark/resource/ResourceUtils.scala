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

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
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

private[spark] case class ResourceRequirement(resourceName: String, amount: Int)

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

/**
 * Resource allocation used in Standalone only, which tracks assignments with
 * worker/driver(client only) pid.
 */
private[spark]
case class StandaloneResourceAllocation(pid: Int, allocations: Seq[ResourceAllocation]) {
  // convert allocations to a resource information map
  def toResourceInformationMap: Map[String, ResourceInformation] = {
    allocations.map { allocation =>
      allocation.id.resourceName -> allocation.toResourceInformation
    }.toMap
  }
}

private[spark] object ResourceUtils extends Logging {
  // These directory/files are used to coordinate the resources between
  // the drivers/workers on the host in Spark Standalone.
  val SPARK_RESOURCES_COORDINATE_DIR = "spark-resources"
  val ALLOCATED_RESOURCES_FILE = "__allocated_resources__.json"
  val RESOURCES_LOCK_FILE = "__allocated_resources__.lock"

  // config suffixes
  val DISCOVERY_SCRIPT = "discoveryScript"
  val VENDOR = "vendor"
  // user facing configs use .amount to allow to extend in the future,
  // internally we currently only support addresses, so its just an integer count
  val AMOUNT = "amount"

  /**
   * Assign resources to workers/drivers from the same host to avoid address conflict.
   *
   * This function works in three steps. First, acquiring the lock on RESOURCES_LOCK_FILE
   * to achieve synchronization among workers and drivers. Second, getting all allocated
   * resources from ALLOCATED_RESOURCES_FILE and assigning isolated resources to the worker
   * or driver after differentiating available resources in discovered resources from
   * allocated resources. If available resources don't meet worker's or driver's requirement,
   * try to update allocated resources by excluding the resource allocation if its related
   * process has already terminated and do the assignment again. If still don't meet requirement,
   * exception should be thrown. Third, updating ALLOCATED_RESOURCES_FILE with new allocated
   * resources along with pid for the worker or driver. Then, return allocated resources
   * information after releasing the lock.
   *
   * @param conf SparkConf
   * @param componentName spark.driver / spark.worker
   * @param resources the resources found by worker/driver on the host
   * @param resourceRequirements the resource requirements asked by the worker/driver
   * @param pid the process id of worker/driver to acquire resources.
   * @return allocated resources for the worker/driver or throws exception if can't
   *         meet worker/driver's requirement
   */
  def acquireResources(
      conf: SparkConf,
      componentName: String,
      resources: Map[String, ResourceInformation],
      resourceRequirements: Seq[ResourceRequirement],
      pid: Int)
    : Map[String, ResourceInformation] = {
    if (resourceRequirements.isEmpty) {
      return Map.empty
    }
    val lock = acquireLock(conf)
    val resourcesFile = new File(getOrCreateResourcesDir(conf), ALLOCATED_RESOURCES_FILE)
    // all allocated resources in ALLOCATED_RESOURCES_FILE, can be updated if any allocations'
    // related processes detected to be terminated while checking pids below.
    var origAllocation = Seq.empty[StandaloneResourceAllocation]
    var allocated = {
      if (resourcesFile.exists()) {
        origAllocation = allocatedStandaloneResources(resourcesFile.getPath)
        val allocations = origAllocation.map { resource =>
          val resourceMap = {
            resource.allocations.map { allocation =>
              allocation.id.resourceName -> allocation.addresses.toArray
            }.toMap
          }
          resource.pid -> resourceMap
        }.toMap
        allocations
      } else {
        Map.empty[Int, Map[String, Array[String]]]
      }
    }

    // new allocated resources for worker or driver,
    // map from resource name to its allocated addresses.
    var newAssignments: Map[String, Array[String]] = null
    // Whether we've checked process status and we'll only do the check at most once.
    // Do the check iff the available resources can't meet the requirements at the first time.
    var checked = false
    // Whether we need to keep allocating for the worker/driver and we'll only go through
    // the loop at most twice.
    var keepAllocating = true
    while (keepAllocating) {
      keepAllocating = false
      // store the pid whose related allocated resources conflict with
      // discovered resources passed in.
      val pidsToCheck = mutable.Set[Int]()
      newAssignments = resourceRequirements.map { req =>
        val rName = req.resourceName
        val amount = req.amount
        // initially, we must have available.length >= amount as we've done pre-check previously
        var available = resources(rName).addresses
        // gets available resource addresses by excluding all
        // allocated resource addresses from discovered resources
        allocated.foreach { a =>
          val thePid = a._1
          val resourceMap = a._2
          val assigned = resourceMap.getOrElse(rName, Array.empty)
          val retained = available.diff(assigned)
          if (retained.length < available.length && !checked) {
            pidsToCheck += thePid
          }
          if (retained.length >= amount) {
            available = retained
          } else if (checked) {
            keepAllocating = false
            releaseLock(lock)
            throw new SparkException(s"No more resources available since they've already" +
              s" assigned to other workers/drivers.")
          } else {
            keepAllocating = true
          }
        }
        val assigned = {
          if (keepAllocating) { // can't meet the requirement
            // excludes the allocation whose related process has already been terminated.
            val (invalid, valid) = allocated.partition { a =>
              pidsToCheck(a._1) && !(Utils.isTesting || Utils.isProcessRunning(a._1))}
            allocated = valid
            origAllocation = origAllocation.filter(allocation => !invalid.contains(allocation.pid))
            checked = true
            // note that this is a meaningless return value, just to avoid creating any new object
            available
          } else {
            available.take(amount)
          }
        }
        rName -> assigned
      }.toMap

    }
    val newAllocation = {
      val allocations = newAssignments.map { case (rName, addresses) =>
        ResourceAllocation(ResourceID(componentName, rName), addresses)
      }.toSeq
      StandaloneResourceAllocation(pid, allocations)
    }
    writeResourceAllocationJson(componentName, origAllocation ++ Seq(newAllocation), resourcesFile)
    releaseLock(lock)
    newAllocation.toResourceInformationMap
  }

  /**
   * Free the indicated resources to make those resources be available for other
   * workers/drivers on the same host.
   * @param conf SparkConf
   * @param componentName spark.driver / spark.worker
   * @param toRelease the resources expected to release
   * @param pid the process id of worker/driver to release resources.
   */
  def releaseResources(
      conf: SparkConf,
      componentName: String,
      toRelease: Map[String, ResourceInformation],
      pid: Int)
    : Unit = {
    if (toRelease != null && toRelease.nonEmpty) {
      val lock = acquireLock(conf)
      val resourcesFile = new File(getOrCreateResourcesDir(conf), ALLOCATED_RESOURCES_FILE)
      if (resourcesFile.exists()) {
        val (target, others) =
          allocatedStandaloneResources(resourcesFile.getPath).partition(_.pid == pid)
        if (target.nonEmpty) {
          val rNameToAddresses = {
            target.head.allocations.map { allocation =>
              allocation.id.resourceName -> allocation.addresses
            }.toMap
          }
          val allocations = {
            rNameToAddresses.map { case (rName, addresses) =>
              val retained = addresses.diff(Option(toRelease.getOrElse(rName, null))
                .map(_.addresses).getOrElse(Array.empty))
              rName -> retained
            }
            .filter(_._2.nonEmpty)
            .map { case (rName, addresses) =>
              ResourceAllocation(ResourceID(componentName, rName), addresses)
            }.toSeq
          }
          if (allocations.nonEmpty) {
            val newAllocation = StandaloneResourceAllocation(pid, allocations)
            writeResourceAllocationJson(componentName, others ++ Seq(newAllocation), resourcesFile)
          } else {
            if (others.isEmpty) {
              if (!resourcesFile.delete()) {
                logWarning(s"Failed to delete $ALLOCATED_RESOURCES_FILE.")
              }
            } else {
              writeResourceAllocationJson(componentName, others, resourcesFile)
            }
          }
        }
      }
      releaseLock(lock)
    }
  }

  private def acquireLock(conf: SparkConf): FileLock = {
    val resourcesDir = getOrCreateResourcesDir(conf)
    val lockFile = new File(resourcesDir, RESOURCES_LOCK_FILE)
    val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel
    var keepTry = true
    var lock: FileLock = null
    while (keepTry) {
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
      lock.channel().close()
      logInfo(s"Released lock on $RESOURCES_LOCK_FILE.")
    } catch {
      case e: Exception =>
        logWarning(s"Error while releasing lock on $RESOURCES_LOCK_FILE.", e)
    }
  }

  private def getOrCreateResourcesDir(conf: SparkConf): File = {
    val coordinateDir = new File(conf.get(SPARK_RESOURCES_DIR).getOrElse {
      val sparkHome = if (Utils.isTesting) {
        assert(sys.props.contains("spark.test.home") ||
          sys.env.contains("SPARK_HOME"), "spark.test.home or SPARK_HOME is not set.")
        sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
      } else {
        sys.env.getOrElse("SPARK_HOME", ".")
      }
      sparkHome
    })
    val resourceDir = new File(coordinateDir, SPARK_RESOURCES_COORDINATE_DIR)
    if (!resourceDir.exists()) {
      Utils.createDirectory(resourceDir)
    }
    resourceDir
  }

  def writeResourceAllocationJson[T](
      componentName: String,
      allocations: Seq[T],
      jsonFile: File): Unit = {
    implicit val formats = DefaultFormats
    val allocationJson = Extraction.decompose(allocations)
    Files.write(jsonFile.toPath, compact(render(allocationJson)).getBytes())
  }

  /**
   * Save the allocated resources of driver(cluster only) or executor into a JSON formatted
   * resources file. Used in Standalone only.
   * @param componentName spark.driver / spark.executor
   * @param resources allocated resources for driver(cluster only) or executor
   * @param dir the target directory used to place the resources file
   * @return resources file
   */
  def prepareResourcesFile(
      componentName: String,
      resources: Map[String, ResourceInformation],
      dir: File): File = {
    val compShortName = componentName.substring(componentName.lastIndexOf(".") + 1)
    val tmpFile = Utils.tempFileWith(dir)
    val allocations = resources.map { case (rName, rInfo) =>
      ResourceAllocation(ResourceID(componentName, rName), rInfo.addresses)
    }.toSeq
    try {
      writeResourceAllocationJson(componentName, allocations, tmpFile)
    } catch {
      case NonFatal(e) =>
        val errMsg = s"Exception threw while preparing resource file for $compShortName"
        logError(errMsg, e)
        throw new SparkException(errMsg, e)
    }
    val resourcesFile = File.createTempFile(s"resource-${compShortName}-", ".json", dir)
    tmpFile.renameTo(resourcesFile)
    resourcesFile
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

  def parseResourceRequirements(sparkConf: SparkConf, componentName: String)
    : Seq[ResourceRequirement] = {
    parseAllResourceRequests(sparkConf, componentName).map { request =>
      ResourceRequirement(request.id.resourceName, request.amount)
    }
  }

  def resourcesMeetRequirements(
      resourcesFree: Map[String, Int],
      resourceRequirements: Seq[ResourceRequirement])
    : Boolean = {
    resourceRequirements.forall { req =>
      resourcesFree.getOrElse(req.resourceName, 0) >= req.amount
    }
  }

  private def withResourcesJson[T](resourcesFile: String)(extract: String => Seq[T]): Seq[T] = {
    val json = new String(Files.readAllBytes(Paths.get(resourcesFile)))
    try {
      extract(json)
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing resources file $resourcesFile", e)
    }
  }

  private def allocatedStandaloneResources(resourcesFile: String)
    : Seq[StandaloneResourceAllocation] = {
    withResourcesJson[StandaloneResourceAllocation](resourcesFile) { json =>
      implicit val formats = DefaultFormats
      parse(json).extract[Seq[StandaloneResourceAllocation]]
    }
  }

  def parseAllocatedFromJsonFile(resourcesFile: String): Seq[ResourceAllocation] = {
    withResourcesJson[ResourceAllocation](resourcesFile) { json =>
      implicit val formats = DefaultFormats
      parse(json).extract[Seq[ResourceAllocation]]
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
    resourceInfoMap
  }

  def logResourceInfo(componentName: String, resources: Map[String, ResourceInformation])
    : Unit = {
    logInfo("==============================================================")
    logInfo(s"Resources for $componentName:\n${resources.mkString("\n")}")
    logInfo("==============================================================")
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
