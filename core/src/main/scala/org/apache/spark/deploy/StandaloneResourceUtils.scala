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

package org.apache.spark.deploy

import java.io.{File, RandomAccessFile}
import java.nio.channels.{FileLock, OverlappingFileLockException}
import java.nio.file.Files

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{SPARK_RESOURCES_COORDINATE, SPARK_RESOURCES_DIR}
import org.apache.spark.resource.{ResourceAllocation, ResourceID, ResourceInformation, ResourceRequirement}
import org.apache.spark.resource.ResourceUtils.{parseResourceRequirements, withResourcesJson}
import org.apache.spark.util.Utils

private[spark] object StandaloneResourceUtils extends Logging {
  // These directory/files are used to coordinate the resources between
  // the drivers/workers on the host in Spark Standalone.
  val SPARK_RESOURCES_COORDINATE_DIR = "spark-resources"
  val ALLOCATED_RESOURCES_FILE = "__allocated_resources__.json"
  val RESOURCES_LOCK_FILE = "__allocated_resources__.lock"

  /**
   * A mutable resource information which provides more efficient modification on addresses.
   */
  private[spark] case class MutableResourceInfo(name: String, addresses: mutable.HashSet[String]) {

    def + (other: MutableResourceInfo): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.add)
      this
    }

    def + (other: ResourceInformation): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.add)
      this
    }

    def - (other: ResourceInformation): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.remove)
      this
    }

    def toResourceInformation: ResourceInformation = {
      new ResourceInformation(name, addresses.toArray)
    }
  }

  /**
   * Resource allocation used in Standalone only, which tracks assignments with
   * worker/driver(client only) pid.
   */
  case class StandaloneResourceAllocation(pid: Int, allocations: Seq[ResourceAllocation]) {
    // convert allocations to a resource information map
    def toResourceInformationMap: Map[String, ResourceInformation] = {
      allocations.map { allocation =>
        allocation.id.resourceName -> allocation.toResourceInformation
      }.toMap
    }
  }

  /**
   * Assigns (if coordinate needed) resources to workers/drivers from the same host to avoid
   * address conflict.
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
   * @param pid the process id of worker/driver to acquire resources.
   * @return allocated resources for the worker/driver or throws exception if can't
   *         meet worker/driver's requirement
   */
  def acquireResources(
      conf: SparkConf,
      componentName: String,
      resources: Map[String, ResourceInformation],
      pid: Int)
    : Map[String, ResourceInformation] = {
    if (!needCoordinate(conf)) {
      return resources
    }
    val resourceRequirements = parseResourceRequirements(conf, componentName)
    if (resourceRequirements.isEmpty) {
      return Map.empty
    }
    val lock = acquireLock(conf)
    try {
      val resourcesFile = new File(getOrCreateResourcesDir(conf), ALLOCATED_RESOURCES_FILE)
      // all allocated resources in ALLOCATED_RESOURCES_FILE, can be updated if any allocations'
      // related processes detected to be terminated while checking pids below.
      var origAllocation = Seq.empty[StandaloneResourceAllocation]
      // Map[pid -> Map[resourceName -> Addresses[]]]
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
            // if len(retained) < len(available) after differ to assigned, then, there must be
            // some conflicting resources addresses between available and assigned. So, we should
            // store its pid here to check whether it's alive in case we don't find enough
            // resources after traversal all allocated resources.
            if (retained.length < available.length && !checked) {
              pidsToCheck += thePid
            }
            if (retained.length >= amount) {
              available = retained
            } else if (checked) {
              keepAllocating = false
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
              origAllocation = origAllocation.filter(
                allocation => !invalid.contains(allocation.pid))
              checked = true
              // note this is a meaningless return value, just to avoid creating any new object
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
      writeResourceAllocationJson(
        componentName, origAllocation ++ Seq(newAllocation), resourcesFile)
      newAllocation.toResourceInformationMap
    } finally {
      releaseLock(lock)
    }
  }

  /**
   * Frees (if coordinate needed) all the resources a worker/driver (pid) has in one shot
   * to make those resources be available for other workers/drivers on the same host.
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
    if (!needCoordinate(conf)) {
      return
    }
    if (toRelease != null && toRelease.nonEmpty) {
      val lock = acquireLock(conf)
      try {
        val resourcesFile = new File(getOrCreateResourcesDir(conf), ALLOCATED_RESOURCES_FILE)
        if (resourcesFile.exists()) {
          val (target, others) =
            allocatedStandaloneResources(resourcesFile.getPath).partition(_.pid == pid)
          if (target.nonEmpty) {
            if (others.isEmpty) {
              if (!resourcesFile.delete()) {
                logError(s"Failed to delete $ALLOCATED_RESOURCES_FILE.")
              }
            } else {
              writeResourceAllocationJson(componentName, others, resourcesFile)
            }
            logDebug(s"$componentName(pid=$pid) released resources: ${toRelease.mkString("\n")}")
          } else {
            logWarning(s"$componentName(pid=$pid) has already released its resources.")
          }
        }
      } finally {
        releaseLock(lock)
      }
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
        logError(s"Error while releasing lock on $RESOURCES_LOCK_FILE.", e)
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

  private def allocatedStandaloneResources(resourcesFile: String)
  : Seq[StandaloneResourceAllocation] = {
    withResourcesJson[StandaloneResourceAllocation](resourcesFile) { json =>
      implicit val formats = DefaultFormats
      parse(json).extract[Seq[StandaloneResourceAllocation]]
    }
  }

  /**
   * Save the allocated resources of driver(cluster only) or executor into a JSON formatted
   * resources file. Used in Standalone only.
   * @param componentName spark.driver / spark.executor
   * @param resources allocated resources for driver(cluster only) or executor
   * @param dir the target directory used to place the resources file
   * @return None if resources is empty or Some(file) which represents the resources file
   */
  def prepareResourcesFile(
      componentName: String,
      resources: Map[String, ResourceInformation],
      dir: File): Option[File] = {
    if (resources.isEmpty) {
      return None
    }

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
    val resourcesFile = File.createTempFile(s"resource-$compShortName-", ".json", dir)
    tmpFile.renameTo(resourcesFile)
    Some(resourcesFile)
  }

  private def writeResourceAllocationJson[T](
      componentName: String,
      allocations: Seq[T],
      jsonFile: File): Unit = {
    implicit val formats = DefaultFormats
    val allocationJson = Extraction.decompose(allocations)
    Files.write(jsonFile.toPath, compact(render(allocationJson)).getBytes())
  }

  /** Whether needs to coordinate resources among workers and drivers for user */
  def needCoordinate(conf: SparkConf): Boolean = {
    conf.get(SPARK_RESOURCES_COORDINATE)
  }

  def toMutable(immutableResources: Map[String, ResourceInformation])
    : Map[String, MutableResourceInfo] = {
    immutableResources.map { case (rName, rInfo) =>
      val mutableAddress = new mutable.HashSet[String]()
      mutableAddress ++= rInfo.addresses
      rName -> MutableResourceInfo(rInfo.name, mutableAddress)
    }
  }

  // used for UI
  def formatResourcesDetails(
      usedInfo: Map[String, ResourceInformation],
      freeInfo: Map[String, ResourceInformation]): String = {
    usedInfo.map { case (rName, rInfo) =>
      val used = rInfo.addresses.mkString("[", ", ", "]")
      val free = freeInfo(rName).addresses.mkString("[", ", ", "]")
      s"$rName: Free: $free / Used: $used"
    }.mkString(", ")
  }

  // used for UI
  def formatResourcesAddresses(resources: Map[String, ResourceInformation]): String = {
    resources.map { case (rName, rInfo) =>
      s"$rName: ${rInfo.addresses.mkString("[", ", ", "]")}"
    }.mkString(", ")
  }

  // used for UI
  def formatResourcesUsed(
      resourcesTotal: Map[String, ResourceInformation],
      resourcesUsed: Map[String, ResourceInformation]): String = {
    resourcesTotal.map { case (rName, rInfo) =>
      val used = resourcesUsed(rName).addresses.length
      val total = rInfo.addresses.length
      s"$used / $total $rName"
    }.mkString(", ")
  }

  // used for UI
  def formatResourceRequirements(requirements: Seq[ResourceRequirement]): String = {
    requirements.map(req => s"${req.amount} ${req.resourceName}").mkString(", ")
  }
}
