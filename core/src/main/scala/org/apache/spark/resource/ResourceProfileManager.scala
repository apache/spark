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

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.HashMap

import org.apache.spark.{SparkConf, SparkException, SparkMasterRegex}
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config.Tests._
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerResourceProfileAdded}
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils.isTesting

/**
 * Manager of resource profiles. The manager allows one place to keep the actual ResourceProfiles
 * and everywhere else we can use the ResourceProfile Id to save on space.
 * Note we never remove a resource profile at this point. Its expected this number is small
 * so this shouldn't be much overhead.
 */
@Evolving
private[spark] class ResourceProfileManager(sparkConf: SparkConf,
    listenerBus: LiveListenerBus) extends Logging {
  private val resourceProfileIdToResourceProfile = new HashMap[Int, ResourceProfile]()

  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock(), lock.writeLock())
  }

  private val dynamicEnabled = Utils.isDynamicAllocationEnabled(sparkConf)
  private val master = sparkConf.getOption("spark.master")
  private val isYarn = master.isDefined && master.get.equals("yarn")
  private val isK8s = SparkMasterRegex.isK8s(master)
  private val isStandaloneOrLocalCluster = master.isDefined && (
      master.get.startsWith("spark://") || master.get.startsWith("local-cluster")
    )
  private val notRunningUnitTests = !isTesting
  private val testExceptionThrown = sparkConf.get(RESOURCE_PROFILE_MANAGER_TESTING)

  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
  addResourceProfile(defaultProfile)

  def defaultResourceProfile: ResourceProfile = defaultProfile

  /**
   * If we use anything except the default profile, it's supported on YARN, Kubernetes and
   * Standalone with dynamic allocation enabled, and task resource profile with dynamic allocation
   * disabled on Standalone. Throw an exception if not supported.
   */
  private[spark] def isSupported(rp: ResourceProfile): Boolean = {
    assert(master != null)
    if (rp.isInstanceOf[TaskResourceProfile] && !dynamicEnabled) {
      if ((notRunningUnitTests || testExceptionThrown) &&
        !(isStandaloneOrLocalCluster || isYarn || isK8s)) {
        throw new SparkException("TaskResourceProfiles are only supported for Standalone, " +
          "Yarn and Kubernetes cluster for now when dynamic allocation is disabled.")
      }
    } else {
      val isNotDefaultProfile = rp.id != ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
      val notYarnOrK8sOrStandaloneAndNotDefaultProfile =
        isNotDefaultProfile && !(isYarn || isK8s || isStandaloneOrLocalCluster)
      val YarnOrK8sOrStandaloneNotDynAllocAndNotDefaultProfile =
        isNotDefaultProfile && (isYarn || isK8s || isStandaloneOrLocalCluster) && !dynamicEnabled

      // We want the exception to be thrown only when we are specifically testing for the
      // exception or in a real application. Otherwise in all other testing scenarios we want
      // to skip throwing the exception so that we can test in other modes to make testing easier.
      if ((notRunningUnitTests || testExceptionThrown) &&
        (notYarnOrK8sOrStandaloneAndNotDefaultProfile ||
          YarnOrK8sOrStandaloneNotDynAllocAndNotDefaultProfile)) {
        throw new SparkException("ResourceProfiles are only supported on YARN and Kubernetes " +
          "and Standalone with dynamic allocation enabled.")
      }

      if (isStandaloneOrLocalCluster && dynamicEnabled && rp.getExecutorCores.isEmpty &&
        sparkConf.getOption(config.EXECUTOR_CORES.key).isEmpty) {
        logWarning("Neither executor cores is set for resource profile, nor spark.executor.cores " +
          "is explicitly set, you may get more executors allocated than expected. " +
          "It's recommended to set executor cores explicitly. " +
          "Please check SPARK-30299 for more details.")
      }
    }

    true
  }

  /**
   * Check whether a task with specific taskRpId can be scheduled to executors
   * with executorRpId.
   *
   * Here are the rules:
   * 1. When dynamic allocation is disabled, only [[TaskResourceProfile]] is supported,
   *    and tasks with [[TaskResourceProfile]] can be scheduled to executors with default
   *    resource profile.
   * 2. For other scenarios(when dynamic allocation is enabled), tasks can be scheduled to
   *    executors where resource profile exactly matches.
   */
  private[spark] def canBeScheduled(taskRpId: Int, executorRpId: Int): Boolean = {
    assert(resourceProfileIdToResourceProfile.contains(taskRpId) &&
      resourceProfileIdToResourceProfile.contains(executorRpId),
      "Tasks and executors must have valid resource profile id")
    val taskRp = resourceProfileFromId(taskRpId)

    // When dynamic allocation disabled, tasks with TaskResourceProfile can always reuse
    // all the executors with default resource profile.
    taskRpId == executorRpId || (!dynamicEnabled && taskRp.isInstanceOf[TaskResourceProfile])
  }

  /**
   * Register the given ResourceProfile unless its id is already registered. Unlike
   * [[getOrAddEquivalentProfile]], this method does not reuse a profile with equal resources.
  */
  def addResourceProfile(rp: ResourceProfile): Unit = {
    isSupported(rp)
    registerProfileIfAbsent(rp, findProfileById)
  }

  /**
   * Get the registered ResourceProfile whose resources are equal to the given one, registering
   * the given profile first if no equivalent one exists yet.
   */
  def getOrAddEquivalentProfile(rp: ResourceProfile): ResourceProfile = {
    isSupported(rp)
    registerProfileIfAbsent(rp, findEquivalentOrSameIdProfile)
  }

  private def registerProfileIfAbsent(
      rp: ResourceProfile,
      findRegistered: ResourceProfile => Option[ResourceProfile]): ResourceProfile = {
    val existingProfile = {
      readLock.lock()
      try {
        findRegistered(rp)
      } finally {
        readLock.unlock()
      }
    }
    existingProfile.getOrElse {
      // Validate before inserting so a malformed profile never enters the registry, where it
      // would stay visible to the whole application. The cpus amount is checked under the map
      // key -- the identity scheduling uses -- with the same rule as the request entry points:
      // construction stays lenient (deserialization of persisted data must accept anything an
      // earlier release wrote), so registration is the enforcement point for every live path,
      // including raw profile construction and Spark Connect.
      rp.taskResources.get(ResourceProfile.CPUS).foreach { treq =>
        require(!treq.amount.isNaN && !treq.amount.isInfinity &&
          CpuAmount.isInRange(CpuAmount.normalize(BigDecimal(treq.amount.toString))),
          s"The cpus amount ${treq.amount} must be at least 1e-9 and at most ${Int.MaxValue}.")
      }
      // Force the computation of maxTasks and limitingResource now so we don't have cost later;
      // doing it before the insert also surfaces any other malformed shape (e.g. a task
      // resource without a matching executor resource) before registration instead of after.
      rp.limitingResource(sparkConf)
      var addedProfile: Option[ResourceProfile] = None
      val resolvedProfile = {
        writeLock.lock()
        try {
          // Another thread may have added this profile after the read-lock fast path, so check
          // again while holding the write lock.
          findRegistered(rp).getOrElse {
            resourceProfileIdToResourceProfile.put(rp.id, rp)
            addedProfile = Some(rp)
            rp
          }
        } finally {
          writeLock.unlock()
        }
      }
      // do this outside the write lock only when we add a new profile
      addedProfile.foreach(onProfileAdded)
      resolvedProfile
    }
  }

  private def findProfileById(rp: ResourceProfile): Option[ResourceProfile] = {
    resourceProfileIdToResourceProfile.get(rp.id)
  }

  private def findEquivalentOrSameIdProfile(rp: ResourceProfile): Option[ResourceProfile] = {
    resourceProfileIdToResourceProfile.get(rp.id).orElse(findEquivalentProfile(rp))
  }

  private def findEquivalentProfile(rp: ResourceProfile): Option[ResourceProfile] = {
    resourceProfileIdToResourceProfile.values.find { existing =>
      val sameDefaultStatus =
        (existing eq defaultProfile) == (rp eq defaultProfile)
      sameDefaultStatus && existing.resourcesEqual(rp)
    }
  }

  private def onProfileAdded(rp: ResourceProfile): Unit = {
    logInfo(log"Added ResourceProfile id: ${MDC(LogKeys.RESOURCE_PROFILE_ID, rp.id)}")
    listenerBus.post(SparkListenerResourceProfileAdded(rp))
  }

  /*
   * Gets the ResourceProfile associated with the id, if a profile doesn't exist
   * it returns the default ResourceProfile created from the application level configs.
   */
  def resourceProfileFromId(rpId: Int): ResourceProfile = {
    readLock.lock()
    try {
      resourceProfileIdToResourceProfile.getOrElse(rpId,
        throw new SparkException(s"ResourceProfileId $rpId not found!")
      )
    } finally {
      readLock.unlock()
    }
  }

  /*
   * If the ResourceProfile passed in is equivalent to an existing one, return the existing one.
   * The actual default profile is kept distinct because cluster managers give its id special
   * semantics that do not apply to explicit profiles with otherwise equal resources.
   */
  def getEquivalentProfile(rp: ResourceProfile): Option[ResourceProfile] = {
    readLock.lock()
    try {
      findEquivalentProfile(rp)
    } finally {
      readLock.unlock()
    }
  }
}
