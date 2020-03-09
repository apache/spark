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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Tests._
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils.isTesting

/**
 * Manager of resource profiles. The manager allows one place to keep the actual ResourceProfiles
 * and everywhere else we can use the ResourceProfile Id to save on space.
 * Note we never remove a resource profile at this point. Its expected this number if small
 * so this shouldn't be much overhead.
 */
@Evolving
private[spark] class ResourceProfileManager(sparkConf: SparkConf) extends Logging {
  private val resourceProfileIdToResourceProfile = new ConcurrentHashMap[Int, ResourceProfile]()

  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
  addResourceProfile(defaultProfile)

  def defaultResourceProfile: ResourceProfile = defaultProfile

  private val taskCpusDefaultProfile = defaultProfile.getTaskCpus.get
  private val dynamicEnabled = Utils.isDynamicAllocationEnabled(sparkConf)
  private val master = sparkConf.getOption("spark.master")
  private val isNotYarn = master.isDefined && !master.get.equals("yarn")
  private val errorForTesting = !isTesting || sparkConf.get(RESOURCE_PROFILE_MANAGER_TESTING)

  // If we use anything except the default profile, its only supported on YARN right now.
  // Throw an exception if not supported.
  private[spark] def isSupported(rp: ResourceProfile): Boolean = {
    val isNotDefaultProfile = rp.id != ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
    val notYarnAndNotDefaultProfile = isNotDefaultProfile && isNotYarn
    val YarnNotDynAllocAndNotDefaultProfile = isNotDefaultProfile && !isNotYarn && !dynamicEnabled
    if (errorForTesting && (notYarnAndNotDefaultProfile || YarnNotDynAllocAndNotDefaultProfile)) {
      throw new SparkException("ResourceProfiles are only supported on YARN with dynamic " +
        "allocation enabled.")
    }
    true
  }

  def addResourceProfile(rp: ResourceProfile): Unit = {
    isSupported(rp)
    // force the computation of maxTasks and limitingResource now so we don't have cost later
    rp.limitingResource(sparkConf)
    logInfo(s"Adding ResourceProfile id: ${rp.id}")
    resourceProfileIdToResourceProfile.putIfAbsent(rp.id, rp)
  }

  /*
   * Gets the ResourceProfile associated with the id, if a profile doesn't exist
   * it returns the default ResourceProfile created from the application level configs.
   */
  def resourceProfileFromId(rpId: Int): ResourceProfile = {
    val rp = resourceProfileIdToResourceProfile.get(rpId)
    if (rp == null) {
      throw new SparkException(s"ResourceProfileId $rpId not found!")
    }
    rp
  }

  def taskCpusForProfileId(rpId: Int): Int = {
    resourceProfileFromId(rpId).getTaskCpus.getOrElse(taskCpusDefaultProfile)
  }
}
