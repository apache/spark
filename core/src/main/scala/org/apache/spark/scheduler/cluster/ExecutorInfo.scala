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
package org.apache.spark.scheduler.cluster

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID

/**
 * :: DeveloperApi ::
 * Stores information about an executor to pass from the scheduler to SparkListeners.
 */
@DeveloperApi
class ExecutorInfo(
    val executorHost: String,
    val totalCores: Int,
    val logUrlMap: Map[String, String],
    val attributes: Map[String, String],
    val resourcesInfo: Map[String, ResourceInformation],
    val resourceProfileId: Int,
    val registrationTime: Option[Long],
    val requestTime: Option[Long]) {

  def this(executorHost: String, totalCores: Int, logUrlMap: Map[String, String],
      attributes: Map[String, String], resourcesInfo: Map[String, ResourceInformation],
      resourceProfileId: Int) = {
    this(executorHost, totalCores, logUrlMap, attributes, resourcesInfo, resourceProfileId,
      None, None)
  }
  def this(executorHost: String, totalCores: Int, logUrlMap: Map[String, String]) = {
    this(executorHost, totalCores, logUrlMap, Map.empty, Map.empty, DEFAULT_RESOURCE_PROFILE_ID,
      None, None)
  }

  def this(
      executorHost: String,
      totalCores: Int,
      logUrlMap: Map[String, String],
      attributes: Map[String, String]) = {
    this(executorHost, totalCores, logUrlMap, attributes, Map.empty, DEFAULT_RESOURCE_PROFILE_ID,
      None, None)
  }

  def this(
      executorHost: String,
      totalCores: Int,
      logUrlMap: Map[String, String],
      attributes: Map[String, String],
      resourcesInfo: Map[String, ResourceInformation]) = {
    this(executorHost, totalCores, logUrlMap, attributes, resourcesInfo,
      DEFAULT_RESOURCE_PROFILE_ID, None, None)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ExecutorInfo]

  override def equals(other: Any): Boolean = other match {
    case that: ExecutorInfo =>
      (that canEqual this) &&
        executorHost == that.executorHost &&
        totalCores == that.totalCores &&
        logUrlMap == that.logUrlMap &&
        attributes == that.attributes &&
        resourcesInfo == that.resourcesInfo &&
        resourceProfileId == that.resourceProfileId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(executorHost, totalCores, logUrlMap, attributes, resourcesInfo,
      resourceProfileId)
    state.filter(_ != null).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
