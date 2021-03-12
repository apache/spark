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

import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Evolving

/**
 * Resource profile builder to build a Resource profile to associate with an RDD.
 * A ResourceProfile allows the user to specify executor and task requirements for an RDD
 * that will get applied during a stage. This allows the user to change the resource
 * requirements between stages.
 *
 * This api is currently private until the rest of the pieces are in place and then it
 * will become public.
 */
@Evolving
private[spark] class ResourceProfileBuilder() {

  private val _taskResources = new ConcurrentHashMap[String, TaskResourceRequest]()
  private val _executorResources = new ConcurrentHashMap[String, ExecutorResourceRequest]()

  def taskResources: Map[String, TaskResourceRequest] = _taskResources.asScala.toMap
  def executorResources: Map[String, ExecutorResourceRequest] = _executorResources.asScala.toMap

  /**
   * (Java-specific) gets a Java Map of resources to TaskResourceRequest
   */
  def taskResourcesJMap: JMap[String, TaskResourceRequest] = _taskResources.asScala.asJava

  /**
   * (Java-specific) gets a Java Map of resources to ExecutorResourceRequest
   */
  def executorResourcesJMap: JMap[String, ExecutorResourceRequest] = {
    _executorResources.asScala.asJava
  }

  def require(requests: ExecutorResourceRequests): this.type = {
    _executorResources.putAll(requests.requests.asJava)
    this
  }

  def require(requests: TaskResourceRequests): this.type = {
    _taskResources.putAll(requests.requests.asJava)
    this
  }

  def clearExecutorResourceRequests(): this.type = {
    _executorResources.clear()
    this
  }

  def clearTaskResourceRequests(): this.type = {
    _taskResources.clear()
    this
  }

  override def toString(): String = {
    "Profile executor resources: " +
      s"${_executorResources.asScala.map(pair => s"${pair._1}=${pair._2.toString()}")}, " +
      s"task resources: ${_taskResources.asScala.map(pair => s"${pair._1}=${pair._2.toString()}")}"
  }

  def build: ResourceProfile = {
    new ResourceProfile(executorResources, taskResources)
  }
}

