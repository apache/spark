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

import org.apache.spark.annotation.Since
import org.apache.spark.resource.ResourceProfileCompatiblePolicy.resourceProfileCompatibleWithEqualCores

/**
 * Resource profile compatible policy interface allows the user to customize its
 * own compatible policy
 *
 */
@Since("3.3.0")
trait ResourceProfileCompatiblePolicyInterface {
  def getName(): String
  def isCompatible(prevRP: ResourceProfile, curRP: ResourceProfile): Boolean
}

/**
 * Resource profile compatible policy: only reuse executors with same cores
 */
class ResourceProfileCompatiblePolicyEqualCores extends ResourceProfileCompatiblePolicyInterface {
  override def getName(): String = {
    return "EXEC_EQUAL_CORES"
  }
  override def isCompatible(prevRP: ResourceProfile, curRP: ResourceProfile): Boolean = {
    resourceProfileCompatibleWithEqualCores(prevRP, curRP)
  }

}

/**
 * Resource profile compatibility based on user policy
 *
 * EQUAL_RESOURCES:
 *     only reuse executors with same resources (including cores, memory and all 3rd party
 *     resources)
 * MORE_RESOURCES:
 *     reuse executors with equal or more resources (including cores, memory and all 3rd party
 *     resources)
 * Notes:
 *     if EQUAL_RESOURCES is specified, there is no resource waste but less
 *     user flexibility.
 *     if MORE_RESOURCES is specified, users should know there are some resources
 *     wasted. They need to tradeoff between reusing executors and creating new ones.
 */
object ResourceProfileCompatiblePolicy extends Enumeration {

  type ResourceProfileCompatiblePolicy = Value

  val EQUAL_RESOURCES, MORE_RESOURCES = Value

  private def resourceProfileCompatibleWithPolicy(
      prevRP: ResourceProfile, curRP: ResourceProfile,
      resourceNames: Set[String], policy: ResourceProfileCompatiblePolicy): Boolean = {
    resourceNames.forall { resourceName: String =>
      policy match {
        case EQUAL_RESOURCES =>
          ! prevRP.executorResources.get(resourceName).isEmpty &&
          ! curRP.executorResources.get(resourceName).isEmpty &&
          prevRP.executorResources(resourceName).amount ==
            curRP.executorResources(resourceName).amount
        case MORE_RESOURCES =>
          ! prevRP.executorResources.get(resourceName).isEmpty &&
          ! curRP.executorResources.get(resourceName).isEmpty &&
          prevRP.executorResources(resourceName).amount >=
            curRP.executorResources(resourceName).amount
      }
    }
  }

  def resourceProfileCompatibleWithEqualCores(prevRP: ResourceProfile,
                                              curRP: ResourceProfile): Boolean = {
    resourceProfileCompatibleWithPolicy(prevRP, curRP, Set("cores"), EQUAL_RESOURCES)
  }

  def resourceProfileCompatibleWithMoreCores(prevRP: ResourceProfile,
                                             curRP: ResourceProfile): Boolean = {
    resourceProfileCompatibleWithPolicy(prevRP, curRP, Set("cores"), MORE_RESOURCES)
  }

  def resourceProfileCompatibleWithEqualResources(
    prevRP: ResourceProfile, curRP: ResourceProfile, resourceNames: Set[String]): Boolean = {
    resourceProfileCompatibleWithPolicy(prevRP, curRP, resourceNames, EQUAL_RESOURCES)
  }

  def resourceProfileCompatibleWithMoreResources(
      prevRP: ResourceProfile, curRP: ResourceProfile, resourceNames: Set[String]): Boolean = {
    resourceProfileCompatibleWithPolicy(prevRP, curRP, resourceNames, MORE_RESOURCES)
  }
}
