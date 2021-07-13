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
package org.apache.spark.errors

import java.io.File
import java.util.Optional

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceRequest, TaskResourceRequest}


private [spark] object ExecutionErrors {

  def specifyADiscoveryScript(resourceName: String): Throwable = {
    new SparkException(s"User is expecting to use resource: $resourceName, but " +
      "didn't specify a discovery script!")
  }

  def runningOtherResource(script: Optional[String],
                           result: ResourceInformation, resourceName: String): Throwable = {
    new SparkException(s"Error running the resource discovery script ${script.get}: " +
      s"script returned resource name ${result.name} and we were expecting $resourceName.")
  }

  def conditionOfResource(rName: String, execReq: ExecutorResourceRequest, taskReq: Double):
  Throwable = {
    new SparkException(s"The executor resource: $rName, amount: ${execReq.amount}" +
      s"needs to be >= the task resource request amount of $taskReq")
  }

  def notExistResourceScript(scriptFile: File, resourceName: String): Throwable = {
    new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
      "doesn't exist!")
  }

  def notExistResource(resource: String, id: Int): Throwable = {
    new SparkException(s"Resource $resource doesn't exist in profile id: $id")
  }

  def noExecutorResourceConfig( taskResourcesToCheck: mutable.HashMap[String, TaskResourceRequest]):
  Throwable = {
    new SparkException("No executor resource configs were not specified for the " +
      s"following task configs: ${taskResourcesToCheck.keys.mkString(",")}")
  }

  def resourceProfileSupport(): Throwable = {
    new SparkException("ResourceProfiles are only supported on YARN and Kubernetes " +
      "with dynamic allocation enabled.")
  }

  def resourceProfile(rpId: Int): Throwable = {
    new SparkException(s"ResourceProfileId $rpId not found!")
  }

  def conditionOfNumberOfCores(execCores: Int, taskCpus: Int ): Throwable = {
    new SparkException(s"The number of cores per executor (=$execCores) has to be >= " +
      s"the number of cpus per task = $taskCpus.")
  }

  def adjustConfiguration(message: String): Throwable = {
    new SparkException(message)
  }

  def returnResourceInformation(resourceRequest: ResourceRequest): Throwable = {
    new SparkException(s"None of the discovery plugins returned ResourceInformation for " +
      s"${resourceRequest.id.resourceName}")
  }
}
