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

package org.apache.spark.status.protobuf

import collection.JavaConverters._

import org.apache.spark.resource.{ExecutorResourceRequest, TaskResourceRequest}
import org.apache.spark.status.ApplicationEnvironmentInfoWrapper
import org.apache.spark.status.api.v1.{ApplicationEnvironmentInfo, ResourceProfileInfo, RuntimeInfo}

class ApplicationEnvironmentInfoWrapperSerializer extends ProtobufSerDe {

  override val supportClass: Class[_] = classOf[ApplicationEnvironmentInfoWrapper]

  override def serialize(input: Any): Array[Byte] =
    serialize(input.asInstanceOf[ApplicationEnvironmentInfoWrapper])

  private def serialize(input: ApplicationEnvironmentInfoWrapper): Array[Byte] = {
    val builder = StoreTypes.ApplicationEnvironmentInfoWrapper.newBuilder()
    builder.setInfo(serializeApplicationEnvironmentInfo(input.info))
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): ApplicationEnvironmentInfoWrapper = {
    val wrapper = StoreTypes.ApplicationEnvironmentInfoWrapper.parseFrom(bytes)
    new ApplicationEnvironmentInfoWrapper(
      info = deserializeApplicationEnvironmentInfo(wrapper.getInfo)
    )
  }

  private def serializeApplicationEnvironmentInfo(info: ApplicationEnvironmentInfo):
    StoreTypes.ApplicationEnvironmentInfo = {

    val runtimeBuilder = StoreTypes.RuntimeInfo.newBuilder()
    runtimeBuilder.setJavaVersion(info.runtime.javaVersion)
    runtimeBuilder.setJavaHome(info.runtime.javaHome)
    runtimeBuilder.setScalaVersion(info.runtime.scalaVersion)

    val builder = StoreTypes.ApplicationEnvironmentInfo.newBuilder()
    builder.setRuntime(runtimeBuilder.build())
    info.sparkProperties.foreach { pair =>
      builder.addSparkProperties(serializePairStrings(pair))
    }
    info.hadoopProperties.foreach { pair =>
      builder.addHadoopProperties(serializePairStrings(pair))
    }
    info.systemProperties.foreach { pair =>
      builder.addSystemProperties(serializePairStrings(pair))
    }
    info.metricsProperties.foreach { pair =>
      builder.addMetricsProperties(serializePairStrings(pair))
    }
    info.classpathEntries.foreach { pair =>
      builder.addClasspathEntries(serializePairStrings(pair))
    }
    info.resourceProfiles.foreach { profile =>
      builder.addResourceProfiles(serializeResourceProfileInfo(profile))
    }
    builder.build()
  }

  private def deserializeApplicationEnvironmentInfo(info: StoreTypes.ApplicationEnvironmentInfo):
    ApplicationEnvironmentInfo = {
    val runtime = new RuntimeInfo (
      javaVersion = info.getRuntime.getJavaVersion,
      javaHome = info.getRuntime.getJavaHome,
      scalaVersion = info.getRuntime.getScalaVersion
    )
    val pairSSToTuple = (pair: StoreTypes.PairStrings) => {
      (pair.getValue1, pair.getValue2)
    }
    new ApplicationEnvironmentInfo(
      runtime = runtime,
      sparkProperties = info.getSparkPropertiesList.asScala.map(pairSSToTuple).toSeq,
      hadoopProperties = info.getHadoopPropertiesList.asScala.map(pairSSToTuple).toSeq,
      systemProperties = info.getSystemPropertiesList.asScala.map(pairSSToTuple).toSeq,
      metricsProperties = info.getMetricsPropertiesList.asScala.map(pairSSToTuple).toSeq,
      classpathEntries = info.getClasspathEntriesList.asScala.map(pairSSToTuple).toSeq,
      resourceProfiles =
        info.getResourceProfilesList.asScala.map(deserializeResourceProfileInfo).toSeq
    )
  }

  private def serializePairStrings(pair: (String, String)): StoreTypes.PairStrings = {
    val builder = StoreTypes.PairStrings.newBuilder()
    builder.setValue1(pair._1)
    builder.setValue2(pair._2)
    builder.build()
  }

  private[status] def serializeResourceProfileInfo(info: ResourceProfileInfo):
    StoreTypes.ResourceProfileInfo = {
    val builder = StoreTypes.ResourceProfileInfo.newBuilder()
    builder.setId(info.id)
    info.executorResources.foreach{case (k, resource) =>
      val requestBuilder = StoreTypes.ExecutorResourceRequest.newBuilder()
      requestBuilder.setResourceName(resource.resourceName)
      requestBuilder.setAmount(resource.amount)
      requestBuilder.setDiscoveryScript(resource.discoveryScript)
      requestBuilder.setVendor(resource.vendor)
      builder.putExecutorResources(k, requestBuilder.build())
    }
    info.taskResources.foreach { case (k, resource) =>
      val requestBuilder = StoreTypes.TaskResourceRequest.newBuilder()
      requestBuilder.setResourceName(resource.resourceName)
      requestBuilder.setAmount(resource.amount)
      builder.putTaskResources(k, requestBuilder.build())
    }
    builder.build()
  }

  private[status] def deserializeResourceProfileInfo(info: StoreTypes.ResourceProfileInfo):
    ResourceProfileInfo = {

    new ResourceProfileInfo(
      id = info.getId,
      executorResources =
        info.getExecutorResourcesMap.asScala.mapValues(deserializeExecutorResourceRequest).toMap,
      taskResources =
        info.getTaskResourcesMap.asScala.mapValues(deserializeTaskResourceRequest).toMap)
  }

  private def deserializeExecutorResourceRequest(info: StoreTypes.ExecutorResourceRequest):
    ExecutorResourceRequest = {
    new ExecutorResourceRequest(
      resourceName = info.getResourceName,
      amount = info.getAmount,
      discoveryScript = info.getDiscoveryScript,
      vendor = info.getVendor
    )
  }

  private def deserializeTaskResourceRequest(info: StoreTypes.TaskResourceRequest):
    TaskResourceRequest = {
    new TaskResourceRequest(resourceName = info.getResourceName, amount = info.getAmount)
  }
}
