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

import scala.jdk.CollectionConverters._

import org.apache.spark.resource.{ExecutorResourceRequest, TaskResourceRequest}
import org.apache.spark.status.ApplicationEnvironmentInfoWrapper
import org.apache.spark.status.api.v1.{ApplicationEnvironmentInfo, ResourceProfileInfo, RuntimeInfo}
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}

private[protobuf] class ApplicationEnvironmentInfoWrapperSerializer
  extends ProtobufSerDe[ApplicationEnvironmentInfoWrapper] {

  override def serialize(input: ApplicationEnvironmentInfoWrapper): Array[Byte] = {
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
    val runtime = info.runtime
    setStringField(runtime.javaHome, runtimeBuilder.setJavaHome)
    setStringField(runtime.javaVersion, runtimeBuilder.setJavaVersion)
    setStringField(runtime.scalaVersion, runtimeBuilder.setScalaVersion)

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
    val rt = info.getRuntime
    val runtime = new RuntimeInfo (
      javaVersion = getStringField(rt.hasJavaVersion, () => rt.getJavaVersion),
      javaHome = getStringField(rt.hasJavaHome, () => rt.getJavaHome),
      scalaVersion = getStringField(rt.hasScalaVersion, () => rt.getScalaVersion)
    )
    val pairSSToTuple = (pair: StoreTypes.PairStrings) => {
      (getStringField(pair.hasValue1, pair.getValue1),
        getStringField(pair.hasValue2, pair.getValue2))
    }
    new ApplicationEnvironmentInfo(
      runtime = runtime,
      sparkProperties = info.getSparkPropertiesList.asScala.map(pairSSToTuple),
      hadoopProperties = info.getHadoopPropertiesList.asScala.map(pairSSToTuple),
      systemProperties = info.getSystemPropertiesList.asScala.map(pairSSToTuple),
      metricsProperties = info.getMetricsPropertiesList.asScala.map(pairSSToTuple),
      classpathEntries = info.getClasspathEntriesList.asScala.map(pairSSToTuple),
      resourceProfiles =
        info.getResourceProfilesList.asScala.map(deserializeResourceProfileInfo)
    )
  }

  private def serializePairStrings(pair: (String, String)): StoreTypes.PairStrings = {
    val builder = StoreTypes.PairStrings.newBuilder()
    setStringField(pair._1, builder.setValue1)
    setStringField(pair._2, builder.setValue2)
    builder.build()
  }

  private[status] def serializeResourceProfileInfo(info: ResourceProfileInfo):
    StoreTypes.ResourceProfileInfo = {
    val builder = StoreTypes.ResourceProfileInfo.newBuilder()
    builder.setId(info.id)
    info.executorResources.foreach{case (k, resource) =>
      val requestBuilder = StoreTypes.ExecutorResourceRequest.newBuilder()
      setStringField(resource.resourceName, requestBuilder.setResourceName)
      requestBuilder.setAmount(resource.amount)
      setStringField(resource.discoveryScript, requestBuilder.setDiscoveryScript)
      setStringField(resource.vendor, requestBuilder.setVendor)
      builder.putExecutorResources(k, requestBuilder.build())
    }
    info.taskResources.foreach { case (k, resource) =>
      val requestBuilder = StoreTypes.TaskResourceRequest.newBuilder()
      setStringField(resource.resourceName, requestBuilder.setResourceName)
      requestBuilder.setAmount(resource.amount)
      builder.putTaskResources(k, requestBuilder.build())
    }
    builder.build()
  }

  private[status] def deserializeResourceProfileInfo(info: StoreTypes.ResourceProfileInfo):
    ResourceProfileInfo = {

    new ResourceProfileInfo(
      id = info.getId,
      executorResources = info.getExecutorResourcesMap.asScala.toMap
        .transform((_, v) => deserializeExecutorResourceRequest(v)),
      taskResources = info.getTaskResourcesMap.asScala.toMap
        .transform((_, v) => deserializeTaskResourceRequest(v))
    )
  }

  private def deserializeExecutorResourceRequest(info: StoreTypes.ExecutorResourceRequest):
    ExecutorResourceRequest = {
    new ExecutorResourceRequest(
      resourceName = getStringField(info.hasResourceName, () => info.getResourceName),
      amount = info.getAmount,
      discoveryScript = getStringField(info.hasDiscoveryScript, () => info.getDiscoveryScript),
      vendor = getStringField(info.hasVendor, () => info.getVendor)
    )
  }

  private def deserializeTaskResourceRequest(info: StoreTypes.TaskResourceRequest):
    TaskResourceRequest = {
    new TaskResourceRequest(
      resourceName = getStringField(info.hasResourceName, () => info.getResourceName),
      amount = info.getAmount
    )
  }
}
