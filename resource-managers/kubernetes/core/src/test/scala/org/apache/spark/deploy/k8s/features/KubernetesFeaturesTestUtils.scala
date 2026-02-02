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
package org.apache.spark.deploy.k8s.features

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import io.fabric8.kubernetes.api.model.{Container, HasMetadata, PodBuilder, SecretBuilder}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.deploy.k8s.SparkPod
import org.apache.spark.resource.ResourceID

object KubernetesFeaturesTestUtils {

  def getMockConfigStepForStepType[T <: KubernetesFeatureConfigStep](
    stepType: String, stepClass: Class[T]): T = {
    val mockStep = mock(stepClass)
    when(mockStep.getAdditionalKubernetesResources()).thenReturn(
      getSecretsForStepType(stepType))

    when(mockStep.getAdditionalPodSystemProperties())
      .thenReturn(Map(stepType -> stepType))
    when(mockStep.configurePod(any(classOf[SparkPod])))
      .thenAnswer((invocation: InvocationOnMock) => {
        val originalPod: SparkPod = invocation.getArgument(0)
        val configuredPod = new PodBuilder(originalPod.pod)
          .editOrNewMetadata()
          .addToLabels(stepType, stepType)
          .endMetadata()
          .build()
        SparkPod(configuredPod, originalPod.container)
      })
    mockStep
  }

  def getSecretsForStepType[T <: KubernetesFeatureConfigStep](stepType: String)
    : Seq[HasMetadata] = {
    Seq(new SecretBuilder()
      .withNewMetadata()
      .withName(stepType)
      .endMetadata()
      .build())
  }

  def containerHasEnvVar(container: Container, envVarName: String): Boolean = {
    container.getEnv.asScala.exists(envVar => envVar.getName == envVarName)
  }

  def filter[T: ClassTag](list: Seq[HasMetadata]): Seq[T] = {
    val desired = implicitly[ClassTag[T]].runtimeClass
    list.filter(_.getClass() == desired).map(_.asInstanceOf[T])
  }

  case class TestResourceInformation(rId: ResourceID, count: String, vendor: String)
}
