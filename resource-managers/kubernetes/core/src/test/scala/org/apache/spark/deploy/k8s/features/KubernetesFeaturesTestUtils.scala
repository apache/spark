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

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{Container, HasMetadata, PodBuilder, SecretBuilder}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.SparkPod
import org.apache.spark.util.Utils

object KubernetesFeaturesTestUtils {

  def getMockConfigStepForStepType[T <: KubernetesFeatureConfigStep](
    stepType: String, stepClass: Class[T]): T = {
    val mockStep = mock(stepClass)
    when(mockStep.getAdditionalKubernetesResources()).thenReturn(
      getSecretsForStepType(stepType))

    when(mockStep.getAdditionalPodSystemProperties())
      .thenReturn(Map(stepType -> stepType))
    when(mockStep.configurePod(Matchers.any(classOf[SparkPod])))
      .thenAnswer(new Answer[SparkPod]() {
        override def answer(invocation: InvocationOnMock): SparkPod = {
          val originalPod = invocation.getArgumentAt(0, classOf[SparkPod])
          val configuredPod = new PodBuilder(originalPod.pod)
            .editOrNewMetadata()
            .addToLabels(stepType, stepType)
            .endMetadata()
            .build()
          SparkPod(configuredPod, originalPod.container)
        }
      })
    mockStep
  }

  def getMockConfigStepForLocalFiles[T <: MountLocalFilesFeatureStep](
       stepType: String, stepClass: Class[T]): T = {
    val mockStep = getMockConfigStepForStepType(stepType, stepClass)
    val tempDir = Utils.createTempDir()
    val tempFile1 = new File(tempDir, "file1.txt")
    Files.write("a", tempFile1, Charsets.UTF_8)
    when(mockStep.allFiles()).thenReturn(Seq(tempFile1.getAbsolutePath))
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
}
