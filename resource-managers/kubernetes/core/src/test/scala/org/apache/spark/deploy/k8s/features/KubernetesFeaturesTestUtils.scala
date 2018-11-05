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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Container, HasMetadata, Pod, PodBuilder, SecretBuilder, Volume}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark.deploy.k8s.SparkPod

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

  def containerHasEnvVars(container: Container, envs: Map[String, String]): Unit = {
    assertHelper[Set[(String, String)]](envs.toSet,
      container.getEnv.asScala
        .map { e => (e.getName, e.getValue) }.toSet,
      subsetOfTup[Set[(String, String)], String], "a subset of")
  }

  def containerHasVolumeMounts(container: Container, vms: Map[String, String]): Unit = {
    assertHelper[Set[(String, String)]](vms.toSet,
      container.getVolumeMounts.asScala
        .map { vm => (vm.getName, vm.getMountPath) }.toSet,
      subsetOfTup[Set[(String, String)], String], "a subset of")
  }

  def podHasLabels(pod: Pod, labels: Map[String, String]): Unit = {
    assertHelper[Set[(String, String)]](labels.toSet, pod.getMetadata.getLabels.asScala.toSet,
      subsetOfTup[Set[(String, String)], String], "a subset of")
  }

  def podHasVolumes(pod: Pod, volumes: Seq[Volume]): Unit = {
    assertHelper[Set[Volume]](volumes.toSet, pod.getSpec.getVolumes.asScala.toSet,
      subsetOfElem[Set[Volume], Volume], "a subset of")
  }

  // Mocking bootstrapHadoopConfDir
  def hadoopConfBootPod(inputPod: SparkPod): SparkPod =
    SparkPod(
      new PodBuilder(inputPod.pod)
        .editOrNewMetadata()
          .addToLabels("bootstrap-hconf", "true")
          .endMetadata()
        .build(),
      inputPod.container)

  // Mocking bootstrapKerberosPod
  def krbBootPod(inputPod: SparkPod): SparkPod =
    SparkPod(
      new PodBuilder(inputPod.pod)
        .editOrNewMetadata()
          .addToLabels("bootstrap-kerberos", "true")
          .endMetadata()
        .build(),
      inputPod.container)

  // Mocking bootstrapSparkUserPod
  def userBootPod(inputPod: SparkPod): SparkPod =
    SparkPod(
      new PodBuilder(inputPod.pod)
        .editOrNewMetadata()
          .addToLabels("bootstrap-user", "true")
          .endMetadata()
        .build(),
      inputPod.container)

  def subsetOfElem[T <: Set[B], B <: Any]: (T, T) => Boolean = (a, b) => a.subsetOf(b)
  def subsetOfTup[T <: Set[(B, B)], B <: Any]: (T, T) => Boolean = (a, b) => a.subsetOf(b)

  def assertHelper[T](con1: T, con2: T,
      expr: (T, T) => Boolean = (a: T, b: T) => a == b, exprMsg: String = "equal to"): Unit = {
    assert(expr(con1, con2), s"$con1 is not $exprMsg $con2 as expected")
  }
}
