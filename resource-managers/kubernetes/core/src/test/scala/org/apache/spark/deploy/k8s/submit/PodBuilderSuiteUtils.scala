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
package org.apache.spark.deploy.k8s.submit

import java.io.File

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, PodResource}
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.FlatSpec
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.SparkPod

object PodBuilderSuiteUtils extends FlatSpec {

  def loadingMockKubernetesClient(pod: Pod = podWithSupportedFeatures()): KubernetesClient = {
    val kubernetesClient = mock(classOf[KubernetesClient])
    val pods =
      mock(classOf[MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]])
    val podResource = mock(classOf[PodResource[Pod, DoneablePod]])
    when(kubernetesClient.pods()).thenReturn(pods)
    when(pods.load(any(classOf[File]))).thenReturn(podResource)
    when(podResource.get()).thenReturn(pod)
    kubernetesClient
  }

  def verifyPodWithSupportedFeatures(pod: SparkPod): Unit = {
    val metadata = pod.pod.getMetadata
    assert(metadata.getLabels.containsKey("test-label-key"))
    assert(metadata.getAnnotations.containsKey("test-annotation-key"))
    assert(metadata.getNamespace === "namespace")
    assert(metadata.getOwnerReferences.asScala.exists(_.getName == "owner-reference"))
    val spec = pod.pod.getSpec
    assert(!spec.getContainers.asScala.exists(_.getName == "executor-container"))
    assert(spec.getDnsPolicy === "dns-policy")
    assert(spec.getHostAliases.asScala.exists(_.getHostnames.asScala.exists(_ == "hostname")))
    assert(spec.getImagePullSecrets.asScala.exists(_.getName == "local-reference"))
    assert(spec.getInitContainers.asScala.exists(_.getName == "init-container"))
    assert(spec.getNodeName == "node-name")
    assert(spec.getNodeSelector.get("node-selector-key") === "node-selector-value")
    assert(spec.getSchedulerName === "scheduler")
    assert(spec.getSecurityContext.getRunAsUser === 1000L)
    assert(spec.getServiceAccount === "service-account")
    assert(spec.getSubdomain === "subdomain")
    assert(spec.getTolerations.asScala.exists(_.getKey == "toleration-key"))
    assert(spec.getVolumes.asScala.exists(_.getName == "test-volume"))
    val container = pod.container
    assert(container.getName === "executor-container")
    assert(container.getArgs.contains("arg"))
    assert(container.getCommand.equals(List("command").asJava))
    assert(container.getEnv.asScala.exists(_.getName == "env-key"))
    assert(container.getResources.getLimits.get("gpu") ===
      new QuantityBuilder().withAmount("1").build())
    assert(container.getSecurityContext.getRunAsNonRoot)
    assert(container.getStdin)
    assert(container.getTerminationMessagePath === "termination-message-path")
    assert(container.getTerminationMessagePolicy === "termination-message-policy")
    assert(pod.container.getVolumeMounts.asScala.exists(_.getName == "test-volume"))

  }


  def podWithSupportedFeatures(): Pod = new PodBuilder()
        .withNewMetadata()
          .addToLabels("test-label-key", "test-label-value")
          .addToAnnotations("test-annotation-key", "test-annotation-value")
          .withNamespace("namespace")
          .addNewOwnerReference()
            .withController(true)
            .withName("owner-reference")
            .endOwnerReference()
          .endMetadata()
        .withNewSpec()
          .withDnsPolicy("dns-policy")
          .withHostAliases(new HostAliasBuilder().withHostnames("hostname").build())
          .withImagePullSecrets(
            new LocalObjectReferenceBuilder().withName("local-reference").build())
          .withInitContainers(new ContainerBuilder().withName("init-container").build())
          .withNodeName("node-name")
          .withNodeSelector(Map("node-selector-key" -> "node-selector-value").asJava)
          .withSchedulerName("scheduler")
          .withNewSecurityContext()
            .withRunAsUser(1000L)
            .endSecurityContext()
          .withServiceAccount("service-account")
          .withSubdomain("subdomain")
          .withTolerations(new TolerationBuilder()
            .withKey("toleration-key")
            .withOperator("Equal")
            .withEffect("NoSchedule")
            .build())
          .addNewVolume()
            .withNewHostPath()
            .withPath("/test")
            .endHostPath()
            .withName("test-volume")
            .endVolume()
          .addNewContainer()
            .withArgs("arg")
            .withCommand("command")
            .addNewEnv()
              .withName("env-key")
              .withValue("env-value")
              .endEnv()
            .withImagePullPolicy("Always")
            .withName("executor-container")
            .withNewResources()
              .withLimits(Map("gpu" -> new QuantityBuilder().withAmount("1").build()).asJava)
              .endResources()
            .withNewSecurityContext()
              .withRunAsNonRoot(true)
              .endSecurityContext()
            .withStdin(true)
            .withTerminationMessagePath("termination-message-path")
            .withTerminationMessagePolicy("termination-message-policy")
            .addToVolumeMounts(
              new VolumeMountBuilder()
                .withName("test-volume")
                .withMountPath("/test")
                .build())
            .endContainer()
          .endSpec()
        .build()

}
