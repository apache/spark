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
package org.apache.spark.scheduler.cluster.k8s

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Config => _, _}
import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.Mockito.{mock, never, verify}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.deploy.k8s.submit.PodBuilderSuiteUtils
import org.apache.spark.util.SparkConfWithEnv

class KubernetesExecutorBuilderSuite extends SparkFunSuite {
  private val BASIC_STEP_TYPE = "basic"
  private val SECRETS_STEP_TYPE = "mount-secrets"
  private val ENV_SECRETS_STEP_TYPE = "env-secrets"
  private val LOCAL_DIRS_STEP_TYPE = "local-dirs"
  private val DELEGATION_TOKEN_CONF_STEP_TYPE = "delegation-token-step"
  private val MOUNT_VOLUMES_STEP_TYPE = "mount-volumes"

  private val secMgr = new SecurityManager(new SparkConf(false))

  private val basicFeatureStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    BASIC_STEP_TYPE, classOf[BasicExecutorFeatureStep])
  private val mountSecretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    SECRETS_STEP_TYPE, classOf[MountSecretsFeatureStep])
  private val envSecretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    ENV_SECRETS_STEP_TYPE, classOf[EnvSecretsFeatureStep])
  private val localDirsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    LOCAL_DIRS_STEP_TYPE, classOf[LocalDirsFeatureStep])
  private val mountVolumesStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    MOUNT_VOLUMES_STEP_TYPE, classOf[MountVolumesFeatureStep])

  private val ALWAYS_ON_STEPS = Seq(BASIC_STEP_TYPE, LOCAL_DIRS_STEP_TYPE)

  private val builderUnderTest = new KubernetesExecutorBuilder(
    (_, _) => basicFeatureStep,
    _ => mountSecretsStep,
    _ => envSecretsStep,
    _ => localDirsStep,
    _ => mountVolumesStep)

  test("Basic steps are consistently applied.") {
    val conf = KubernetesTestConf.createExecutorConf()
    validateStepTypesApplied(builderUnderTest.buildFromFeatures(conf, secMgr))
  }

  test("Apply secrets step if secrets are present.") {
    val conf = KubernetesTestConf.createExecutorConf(
      secretEnvNamesToKeyRefs = Map("secret-name" -> "secret-key"),
      secretNamesToMountPaths = Map("secret" -> "secretMountPath"))
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf, secMgr),
      SECRETS_STEP_TYPE,
      ENV_SECRETS_STEP_TYPE)
  }

  test("Apply volumes step if mounts are present.") {
    val volumeSpec = KubernetesVolumeSpec(
      "volume",
      "/tmp",
      "",
      false,
      KubernetesHostPathVolumeConf("/checkpoint"))
    val conf = KubernetesTestConf.createExecutorConf(
      volumes = Seq(volumeSpec))
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf, secMgr),
      MOUNT_VOLUMES_STEP_TYPE)
  }

  private def validateStepTypesApplied(resolvedPod: SparkPod, stepTypes: String*): Unit = {
    val validSteps = (stepTypes ++ ALWAYS_ON_STEPS).toSet
    assert(resolvedPod.pod.getMetadata.getLabels.keySet.asScala === validSteps)
  }

  test("Starts with empty executor pod if template is not specified") {
    val kubernetesClient = mock(classOf[KubernetesClient])
    val executorBuilder = KubernetesExecutorBuilder.apply(kubernetesClient, new SparkConf())
    verify(kubernetesClient, never()).pods()
  }

  test("Starts with executor template if specified") {
    val kubernetesClient = PodBuilderSuiteUtils.loadingMockKubernetesClient()
    val sparkConf = new SparkConf(false)
      .set("spark.driver.host", "https://driver.host.com")
      .set(Config.CONTAINER_IMAGE, "spark-executor:latest")
      .set(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE, "template-file.yaml")
    val kubernetesConf = KubernetesTestConf.createExecutorConf(
      sparkConf = sparkConf,
      driverPod = Some(new PodBuilder()
        .withNewMetadata()
          .withName("driver")
          .endMetadata()
        .build()))
    val sparkPod = KubernetesExecutorBuilder(kubernetesClient, sparkConf)
      .buildFromFeatures(kubernetesConf, secMgr)
    PodBuilderSuiteUtils.verifyPodWithSupportedFeatures(sparkPod)
  }
}
