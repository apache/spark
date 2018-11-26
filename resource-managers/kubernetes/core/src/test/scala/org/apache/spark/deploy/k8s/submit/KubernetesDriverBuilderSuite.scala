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

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.Mockito._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config.{CONTAINER_IMAGE, KUBERNETES_DRIVER_PODTEMPLATE_FILE, KUBERNETES_EXECUTOR_PODTEMPLATE_FILE}
import org.apache.spark.deploy.k8s.features._

class KubernetesDriverBuilderSuite extends SparkFunSuite {

  private val BASIC_STEP_TYPE = "basic"
  private val CREDENTIALS_STEP_TYPE = "credentials"
  private val SERVICE_STEP_TYPE = "service"
  private val LOCAL_DIRS_STEP_TYPE = "local-dirs"
  private val SECRETS_STEP_TYPE = "mount-secrets"
  private val DRIVER_CMD_STEP_TYPE = "driver-command"
  private val ENV_SECRETS_STEP_TYPE = "env-secrets"
  private val HADOOP_GLOBAL_STEP_TYPE = "hadoop-global"
  private val MOUNT_VOLUMES_STEP_TYPE = "mount-volumes"
  private val TEMPLATE_VOLUME_STEP_TYPE = "template-volume"

  private val basicFeatureStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    BASIC_STEP_TYPE, classOf[BasicDriverFeatureStep])

  private val credentialsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    CREDENTIALS_STEP_TYPE, classOf[DriverKubernetesCredentialsFeatureStep])

  private val serviceStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    SERVICE_STEP_TYPE, classOf[DriverServiceFeatureStep])

  private val localDirsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    LOCAL_DIRS_STEP_TYPE, classOf[LocalDirsFeatureStep])

  private val secretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    SECRETS_STEP_TYPE, classOf[MountSecretsFeatureStep])

  private val driverCommandStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    DRIVER_CMD_STEP_TYPE, classOf[DriverCommandFeatureStep])

  private val envSecretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    ENV_SECRETS_STEP_TYPE, classOf[EnvSecretsFeatureStep])

  private val hadoopGlobalStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    HADOOP_GLOBAL_STEP_TYPE, classOf[KerberosConfDriverFeatureStep])

  private val mountVolumesStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    MOUNT_VOLUMES_STEP_TYPE, classOf[MountVolumesFeatureStep])

  private val templateVolumeStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    TEMPLATE_VOLUME_STEP_TYPE, classOf[PodTemplateConfigMapStep]
  )

  private val builderUnderTest: KubernetesDriverBuilder =
    new KubernetesDriverBuilder(
      _ => basicFeatureStep,
      _ => credentialsStep,
      _ => serviceStep,
      _ => secretsStep,
      _ => envSecretsStep,
      _ => localDirsStep,
      _ => mountVolumesStep,
      _ => driverCommandStep,
      _ => hadoopGlobalStep,
      _ => templateVolumeStep)

  test("Apply fundamental steps all the time.") {
    val conf = KubernetesTestConf.createDriverConf()
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      CREDENTIALS_STEP_TYPE,
      SERVICE_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      DRIVER_CMD_STEP_TYPE,
      HADOOP_GLOBAL_STEP_TYPE)
  }

  test("Apply secrets step if secrets are present.") {
    val conf = KubernetesTestConf.createDriverConf(
      secretEnvNamesToKeyRefs = Map("EnvName" -> "SecretName:secretKey"),
      secretNamesToMountPaths = Map("secret" -> "secretMountPath"))
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      CREDENTIALS_STEP_TYPE,
      SERVICE_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      SECRETS_STEP_TYPE,
      ENV_SECRETS_STEP_TYPE,
      DRIVER_CMD_STEP_TYPE,
      HADOOP_GLOBAL_STEP_TYPE)
  }

  test("Apply volumes step if mounts are present.") {
    val volumeSpec = KubernetesVolumeSpec(
      "volume",
      "/tmp",
      "",
      false,
      KubernetesHostPathVolumeConf("/path"))
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesDriverSpecificConf(
        JavaMainAppResource(None),
        "test-app",
        "main",
        Seq.empty),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      volumeSpec :: Nil,
      hadoopConfSpec = None)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      CREDENTIALS_STEP_TYPE,
      SERVICE_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      MOUNT_VOLUMES_STEP_TYPE,
      DRIVER_CMD_STEP_TYPE)
  }

  test("Apply volumes step if a mount subpath is present.") {
    val volumeSpec = KubernetesVolumeSpec(
      "volume",
      "/tmp",
      "foo",
      false,
      KubernetesHostPathVolumeConf("/path"))
    val conf = KubernetesTestConf.createDriverConf(
      volumes = Seq(volumeSpec))
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      CREDENTIALS_STEP_TYPE,
      SERVICE_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      MOUNT_VOLUMES_STEP_TYPE,
      DRIVER_CMD_STEP_TYPE,
      HADOOP_GLOBAL_STEP_TYPE)
  }

  test("Apply template volume step if executor template is present.") {
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE, "filename")
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      CREDENTIALS_STEP_TYPE,
      SERVICE_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      DRIVER_CMD_STEP_TYPE,
      HADOOP_GLOBAL_STEP_TYPE,
      TEMPLATE_VOLUME_STEP_TYPE)
  }

  private def validateStepTypesApplied(resolvedSpec: KubernetesDriverSpec, stepTypes: String*)
  : Unit = {
    val addedProperties = resolvedSpec.systemProperties
      .filter { case (k, _) => !k.startsWith("spark.") }
      .toMap
    assert(addedProperties.keys.toSet === stepTypes.toSet)
    stepTypes.foreach { stepType =>
      assert(resolvedSpec.pod.pod.getMetadata.getLabels.get(stepType) === stepType)
      assert(resolvedSpec.driverKubernetesResources.containsSlice(
        KubernetesFeaturesTestUtils.getSecretsForStepType(stepType)))
      assert(resolvedSpec.systemProperties(stepType) === stepType)
    }
  }

  test("Start with empty pod if template is not specified") {
    val kubernetesClient = mock(classOf[KubernetesClient])
    val driverBuilder = KubernetesDriverBuilder.apply(kubernetesClient, new SparkConf())
    verify(kubernetesClient, never()).pods()
  }

  test("Starts with template if specified") {
    val kubernetesClient = PodBuilderSuiteUtils.loadingMockKubernetesClient()
    val sparkConf = new SparkConf(false)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(KUBERNETES_DRIVER_PODTEMPLATE_FILE, "template-file.yaml")
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val driverSpec = KubernetesDriverBuilder
      .apply(kubernetesClient, sparkConf)
      .buildFromFeatures(kubernetesConf)
    PodBuilderSuiteUtils.verifyPodWithSupportedFeatures(driverSpec.pod)
  }

  test("Throws on misconfigured pod template") {
    val kubernetesClient = PodBuilderSuiteUtils.loadingMockKubernetesClient(
      new PodBuilder()
        .withNewMetadata()
        .addToLabels("test-label-key", "test-label-value")
        .endMetadata()
        .build())
    val sparkConf = new SparkConf(false)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(KUBERNETES_DRIVER_PODTEMPLATE_FILE, "template-file.yaml")
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val exception = intercept[SparkException] {
      KubernetesDriverBuilder
        .apply(kubernetesClient, sparkConf)
        .buildFromFeatures(kubernetesConf)
    }
    assert(exception.getMessage.contains("Could not load pod from template file."))
  }
}
