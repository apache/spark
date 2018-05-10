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

import io.fabric8.kubernetes.api.model.PodBuilder

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesExecutorSpecificConf, SparkPod}
<<<<<<< HEAD
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, KubernetesFeaturesTestUtils, MountLocalFilesFeatureStep, MountSecretsFeatureStep}
||||||| merged common ancestors
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, KubernetesFeaturesTestUtils, MountSecretsFeatureStep}
=======
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, KubernetesFeaturesTestUtils, LocalDirsFeatureStep, MountSecretsFeatureStep}
>>>>>>> apache/master

class KubernetesExecutorBuilderSuite extends SparkFunSuite {
  private val BASIC_STEP_TYPE = "basic"
  private val SECRETS_STEP_TYPE = "mount-secrets"
<<<<<<< HEAD
  private val MOUNT_LOCAL_FILES_STEP_TYPE = "mount-local-files"
||||||| merged common ancestors
=======
  private val LOCAL_DIRS_STEP_TYPE = "local-dirs"
>>>>>>> apache/master

  private val basicFeatureStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    BASIC_STEP_TYPE, classOf[BasicExecutorFeatureStep])
  private val mountSecretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    SECRETS_STEP_TYPE, classOf[MountSecretsFeatureStep])
<<<<<<< HEAD
  private val mountLocalFilesStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    MOUNT_LOCAL_FILES_STEP_TYPE, classOf[MountLocalFilesFeatureStep])
||||||| merged common ancestors
=======
  private val localDirsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    LOCAL_DIRS_STEP_TYPE, classOf[LocalDirsFeatureStep])
>>>>>>> apache/master

  private val builderUnderTest = new KubernetesExecutorBuilder(
    _ => basicFeatureStep,
<<<<<<< HEAD
    _ => mountSecretsStep,
    _ => mountLocalFilesStep)
||||||| merged common ancestors
    _ => mountSecretsStep)
=======
    _ => mountSecretsStep,
    _ => localDirsStep)
>>>>>>> apache/master

  test("Basic steps are consistently applied.") {
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", new PodBuilder().build()),
      "prefix",
      "appId",
      None,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf), BASIC_STEP_TYPE, LOCAL_DIRS_STEP_TYPE)
  }

  test("Apply secrets step if secrets are present.") {
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", new PodBuilder().build()),
      "prefix",
      "appId",
      None,
      Map.empty,
      Map.empty,
      Map("secret" -> "secretMountPath"),
      Map.empty)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      SECRETS_STEP_TYPE)
  }

  test("Apply mount local files step if secret name is present.") {
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", new PodBuilder().build()),
      "prefix",
      "appId",
      Some("secret"),
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      MOUNT_LOCAL_FILES_STEP_TYPE)
  }

  private def validateStepTypesApplied(resolvedPod: SparkPod, stepTypes: String*): Unit = {
    assert(resolvedPod.pod.getMetadata.getLabels.size === stepTypes.size)
    stepTypes.foreach { stepType =>
      assert(resolvedPod.pod.getMetadata.getLabels.get(stepType) === stepType)
    }
  }
}
