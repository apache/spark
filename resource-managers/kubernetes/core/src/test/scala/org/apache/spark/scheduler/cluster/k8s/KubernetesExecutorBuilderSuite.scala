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
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features._

class KubernetesExecutorBuilderSuite extends SparkFunSuite {
  private val BASIC_STEP_TYPE = "basic"
  private val SECRETS_STEP_TYPE = "mount-secrets"
  private val ENV_SECRETS_STEP_TYPE = "env-secrets"
  private val LOCAL_DIRS_STEP_TYPE = "local-dirs"
  private val HADOOP_CONF_STEP_TYPE = "hadoop-conf-step"
  private val HADOOP_SPARK_USER_STEP_TYPE = "hadoop-spark-user"
  private val KERBEROS_CONF_STEP_TYPE = "kerberos-step"
  private val MOUNT_VOLUMES_STEP_TYPE = "mount-volumes"

  private val basicFeatureStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    BASIC_STEP_TYPE, classOf[BasicExecutorFeatureStep])
  private val mountSecretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    SECRETS_STEP_TYPE, classOf[MountSecretsFeatureStep])
  private val envSecretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    ENV_SECRETS_STEP_TYPE, classOf[EnvSecretsFeatureStep])
  private val localDirsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    LOCAL_DIRS_STEP_TYPE, classOf[LocalDirsFeatureStep])
  private val hadoopConfStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    HADOOP_CONF_STEP_TYPE, classOf[HadoopConfExecutorFeatureStep])
  private val hadoopSparkUser = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    HADOOP_SPARK_USER_STEP_TYPE, classOf[HadoopSparkUserExecutorFeatureStep])
  private val kerberosConf = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    KERBEROS_CONF_STEP_TYPE, classOf[KerberosConfExecutorFeatureStep])
  private val mountVolumesStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    MOUNT_VOLUMES_STEP_TYPE, classOf[MountVolumesFeatureStep])

  private val builderUnderTest = new KubernetesExecutorBuilder(
    _ => basicFeatureStep,
    _ => mountSecretsStep,
    _ => envSecretsStep,
    _ => localDirsStep,
    _ => mountVolumesStep,
    _ => hadoopConfStep,
    _ => kerberosConf,
    _ => hadoopSparkUser)

  test("Basic steps are consistently applied.") {
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", Some(new PodBuilder().build())),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      Seq.empty[String],
      None)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf), BASIC_STEP_TYPE, LOCAL_DIRS_STEP_TYPE)
  }

  test("Apply secrets step if secrets are present.") {
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", Some(new PodBuilder().build())),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map("secret" -> "secretMountPath"),
      Map("secret-name" -> "secret-key"),
      Map.empty,
      Nil,
      Seq.empty[String],
      None)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      SECRETS_STEP_TYPE,
      ENV_SECRETS_STEP_TYPE)
  }

  test("Apply volumes step if mounts are present.") {
    val volumeSpec = KubernetesVolumeSpec(
      "volume",
      "/tmp",
      false,
      KubernetesHostPathVolumeConf("/checkpoint"))
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", Some(new PodBuilder().build())),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      volumeSpec :: Nil,
      Seq.empty[String],
      None)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      MOUNT_VOLUMES_STEP_TYPE)
  }

  test("Apply basicHadoop step if HADOOP_CONF_DIR is defined") {
    // HADOOP_DELEGATION_TOKEN
    val HADOOP_CREDS_PREFIX = "spark.security.credentials."
    val HADOOPFS_PROVIDER = s"$HADOOP_CREDS_PREFIX.hadoopfs.enabled"
    val conf = KubernetesConf(
      new SparkConf(false)
        .set(HADOOP_CONFIG_MAP_NAME, "hadoop-conf-map-name")
        .set(KRB5_CONFIG_MAP_NAME, "krb5-conf-map-name")
        .set(KERBEROS_SPARK_USER_NAME, "spark-user")
        .set(HADOOPFS_PROVIDER, "true"),
      KubernetesExecutorSpecificConf(
        "executor-id", Some(new PodBuilder().build())),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      Seq.empty[String],
      Some(HadoopConfSpec(Some("/var/hadoop-conf"), None)))
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      HADOOP_CONF_STEP_TYPE,
      HADOOP_SPARK_USER_STEP_TYPE)
  }

  test("Apply kerberos step if DT secrets created") {
    val conf = KubernetesConf(
      new SparkConf(false)
        .set(HADOOP_CONFIG_MAP_NAME, "hadoop-conf-map-name")
        .set(KRB5_CONFIG_MAP_NAME, "krb5-conf-map-name")
        .set(KERBEROS_SPARK_USER_NAME, "spark-user")
        .set(KERBEROS_DT_SECRET_NAME, "dt-secret")
        .set(KERBEROS_DT_SECRET_KEY, "dt-key"),
      KubernetesExecutorSpecificConf(
        "executor-id", Some(new PodBuilder().build())),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      Seq.empty[String],
      Some(HadoopConfSpec(None, Some("pre-defined-onfigMapName"))))
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      LOCAL_DIRS_STEP_TYPE,
      HADOOP_CONF_STEP_TYPE,
      KERBEROS_CONF_STEP_TYPE)
  }

  private def validateStepTypesApplied(resolvedPod: SparkPod, stepTypes: String*): Unit = {
    assert(resolvedPod.pod.getMetadata.getLabels.size === stepTypes.size)
    stepTypes.foreach { stepType =>
      assert(resolvedPod.pod.getMetadata.getLabels.get(stepType) === stepType)
    }
  }
}
