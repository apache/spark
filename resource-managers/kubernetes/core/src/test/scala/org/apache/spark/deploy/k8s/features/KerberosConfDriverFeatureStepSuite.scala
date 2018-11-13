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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, SecretBuilder}
import org.apache.hadoop.security.UserGroupInformation
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.{any, eq => Eq}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.HadoopConfSpec
import org.apache.spark.deploy.k8s.features.hadooputils.{HadoopBootstrapUtil, HadoopKerberosLogin, KerberosConfigSpec}
import org.apache.spark.deploy.k8s.security.KubernetesHadoopDelegationTokenManager

class KerberosConfDriverFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val newCMapName = "HCONF_MAP_NAME"
  private val newKCMapName = "KRB_MAP_NAME"
  private val sparkPod = SparkPod.initialPod()

  private val newDTSecret = new SecretBuilder()
    .withData(Map("fake" -> "data").asJava)
    .build()

  private val newCMap = new ConfigMapBuilder()
    .withNewMetadata()
      .withName("hCMap")
      .endMetadata()
    .build()

  private val krbMap = new ConfigMapBuilder()
    .withNewMetadata()
    .withName("kCMap")
    .endMetadata()
    .build()

  private val dtSecretName = "DT_SECRET_NAME"
  private val dtSecretKey = "DT_SECRET_KEY"
  private val jobUserName = "JOB_USER_NAME"

  @Mock
  private var kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf] = _

  @Mock
  private var hadoopBootstrapUtil: HadoopBootstrapUtil = _

  @Mock
  private var hadoopKerberosLogin: HadoopKerberosLogin = _

  @Mock
  private var tokenManager: KubernetesHadoopDelegationTokenManager = _

  @Mock
  private var ugi: UserGroupInformation = _

  @Mock
  private var hadoopFiles: Seq[File] = _

  before {
    MockitoAnnotations.initMocks(this)
    when(tokenManager.getCurrentUser).thenReturn(ugi)
    when(ugi.getShortUserName).thenReturn(jobUserName)
    when(kubernetesConf.hadoopConfigMapName).thenReturn(newCMapName)
    when(kubernetesConf.krbConfigMapName).thenReturn(newKCMapName)
    when(hadoopBootstrapUtil.getHadoopConfFiles(any[String]))
      .thenReturn(hadoopFiles)
  }

  test("running HadoopBootstrapUtil without Kerberos") {
    val hConf = HadoopConfSpec(Option("HADOOP_CONF_DIR"), None)
    when(tokenManager.isSecurityEnabled).thenReturn(false)
    when(kubernetesConf.hadoopConfSpec).thenReturn(Some(hConf))
    val sparkConf = new SparkConf(false)
    when(kubernetesConf.sparkConf).thenReturn(sparkConf)
    when(hadoopBootstrapUtil.bootstrapHadoopConfDir(
      Eq(Option("HADOOP_CONF_DIR")),
      Eq(Some(newCMapName)),
      Eq(None),
      Eq(sparkPod)
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.hadoopConfBootPod(
          invocation.getArgumentAt(3, classOf[SparkPod]))
      }
    })
    when(hadoopBootstrapUtil.buildHadoopConfigMap(any[String], Eq(hadoopFiles)))
      .thenReturn(newCMap)
    when(hadoopBootstrapUtil.bootstrapSparkUserPod(
      Eq(jobUserName),
      any[SparkPod]
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.userBootPod(invocation.getArgumentAt(1, classOf[SparkPod]))
      }
    })

    val kConfStep = new KerberosConfDriverFeatureStep(kubernetesConf,
      hadoopBootstrapUtil,
      hadoopKerberosLogin,
      tokenManager)
    val pod = kConfStep.configurePod(sparkPod)
    KubernetesFeaturesTestUtils.podHasLabels(pod.pod,
      Map("bootstrap-hconf" -> "true", "bootstrap-user" -> "true"))
    val expectedProps = Map(KERBEROS_SPARK_USER_NAME -> jobUserName,
      HADOOP_CONFIG_MAP_NAME -> newCMapName)
    assert(kConfStep.getAdditionalPodSystemProperties() === expectedProps,
      s"${kConfStep.getAdditionalPodSystemProperties()} is not equal to $expectedProps")
    assert(kConfStep.getAdditionalKubernetesResources() === List(newCMap),
      s"${kConfStep.getAdditionalKubernetesResources()} is not equal to ${List(newCMap)}")
  }

  test("running HadoopBootstrapUtil and HadoopKerberosLogin with" +
    " no Keytab and no Secret (krb5.conf path) (HADOOP_CONF_DIR env)") {
    val krbFileName = "KRB_FILE_NAME"
    val hConf = HadoopConfSpec(Option("HADOOP_CONF_DIR"), None)
    when(tokenManager.isSecurityEnabled).thenReturn(true)
    when(kubernetesConf.hadoopConfSpec).thenReturn(Some(hConf))
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_KERBEROS_KRB5_FILE, krbFileName)
    when(kubernetesConf.sparkConf).thenReturn(sparkConf)
    when(hadoopBootstrapUtil.bootstrapHadoopConfDir(
      Eq(Option("HADOOP_CONF_DIR")),
      Eq(Some(newCMapName)),
      Eq(None),
      Eq(sparkPod)
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.hadoopConfBootPod(
          invocation.getArgumentAt(3, classOf[SparkPod]))
      }
    })
    when(hadoopBootstrapUtil.buildHadoopConfigMap(any[String], Eq(hadoopFiles)))
        .thenReturn(newCMap)
    when(hadoopBootstrapUtil.buildkrb5ConfigMap(Eq(newKCMapName), Eq(krbFileName)))
        .thenReturn(krbMap)
    when(hadoopKerberosLogin.buildSpec(
      Eq(sparkConf), any[String], any[KubernetesHadoopDelegationTokenManager]))
      .thenReturn(KerberosConfigSpec(
        Some(newDTSecret),
        s"$dtSecretName-1",
        s"$dtSecretKey-1",
        jobUserName))
    when(hadoopBootstrapUtil.bootstrapKerberosPod(
      Eq(s"$dtSecretName-1"),
      Eq(s"$dtSecretKey-1"),
      Eq(jobUserName),
      Eq(Some(krbFileName)),
      Eq(Some(newKCMapName)),
      Eq(None),
      any[SparkPod]
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.krbBootPod(invocation.getArgumentAt(6, classOf[SparkPod]))
      }
    })

    val kConfStep = new KerberosConfDriverFeatureStep(kubernetesConf,
      hadoopBootstrapUtil,
      hadoopKerberosLogin,
      tokenManager)
    val pod = kConfStep.configurePod(sparkPod)
    KubernetesFeaturesTestUtils.podHasLabels(pod.pod,
      Map("bootstrap-hconf" -> "true", "bootstrap-kerberos" -> "true"))
    val expectedProps = Map(KERBEROS_DT_SECRET_NAME -> s"$dtSecretName-1",
      KERBEROS_DT_SECRET_KEY -> s"$dtSecretKey-1",
      KERBEROS_SPARK_USER_NAME -> jobUserName,
      KRB5_CONFIG_MAP_NAME -> newKCMapName,
      HADOOP_CONFIG_MAP_NAME -> newCMapName)
    assert(kConfStep.getAdditionalPodSystemProperties() === expectedProps,
      s"${kConfStep.getAdditionalPodSystemProperties()} is not equal to $expectedProps")
    assert(kConfStep.getAdditionalKubernetesResources() === List(newCMap, krbMap, newDTSecret),
      s"${kConfStep.getAdditionalKubernetesResources()} is not equal to" +
        s"${List(newCMap, krbMap, newDTSecret)}")
  }

  test("running HadoopBootstrapUtil and HadoopKerberosLogin with" +
    "Keytab (krb5.conf cmap) (HADOOP_CONF_DIR env)") {
    val krbConfName = "KRB_CMAP_NAME"
    val hConf = HadoopConfSpec(Option("HADOOP_CONF_DIR"), None)
    when(tokenManager.isSecurityEnabled).thenReturn(true)
    when(kubernetesConf.hadoopConfSpec).thenReturn(Some(hConf))
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP, krbConfName)
      .set(org.apache.spark.internal.config.KEYTAB, "KEYTAB")
      .set(org.apache.spark.internal.config.PRINCIPAL, "PRINCIPAL")
    when(kubernetesConf.sparkConf).thenReturn(sparkConf)
    when(hadoopBootstrapUtil.bootstrapHadoopConfDir(
      Eq(Option("HADOOP_CONF_DIR")),
      Eq(Some(newCMapName)),
      Eq(None),
      Eq(sparkPod)
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.hadoopConfBootPod(
          invocation.getArgumentAt(3, classOf[SparkPod]))
      }
    })
    when(hadoopBootstrapUtil.buildHadoopConfigMap(any[String], Eq(hadoopFiles)))
      .thenReturn(newCMap)
    when(hadoopKerberosLogin.buildSpec(
      Eq(sparkConf), any[String], any[KubernetesHadoopDelegationTokenManager]))
      .thenReturn(KerberosConfigSpec(
        Some(newDTSecret),
        s"$dtSecretName-2",
        s"$dtSecretKey-2",
        jobUserName))
    when(hadoopBootstrapUtil.bootstrapKerberosPod(
      Eq(s"$dtSecretName-2"),
      Eq(s"$dtSecretKey-2"),
      Eq(jobUserName),
      Eq(None),
      Eq(Some(newKCMapName)),
      Eq(Some(krbConfName)),
      any[SparkPod]
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.krbBootPod(invocation.getArgumentAt(6, classOf[SparkPod]))
      }
    })

    val kConfStep = new KerberosConfDriverFeatureStep(kubernetesConf,
      hadoopBootstrapUtil,
      hadoopKerberosLogin,
      tokenManager)
    val pod = kConfStep.configurePod(sparkPod)
    KubernetesFeaturesTestUtils.podHasLabels(pod.pod,
      Map("bootstrap-hconf" -> "true", "bootstrap-kerberos" -> "true"))
    val expectedProps = Map(KERBEROS_DT_SECRET_NAME -> s"$dtSecretName-2",
      KERBEROS_DT_SECRET_KEY -> s"$dtSecretKey-2",
      KERBEROS_SPARK_USER_NAME -> jobUserName,
      KRB5_CONFIG_MAP_NAME -> krbConfName,
      HADOOP_CONFIG_MAP_NAME -> newCMapName)
    assert(kConfStep.getAdditionalPodSystemProperties() === expectedProps,
      s"${kConfStep.getAdditionalPodSystemProperties()} is not equal to $expectedProps")
    assert(kConfStep.getAdditionalKubernetesResources() === List(newCMap, newDTSecret),
      s"${kConfStep.getAdditionalKubernetesResources()} is not equal to" +
        s"${List(newCMap, newDTSecret)}")
  }

  test("running HadoopBootstrapUtil with Secrets (krb5.conf cmap) (HADOOP_CONF_DIR cmap)") {
    val krbConfName = "KRB_CMAP_NAME"
    val hConfName = "HCONF_CMAP_NAME"
    val hConf = HadoopConfSpec(None, Some(hConfName))
    when(tokenManager.isSecurityEnabled).thenReturn(true)
    when(kubernetesConf.hadoopConfSpec).thenReturn(Some(hConf))
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP, krbConfName)
      .set(KUBERNETES_KERBEROS_DT_SECRET_NAME, s"$dtSecretName-3")
      .set(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY, s"$dtSecretKey-3")
    when(kubernetesConf.sparkConf).thenReturn(sparkConf)
    when(hadoopBootstrapUtil.bootstrapHadoopConfDir(
      Eq(None),
      Eq(None),
      Eq(Some(hConfName)),
      Eq(sparkPod)
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.hadoopConfBootPod(
          invocation.getArgumentAt(3, classOf[SparkPod]))
      }
    })
    when(hadoopBootstrapUtil.buildHadoopConfigMap(any[String], Eq(hadoopFiles)))
      .thenReturn(newCMap)
    when(hadoopKerberosLogin.buildSpec(
      Eq(sparkConf), any[String], any[KubernetesHadoopDelegationTokenManager]))
      .thenReturn(KerberosConfigSpec(
        None,
        s"$dtSecretName-3",
        s"$dtSecretKey-3",
        jobUserName))
    when(hadoopBootstrapUtil.bootstrapKerberosPod(
      Eq(s"$dtSecretName-3"),
      Eq(s"$dtSecretKey-3"),
      Eq(jobUserName),
      Eq(None),
      Eq(Some(newKCMapName)),
      Eq(Some(krbConfName)),
      any[SparkPod]
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        KubernetesFeaturesTestUtils.krbBootPod(invocation.getArgumentAt(6, classOf[SparkPod]))
      }
    })

    val kConfStep = new KerberosConfDriverFeatureStep(kubernetesConf,
      hadoopBootstrapUtil,
      hadoopKerberosLogin,
      tokenManager)
    val pod = kConfStep.configurePod(sparkPod)
    KubernetesFeaturesTestUtils.podHasLabels(pod.pod,
      Map("bootstrap-hconf" -> "true"))
    val expectedProps = Map(KERBEROS_DT_SECRET_NAME -> s"$dtSecretName-3",
      KERBEROS_DT_SECRET_KEY -> s"$dtSecretKey-3",
      KERBEROS_SPARK_USER_NAME -> jobUserName,
      KRB5_CONFIG_MAP_NAME -> krbConfName,
      HADOOP_CONFIG_MAP_NAME -> hConfName)
    assert(kConfStep.getAdditionalPodSystemProperties() === expectedProps,
      s"${kConfStep.getAdditionalPodSystemProperties()} is not equal to $expectedProps")
    assert(kConfStep.getAdditionalKubernetesResources().isEmpty)
  }
}
