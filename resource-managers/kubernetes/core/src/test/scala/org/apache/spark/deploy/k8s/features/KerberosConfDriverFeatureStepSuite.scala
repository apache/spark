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
import java.nio.charset.StandardCharsets.UTF_8
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{ConfigMap, Secret}
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

class KerberosConfDriverFeatureStepSuite extends SparkFunSuite {

  import KubernetesFeaturesTestUtils._
  import SecretVolumeUtils._

  private val tmpDir = Utils.createTempDir()

  test("mount krb5 config map if defined") {
    val configMap = "testConfigMap"
    val step = createStep(
      new SparkConf(false).set(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP, configMap))

    checkPodForKrbConf(step.configurePod(SparkPod.initialPod()), configMap)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
    assert(filter[ConfigMap](step.getAdditionalKubernetesResources()).isEmpty)
  }

  test("create krb5.conf config map if local config provided") {
    val krbConf = File.createTempFile("krb5", ".conf", tmpDir)
    Files.write("some data", krbConf, UTF_8)

    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_KERBEROS_KRB5_FILE, krbConf.getAbsolutePath())
    val step = createStep(sparkConf)

    val confMap = filter[ConfigMap](step.getAdditionalKubernetesResources()).head
    assert(confMap.getData().keySet().asScala === Set(krbConf.getName()))

    checkPodForKrbConf(step.configurePod(SparkPod.initialPod()), confMap.getMetadata().getName())
    assert(step.getAdditionalPodSystemProperties().isEmpty)
  }

  test("create keytab secret if client keytab file used") {
    val keytab = File.createTempFile("keytab", ".bin", tmpDir)
    Files.write("some data", keytab, UTF_8)

    val sparkConf = new SparkConf(false)
      .set(KEYTAB, keytab.getAbsolutePath())
      .set(PRINCIPAL, "alice")
    val step = createStep(sparkConf)

    val pod = step.configurePod(SparkPod.initialPod())
    assert(podHasVolume(pod.pod, KERBEROS_KEYTAB_VOLUME))
    assert(containerHasVolume(pod.container, KERBEROS_KEYTAB_VOLUME, KERBEROS_KEYTAB_MOUNT_POINT))

    assert(step.getAdditionalPodSystemProperties().keys === Set(KEYTAB.key))

    val secret = filter[Secret](step.getAdditionalKubernetesResources()).head
    assert(secret.getData().keySet().asScala === Set(keytab.getName()))
  }

  test("do nothing if container-local keytab used") {
    val sparkConf = new SparkConf(false)
      .set(KEYTAB, "local:/my.keytab")
      .set(PRINCIPAL, "alice")
    val step = createStep(sparkConf)

    val initial = SparkPod.initialPod()
    assert(step.configurePod(initial) === initial)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  test("mount delegation tokens if provided") {
    val dtSecret = "tokenSecret"
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_KERBEROS_DT_SECRET_NAME, dtSecret)
      .set(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY, "dtokens")
    val step = createStep(sparkConf)

    checkPodForTokens(step.configurePod(SparkPod.initialPod()), dtSecret)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  test("create delegation tokens if needed") {
    // Since HadoopDelegationTokenManager does not create any tokens without proper configs and
    // services, start with a test user that already has some tokens that will just be piped
    // through to the driver.
    val testUser = UserGroupInformation.createUserForTesting("k8s", Array())
    testUser.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val creds = testUser.getCredentials()
        creds.addSecretKey(new Text("K8S_TEST_KEY"), Array[Byte](0x4, 0x2))
        testUser.addCredentials(creds)

        val tokens = SparkHadoopUtil.get.serialize(creds)

        val step = createStep(new SparkConf(false))

        val dtSecret = filter[Secret](step.getAdditionalKubernetesResources()).head
        assert(dtSecret.getData().get(KERBEROS_SECRET_KEY) === Base64.encodeBase64String(tokens))

        checkPodForTokens(step.configurePod(SparkPod.initialPod()),
          dtSecret.getMetadata().getName())

        assert(step.getAdditionalPodSystemProperties().isEmpty)
      }
    })
  }

  test("do nothing if no config and no tokens") {
    val step = createStep(new SparkConf(false))
    val initial = SparkPod.initialPod()
    assert(step.configurePod(initial) === initial)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  private def checkPodForKrbConf(pod: SparkPod, confMapName: String): Unit = {
    val podVolume = pod.pod.getSpec().getVolumes().asScala.find(_.getName() == KRB_FILE_VOLUME)
    assert(podVolume.isDefined)
    assert(containerHasVolume(pod.container, KRB_FILE_VOLUME, KRB_FILE_DIR_PATH + "/krb5.conf"))
    assert(podVolume.get.getConfigMap().getName() === confMapName)
  }

  private def checkPodForTokens(pod: SparkPod, dtSecretName: String): Unit = {
    val podVolume = pod.pod.getSpec().getVolumes().asScala
      .find(_.getName() == SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
    assert(podVolume.isDefined)
    assert(containerHasVolume(pod.container, SPARK_APP_HADOOP_SECRET_VOLUME_NAME,
      SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR))
    assert(containerHasEnvVar(pod.container, ENV_HADOOP_TOKEN_FILE_LOCATION))
    assert(podVolume.get.getSecret().getSecretName() === dtSecretName)
  }

  private def createStep(conf: SparkConf): KerberosConfDriverFeatureStep = {
    val kconf = KubernetesTestConf.createDriverConf(sparkConf = conf)
    new KerberosConfDriverFeatureStep(kconf)
  }

}
