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

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{ConfigMap, Secret}

import org.apache.spark.{SparkConf, SparkFunSuite}
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
    val conf = createConf(
      new SparkConf(false).set(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP, configMap))
    val step = new KerberosConfDriverFeatureStep(conf)

    checkPodForKrbConf(step.configurePod(SparkPod.initialPod()), configMap)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  test("create krb5.conf config map if local config provided") {
    val krbConf = File.createTempFile("krb5", ".conf", tmpDir)
    Files.write("some data", krbConf, UTF_8)

    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_KERBEROS_KRB5_FILE, krbConf.getAbsolutePath())
    val conf = createConf(sparkConf)

    val step = new KerberosConfDriverFeatureStep(conf)

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
    val conf = createConf(sparkConf)
    val step = new KerberosConfDriverFeatureStep(conf)

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
    val conf = createConf(sparkConf)
    val step = new KerberosConfDriverFeatureStep(conf)

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

  private def createConf(conf: SparkConf): KubernetesConf[_] = {
    KubernetesConf(
      conf,
      KubernetesDriverSpecificConf(JavaMainAppResource(None), "class", "name", Nil),
      "resource-name-prefix",
      "app-id",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      None)
  }

}
