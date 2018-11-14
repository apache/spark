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

import io.fabric8.kubernetes.api.model.Secret

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource
import org.apache.spark.internal.config._

class DelegationTokenFeatureStepSuite extends SparkFunSuite {

  import KubernetesFeaturesTestUtils._
  import SecretVolumeUtils._

  test("mount delegation tokens if provided") {
    val dtSecret = "tokenSecret"
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_KERBEROS_DT_SECRET_NAME, dtSecret)
      .set(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY, "dtokens")
    val conf = createConf(sparkConf)
    val step = new DelegationTokenFeatureStep(conf, true)

    checkPodForTokens(step.configurePod(SparkPod.initialPod()), dtSecret)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  test("do nothing if a keytab is provided") {
    val sparkConf = new SparkConf(false).set(KEYTAB, "fakeKeytab")
    val conf = createConf(sparkConf)
    val step = new DelegationTokenFeatureStep(conf, true)

    val initial = SparkPod.initialPod()
    assert(step.configurePod(initial) === initial)

    assert(step.getAdditionalPodSystemProperties().isEmpty)
    assert(step.getAdditionalKubernetesResources().isEmpty)
  }

  test("create delegation tokens if needed") {
    val conf = createConf(new SparkConf(false))
    val step = new DelegationTokenFeatureStep(conf, true) {
      override def isSecurityEnabled: Boolean = true
      override def createDelegationTokens(): Array[Byte] = Array(0x4, 0x2)
    }

    val dtSecret = filter[Secret](step.getAdditionalKubernetesResources())
    assert(dtSecret.size === 1)
    checkPodForTokens(step.configurePod(SparkPod.initialPod()),
      dtSecret.head.getMetadata().getName())

    assert(step.getAdditionalPodSystemProperties().isEmpty)
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
