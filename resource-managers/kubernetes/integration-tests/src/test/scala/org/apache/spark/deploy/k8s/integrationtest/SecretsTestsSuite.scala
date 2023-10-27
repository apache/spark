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
package org.apache.spark.deploy.k8s.integrationtest

import java.util.Locale

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{Pod, SecretBuilder}
import org.apache.commons.codec.binary.Base64
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._

private[spark] trait SecretsTestsSuite { k8sSuite: KubernetesSuite =>

  import SecretsTestsSuite._

  private def createTestSecret(): Unit = {
    val sb = new SecretBuilder()
    sb.withNewMetadata()
      .withName(ENV_SECRET_NAME)
      .endMetadata()
    val secUsername = Base64.encodeBase64String(ENV_SECRET_VALUE_1.getBytes())
    val secPassword = Base64.encodeBase64String(ENV_SECRET_VALUE_2.getBytes())
    val envSecretData = Map(ENV_SECRET_KEY_1 -> secUsername, ENV_SECRET_KEY_2 -> secPassword)
    sb.addToData(envSecretData.asJava)
    val envSecret = sb.build()
    val sec = kubernetesTestComponents
      .kubernetesClient
      .secrets()
      .inNamespace(kubernetesTestComponents.namespace)
      .createOrReplace(envSecret)
  }

  private def deleteTestSecret(): Unit = {
    kubernetesTestComponents
      .kubernetesClient
      .secrets()
      .inNamespace(kubernetesTestComponents.namespace)
      .withName(ENV_SECRET_NAME)
      .delete()
  }

  test("Run SparkPi with env and mount secrets.", k8sTestTag) {
    createTestSecret()
    sparkAppConf
      .set(s"spark.kubernetes.driver.secrets.$ENV_SECRET_NAME", SECRET_MOUNT_PATH)
      .set(
        s"spark.kubernetes.driver.secretKeyRef.${ENV_SECRET_KEY_1_CAP}",
        s"$ENV_SECRET_NAME:${ENV_SECRET_KEY_1}")
      .set(
        s"spark.kubernetes.driver.secretKeyRef.${ENV_SECRET_KEY_2_CAP}",
        s"$ENV_SECRET_NAME:${ENV_SECRET_KEY_2}")
      .set(s"spark.kubernetes.executor.secrets.$ENV_SECRET_NAME", SECRET_MOUNT_PATH)
      .set(s"spark.kubernetes.executor.secretKeyRef.${ENV_SECRET_KEY_1_CAP}",
        s"${ENV_SECRET_NAME}:$ENV_SECRET_KEY_1")
      .set(s"spark.kubernetes.executor.secretKeyRef.${ENV_SECRET_KEY_2_CAP}",
        s"${ENV_SECRET_NAME}:$ENV_SECRET_KEY_2")
    try {
      runSparkPiAndVerifyCompletion(
        driverPodChecker = (driverPod: Pod) => {
          doBasicDriverPodCheck(driverPod)
          checkSecrets(driverPod)
        },
        executorPodChecker = (executorPod: Pod) => {
          doBasicExecutorPodCheck(executorPod)
          checkSecrets(executorPod)
        },
        appArgs = Array("1000") // give it enough time for all execs to be visible
      )
    } finally {
      // make sure this always run
      deleteTestSecret()
    }
  }

  private def checkSecrets(pod: Pod): Unit = {
    logDebug(s"Checking secrets for ${pod}")
    // Wait for the pod to become ready & have secrets provisioned
    implicit val podName: String = pod.getMetadata.getName
    implicit val components: KubernetesTestComponents = kubernetesTestComponents
    val env = Eventually.eventually(TIMEOUT, INTERVAL) {
      logDebug(s"Checking env of ${pod.getMetadata().getName()} ....")
      val env = Utils.executeCommand("env")
      assert(!env.isEmpty)
      env
    }
    env.toString should include (s"${ENV_SECRET_KEY_1_CAP}=$ENV_SECRET_VALUE_1")
    env.toString should include (s"${ENV_SECRET_KEY_2_CAP}=$ENV_SECRET_VALUE_2")

    // Make sure our secret files are mounted correctly
    val files = Utils.executeCommand("ls", s"$SECRET_MOUNT_PATH")
    files should include (ENV_SECRET_KEY_1)
    files should include (ENV_SECRET_KEY_2)
    // Validate the contents
    val fileUsernameContents = Utils
      .executeCommand("cat", s"$SECRET_MOUNT_PATH/$ENV_SECRET_KEY_1")
    fileUsernameContents.toString.trim should equal(ENV_SECRET_VALUE_1)
    val filePasswordContents = Utils
      .executeCommand("cat", s"$SECRET_MOUNT_PATH/$ENV_SECRET_KEY_2")
    filePasswordContents.toString.trim should equal(ENV_SECRET_VALUE_2)
  }
}

private[spark] object SecretsTestsSuite {
  val ENV_SECRET_NAME = "mysecret"
  val SECRET_MOUNT_PATH = "/etc/secret"
  val ENV_SECRET_KEY_1 = "username"
  val ENV_SECRET_KEY_2 = "password"
  val ENV_SECRET_KEY_1_CAP = ENV_SECRET_KEY_1.toUpperCase(Locale.ROOT)
  val ENV_SECRET_KEY_2_CAP = ENV_SECRET_KEY_2.toUpperCase(Locale.ROOT)
  val ENV_SECRET_VALUE_1 = "secretusername"
  val ENV_SECRET_VALUE_2 = "secretpassword"
}
