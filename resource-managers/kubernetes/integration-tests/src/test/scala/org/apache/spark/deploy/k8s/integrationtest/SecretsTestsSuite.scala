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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Pod, SecretBuilder}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest.concurrent.Eventually

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
      .createOrReplace(envSecret)
  }

  private def deleteTestSecret(): Unit = {
    kubernetesTestComponents
      .kubernetesClient
      .secrets()
      .withName(ENV_SECRET_NAME)
      .delete()
  }

  test("Run SparkPi with env and mount secrets.", k8sTestTag) {
    createTestSecret()
    sparkAppConf
      .set(s"spark.kubernetes.driver.secrets.$ENV_SECRET_NAME", SECRET_MOUNT_PATH)
      .set(s"spark.kubernetes.driver.secretKeyRef.USERNAME", s"$ENV_SECRET_NAME:username")
      .set(s"spark.kubernetes.driver.secretKeyRef.PASSWORD", s"$ENV_SECRET_NAME:password")
      .set(s"spark.kubernetes.executor.secrets.$ENV_SECRET_NAME", SECRET_MOUNT_PATH)
      .set(s"spark.kubernetes.executor.secretKeyRef.USERNAME", s"$ENV_SECRET_NAME:username")
      .set(s"spark.kubernetes.executor.secretKeyRef.PASSWORD", s"$ENV_SECRET_NAME:password")
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
    Eventually.eventually(TIMEOUT, INTERVAL) {
      implicit val podName: String = pod.getMetadata.getName
      implicit val components: KubernetesTestComponents = kubernetesTestComponents
      val env = Utils.executeCommand("env")
      assert(env.toString.contains(ENV_SECRET_VALUE_1))
      assert(env.toString.contains(ENV_SECRET_VALUE_2))
      val fileUsernameContents = Utils
        .executeCommand("cat", s"$SECRET_MOUNT_PATH/$ENV_SECRET_KEY_1")
      val filePasswordContents = Utils
        .executeCommand("cat", s"$SECRET_MOUNT_PATH/$ENV_SECRET_KEY_2")
      assert(fileUsernameContents.toString.trim.equals(ENV_SECRET_VALUE_1))
      assert(filePasswordContents.toString.trim.equals(ENV_SECRET_VALUE_2))
    }
  }
}

private[spark] object SecretsTestsSuite {
  val ENV_SECRET_NAME = "mysecret"
  val SECRET_MOUNT_PATH = "/etc/secret"
  val ENV_SECRET_KEY_1 = "username"
  val ENV_SECRET_KEY_2 = "password"
  val ENV_SECRET_VALUE_1 = "secretusername"
  val ENV_SECRET_VALUE_2 = "secretpassword"
}
