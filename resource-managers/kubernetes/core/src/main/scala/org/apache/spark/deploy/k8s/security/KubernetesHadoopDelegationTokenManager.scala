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

package org.apache.spark.deploy.k8s.security

import java.io.{ByteArrayInputStream, DataInputStream}
import java.io.File

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens

/**
 * Adds Kubernetes-specific functionality to HadoopDelegationTokenManager.
 */
private[spark] class KubernetesHadoopDelegationTokenManager(
    _sparkConf: SparkConf,
    _hadoopConf: Configuration,
    kubernetesClient: Option[KubernetesClient])
  extends HadoopDelegationTokenManager(_sparkConf, _hadoopConf) {

  def getCurrentUser: UserGroupInformation = UserGroupInformation.getCurrentUser
  def isSecurityEnabled: Boolean = UserGroupInformation.isSecurityEnabled

  private val isTokenRenewalEnabled =
    _sparkConf.get(KUBERNETES_KERBEROS_DT_SECRET_RENEWAL)

  private val dtSecretName = _sparkConf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)

  if (isTokenRenewalEnabled) {
    require(dtSecretName.isDefined,
      "Must specify the token secret which the driver must watch for updates")
  }

  private def deserialize(credentials: Credentials, data: Array[Byte]): Unit = {
    val byteStream = new ByteArrayInputStream(data)
    val dataStream = new DataInputStream(byteStream)
    credentials.readTokenStorageStream(dataStream)
  }

  private var watch: Watch = _

  /**
   * As in HadoopDelegationTokenManager this starts the token renewer.
   * Upon start, if a principal and keytab are defined, the renewer will:
   *
    * - log in the configured principal, and set up a task to keep that user's ticket renewed
    * - obtain delegation tokens from all available providers
    * - send the tokens to the driver, if it's already registered
    * - schedule a periodic task to update the tokens when needed.
   *
   * In the case that the principal is NOT configured, one may still service a long running
   * app by enabling the KERBEROS_SECRET_RENEWER config and relying on an external service
   * to populate a secret with valid Delegation Tokens that the application will then use.
   * This is possibly via the use of a Secret watcher which the driver will leverage to
   * detect updates that happen to the secret so that it may retrieve that secret's contents
   * and send it to all expiring executors
   *
   * @return The newly logged in user, or null
   */
  override def start(): UserGroupInformation = {
    val driver = driverRef.get()
    if (isTokenRenewalEnabled &&
      kubernetesClient.isDefined && driver != null) {
      watch = kubernetesClient.get
        .secrets()
        .inNamespace(_sparkConf.get(KUBERNETES_NAMESPACE))
        .withName(dtSecretName.get)
        .watch(new Watcher[Secret] {
          override def onClose(cause: KubernetesClientException): Unit =
            logInfo("Ending the watch of DT Secret")
          override def eventReceived(action: Watcher.Action, resource: Secret): Unit = {
            action match {
              case Action.ADDED | Action.MODIFIED =>
                logInfo("Secret update")
                val dataItems = resource.getData.asScala.filterKeys(
                  _.startsWith(SECRET_DATA_ITEM_PREFIX_TOKENS)).toSeq.sorted
                val latestToken = if (dataItems.nonEmpty) Some(dataItems.max) else None
                latestToken.foreach {
                  case (_, data) =>
                    val credentials = new Credentials
                    deserialize(credentials, Base64.decodeBase64(data))
                    val tokens = SparkHadoopUtil.get.serialize(credentials)
                    driver.send(UpdateDelegationTokens(tokens))
                }
            }
          }
        })
      null
    } else {
      super.start()
    }
  }
}
