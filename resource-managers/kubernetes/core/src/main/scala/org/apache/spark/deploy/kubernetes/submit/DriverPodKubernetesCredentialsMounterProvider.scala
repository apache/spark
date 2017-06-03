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
package org.apache.spark.deploy.kubernetes.submit

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._

private[spark] trait DriverPodKubernetesCredentialsMounterProvider {

  def getDriverPodKubernetesCredentialsMounter()
      : DriverPodKubernetesCredentialsMounter
}

private[spark] class DriverPodKubernetesCredentialsMounterProviderImpl(
    sparkConf: SparkConf,
    kubernetesAppId: String)
    extends DriverPodKubernetesCredentialsMounterProvider {

  override def getDriverPodKubernetesCredentialsMounter()
      : DriverPodKubernetesCredentialsMounter = {
    val submitterLocalDriverPodKubernetesCredentials =
      new DriverPodKubernetesCredentialsProvider(sparkConf).get()
    new DriverPodKubernetesCredentialsMounterImpl(
      kubernetesAppId,
      submitterLocalDriverPodKubernetesCredentials,
      sparkConf.getOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX"),
      sparkConf.getOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX"),
      sparkConf.getOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX"),
      sparkConf.getOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX"))
  }
}
