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

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.KubernetesCredentials
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.internal.config.OptionalConfigEntry

private[spark] class DriverPodKubernetesCredentialsProvider(sparkConf: SparkConf) {

  def get(): KubernetesCredentials = {
    sparkConf.get(KUBERNETES_SERVICE_ACCOUNT_NAME).foreach { _ =>
      require(sparkConf.get(KUBERNETES_DRIVER_OAUTH_TOKEN).isEmpty,
        "Cannot specify both a service account and a driver pod OAuth token.")
      require(sparkConf.get(KUBERNETES_DRIVER_CA_CERT_FILE).isEmpty,
        "Cannot specify both a service account and a driver pod CA cert file.")
      require(sparkConf.get(KUBERNETES_DRIVER_CLIENT_KEY_FILE).isEmpty,
        "Cannot specify both a service account and a driver pod client key file.")
      require(sparkConf.get(KUBERNETES_DRIVER_CLIENT_CERT_FILE).isEmpty,
        "Cannot specify both a service account and a driver pod client cert file.")
    }
    val oauthTokenBase64 = sparkConf.get(KUBERNETES_DRIVER_OAUTH_TOKEN).map { token =>
      BaseEncoding.base64().encode(token.getBytes(Charsets.UTF_8))
    }
    val caCertDataBase64 = safeFileConfToBase64(KUBERNETES_DRIVER_CA_CERT_FILE,
      s"Driver CA cert file provided at %s does not exist or is not a file.")
    val clientKeyDataBase64 = safeFileConfToBase64(KUBERNETES_DRIVER_CLIENT_KEY_FILE,
      s"Driver client key file provided at %s does not exist or is not a file.")
    val clientCertDataBase64 = safeFileConfToBase64(KUBERNETES_DRIVER_CLIENT_CERT_FILE,
      s"Driver client cert file provided at %s does not exist or is not a file.")
    KubernetesCredentials(
      oauthTokenBase64 = oauthTokenBase64,
      caCertDataBase64 = caCertDataBase64,
      clientKeyDataBase64 = clientKeyDataBase64,
      clientCertDataBase64 = clientCertDataBase64)
  }

  private def safeFileConfToBase64(
      conf: OptionalConfigEntry[String],
      fileNotFoundFormatString: String): Option[String] = {
    sparkConf.get(conf)
      .map(new File(_))
      .map { file =>
        require(file.isFile, String.format(fileNotFoundFormatString, file.getAbsolutePath))
        BaseEncoding.base64().encode(Files.toByteArray(file))
      }
  }
}
