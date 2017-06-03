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
    val oauthTokenBase64 = sparkConf
        .getOption(s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.$OAUTH_TOKEN_CONF_SUFFIX")
        .map { token =>
      BaseEncoding.base64().encode(token.getBytes(Charsets.UTF_8))
    }
    val caCertDataBase64 = safeFileConfToBase64(
        s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
        s"Driver CA cert file provided at %s does not exist or is not a file.")
    val clientKeyDataBase64 = safeFileConfToBase64(
        s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
        s"Driver client key file provided at %s does not exist or is not a file.")
    val clientCertDataBase64 = safeFileConfToBase64(
        s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
        s"Driver client cert file provided at %s does not exist or is not a file.")
    KubernetesCredentials(
      oauthTokenBase64 = oauthTokenBase64,
      caCertDataBase64 = caCertDataBase64,
      clientKeyDataBase64 = clientKeyDataBase64,
      clientCertDataBase64 = clientCertDataBase64)
  }

  private def safeFileConfToBase64(
      conf: String,
      fileNotFoundFormatString: String): Option[String] = {
    sparkConf.getOption(conf)
      .map(new File(_))
      .map { file =>
        require(file.isFile, String.format(fileNotFoundFormatString, file.getAbsolutePath))
        BaseEncoding.base64().encode(Files.toByteArray(file))
      }
  }
}
