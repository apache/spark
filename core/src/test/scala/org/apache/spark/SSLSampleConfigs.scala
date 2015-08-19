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

package org.apache.spark

import java.io.File

object SSLSampleConfigs {
  val keyStorePath = new File(this.getClass.getResource("/keystore").toURI).getAbsolutePath
  val untrustedKeyStorePath = new File(
    this.getClass.getResource("/untrusted-keystore").toURI).getAbsolutePath
  val trustStorePath = new File(this.getClass.getResource("/truststore").toURI).getAbsolutePath

  val enabledAlgorithms =
    // A reasonable set of TLSv1.2 Oracle security provider suites
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384, " +
    "TLS_RSA_WITH_AES_256_CBC_SHA256, " +
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256, " +
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, " +
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256, " +
    // and their equivalent names in the IBM Security provider
    "SSL_ECDHE_RSA_WITH_AES_256_CBC_SHA384, " +
    "SSL_RSA_WITH_AES_256_CBC_SHA256, " +
    "SSL_DHE_RSA_WITH_AES_256_CBC_SHA256, " +
    "SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA256, " +
    "SSL_DHE_RSA_WITH_AES_128_CBC_SHA256"

  def sparkSSLConfig(): SparkConf = {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyStore", keyStorePath)
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.trustStore", trustStorePath)
    conf.set("spark.ssl.trustStorePassword", "password")
    conf.set("spark.ssl.enabledAlgorithms", enabledAlgorithms)
    conf.set("spark.ssl.protocol", "TLSv1.2")
    conf
  }

  def sparkSSLConfigUntrusted(): SparkConf = {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyStore", untrustedKeyStorePath)
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.trustStore", trustStorePath)
    conf.set("spark.ssl.trustStorePassword", "password")
    conf.set("spark.ssl.enabledAlgorithms", enabledAlgorithms)
    conf.set("spark.ssl.protocol", "TLSv1.2")
    conf
  }

}
