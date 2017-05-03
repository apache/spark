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
package org.apache.spark.deploy.rest.kubernetes.v2

import java.io.{File, FileInputStream, StringWriter}
import java.security.KeyStore

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.util.Utils

class ResourceStagingServerSslOptionsProviderSuite extends SparkFunSuite with BeforeAndAfter {

  private var sslTempDir: File = _
  private var keyStoreFile: File = _

  private var sparkConf: SparkConf = _
  private var sslOptionsProvider: ResourceStagingServerSslOptionsProvider = _

  before {
    sslTempDir = Utils.createTempDir(namePrefix = "resource-staging-server-ssl-test")
    keyStoreFile = new File(sslTempDir, "keyStore.jks")
    sparkConf = new SparkConf(true)
    sslOptionsProvider = new ResourceStagingServerSslOptionsProviderImpl(sparkConf)
  }

  test("Default SparkConf does not have TLS enabled.") {
    assert(sslOptionsProvider.getSslOptions === SSLOptions())
    assert(!sslOptionsProvider.getSslOptions.enabled)
    keyStoreFile.delete()
    sslTempDir.delete()
  }

  test("Setting keyStore, key password, and key field directly.") {
    sparkConf.set("spark.ssl.kubernetes.resourceStagingServer.enabled", "true")
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStore", keyStoreFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStorePassword", "keyStorePassword")
      .set("spark.ssl.kubernetes.resourceStagingServer.keyPassword", "keyPassword")
    val sslOptions = sslOptionsProvider.getSslOptions
    assert(sslOptions.enabled, "SSL should be enabled.")
    assert(sslOptions.keyStore.map(_.getAbsolutePath) === Some(keyStoreFile.getAbsolutePath),
      "Incorrect keyStore path or it was not set.")
    assert(sslOptions.keyStorePassword === Some("keyStorePassword"),
      "Incorrect keyStore password or it was not set.")
    assert(sslOptions.keyPassword === Some("keyPassword"),
      "Incorrect key password or it was not set.")
  }

  test("Setting key and certificate pem files should write an appropriate keyStore.") {
    val (keyPemFile, certPemFile) = SSLUtils.generateKeyCertPemPair("127.0.0.1")
    sparkConf.set("spark.ssl.kubernetes.resourceStagingServer.enabled", "true")
      .set("spark.ssl.kubernetes.resourceStagingServer.keyPem", keyPemFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.serverCertPem", certPemFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStorePassword", "keyStorePassword")
      .set("spark.ssl.kubernetes.resourceStagingServer.keyPassword", "keyPassword")
    val sslOptions = sslOptionsProvider.getSslOptions
    assert(sslOptions.enabled, "SSL should be enabled.")
    assert(sslOptions.keyStore.isDefined, "KeyStore should be defined.")
    sslOptions.keyStore.foreach { keyStoreFile =>
      val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
      Utils.tryWithResource(new FileInputStream(keyStoreFile)) {
        keyStore.load(_, "keyStorePassword".toCharArray)
      }
      val key = keyStore.getKey("key", "keyPassword".toCharArray)
      compareJcaPemObjectToFileString(key, keyPemFile)
      val certificate = keyStore.getCertificateChain("key")(0)
      compareJcaPemObjectToFileString(certificate, certPemFile)
    }
  }

  test("Using password files should read from the appropriate locations.") {
    val keyStorePasswordFile = new File(sslTempDir, "keyStorePassword.txt")
    Files.write("keyStorePassword", keyStorePasswordFile, Charsets.UTF_8)
    val keyPasswordFile = new File(sslTempDir, "keyPassword.txt")
    Files.write("keyPassword", keyPasswordFile, Charsets.UTF_8)
    sparkConf.set("spark.ssl.kubernetes.resourceStagingServer.enabled", "true")
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStore", keyStoreFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStorePasswordFile",
        keyStorePasswordFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyPasswordFile",
        keyPasswordFile.getAbsolutePath)
    val sslOptions = sslOptionsProvider.getSslOptions
    assert(sslOptions.keyStorePassword === Some("keyStorePassword"),
      "Incorrect keyStore password or it was not set.")
    assert(sslOptions.keyPassword === Some("keyPassword"),
      "Incorrect key password or it was not set.")
  }

  private def compareJcaPemObjectToFileString(pemObject: Any, pemFile: File): Unit = {
    Utils.tryWithResource(new StringWriter()) { stringWriter =>
      Utils.tryWithResource(new JcaPEMWriter(stringWriter)) { pemWriter =>
        pemWriter.writeObject(pemObject)
      }
      val pemFileAsString = Files.toString(pemFile, Charsets.UTF_8)
      assert(stringWriter.toString === pemFileAsString)
    }
  }
}
