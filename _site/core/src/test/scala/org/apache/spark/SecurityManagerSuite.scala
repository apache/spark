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

import org.scalatest.FunSuite

import org.apache.spark.util.Utils

class SecurityManagerSuite extends FunSuite {

  test("set security with conf") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    conf.set("spark.ui.acls.enable", "true")
    conf.set("spark.ui.view.acls", "user1,user2")
    val securityManager = new SecurityManager(conf);
    assert(securityManager.isAuthenticationEnabled() === true)
    assert(securityManager.aclsEnabled() === true)
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user2") === true)
    assert(securityManager.checkUIViewPermissions("user3") === false)
  }

  test("set security with api") {
    val conf = new SparkConf
    conf.set("spark.ui.view.acls", "user1,user2")
    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setAcls(false)
    assert(securityManager.aclsEnabled() === false)

    // acls are off so doesn't matter what view acls set to
    assert(securityManager.checkUIViewPermissions("user4") === true)

    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setViewAcls(Set[String]("user5"), "user6,user7")
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user5") === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkUIViewPermissions("user7") === true)
    assert(securityManager.checkUIViewPermissions("user8") === false)
    assert(securityManager.checkUIViewPermissions(null) === true)
  }

  test("set security modify acls") {
    val conf = new SparkConf
    conf.set("spark.modify.acls", "user1,user2")

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setAcls(false)
    assert(securityManager.aclsEnabled() === false)

    // acls are off so doesn't matter what view acls set to
    assert(securityManager.checkModifyPermissions("user4") === true)

    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setModifyAcls(Set("user5"), "user6,user7")
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user5") === true)
    assert(securityManager.checkModifyPermissions("user6") === true)
    assert(securityManager.checkModifyPermissions("user7") === true)
    assert(securityManager.checkModifyPermissions("user8") === false)
    assert(securityManager.checkModifyPermissions(null) === true)
  }

  test("set security admin acls") {
    val conf = new SparkConf
    conf.set("spark.admin.acls", "user1,user2")
    conf.set("spark.ui.view.acls", "user3")
    conf.set("spark.modify.acls", "user4")

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)

    assert(securityManager.checkModifyPermissions("user1") === true)
    assert(securityManager.checkModifyPermissions("user2") === true)
    assert(securityManager.checkModifyPermissions("user4") === true)
    assert(securityManager.checkModifyPermissions("user3") === false)
    assert(securityManager.checkModifyPermissions("user5") === false)
    assert(securityManager.checkModifyPermissions(null) === true)
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user2") === true)
    assert(securityManager.checkUIViewPermissions("user3") === true)
    assert(securityManager.checkUIViewPermissions("user4") === false)
    assert(securityManager.checkUIViewPermissions("user5") === false)
    assert(securityManager.checkUIViewPermissions(null) === true)

    securityManager.setAdminAcls("user6")
    securityManager.setViewAcls(Set[String]("user8"), "user9")
    securityManager.setModifyAcls(Set("user11"), "user9")
    assert(securityManager.checkModifyPermissions("user6") === true)
    assert(securityManager.checkModifyPermissions("user11") === true)
    assert(securityManager.checkModifyPermissions("user9") === true)
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user4") === false)
    assert(securityManager.checkModifyPermissions(null) === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkUIViewPermissions("user8") === true)
    assert(securityManager.checkUIViewPermissions("user9") === true)
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user3") === false)
    assert(securityManager.checkUIViewPermissions(null) === true)

  }

  test("ssl on setup") {
    val conf = SSLSampleConfigs.sparkSSLConfig()

    val securityManager = new SecurityManager(conf)

    assert(securityManager.fileServerSSLOptions.enabled === true)
    assert(securityManager.akkaSSLOptions.enabled === true)

    assert(securityManager.sslSocketFactory.isDefined === true)
    assert(securityManager.hostnameVerifier.isDefined === true)

    assert(securityManager.fileServerSSLOptions.trustStore.isDefined === true)
    assert(securityManager.fileServerSSLOptions.trustStore.get.getName === "truststore")
    assert(securityManager.fileServerSSLOptions.keyStore.isDefined === true)
    assert(securityManager.fileServerSSLOptions.keyStore.get.getName === "keystore")
    assert(securityManager.fileServerSSLOptions.trustStorePassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.keyStorePassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.keyPassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.protocol === Some("TLSv1"))
    assert(securityManager.fileServerSSLOptions.enabledAlgorithms ===
        Set("TLS_RSA_WITH_AES_128_CBC_SHA", "SSL_RSA_WITH_DES_CBC_SHA"))

    assert(securityManager.akkaSSLOptions.trustStore.isDefined === true)
    assert(securityManager.akkaSSLOptions.trustStore.get.getName === "truststore")
    assert(securityManager.akkaSSLOptions.keyStore.isDefined === true)
    assert(securityManager.akkaSSLOptions.keyStore.get.getName === "keystore")
    assert(securityManager.akkaSSLOptions.trustStorePassword === Some("password"))
    assert(securityManager.akkaSSLOptions.keyStorePassword === Some("password"))
    assert(securityManager.akkaSSLOptions.keyPassword === Some("password"))
    assert(securityManager.akkaSSLOptions.protocol === Some("TLSv1"))
    assert(securityManager.akkaSSLOptions.enabledAlgorithms ===
        Set("TLS_RSA_WITH_AES_128_CBC_SHA", "SSL_RSA_WITH_DES_CBC_SHA"))
  }

  test("ssl off setup") {
    val file = File.createTempFile("SSLOptionsSuite", "conf", Utils.createTempDir())

    System.setProperty("spark.ssl.configFile", file.getAbsolutePath)
    val conf = new SparkConf()

    val securityManager = new SecurityManager(conf)

    assert(securityManager.fileServerSSLOptions.enabled === false)
    assert(securityManager.akkaSSLOptions.enabled === false)
    assert(securityManager.sslSocketFactory.isDefined === false)
    assert(securityManager.hostnameVerifier.isDefined === false)
  }

}

