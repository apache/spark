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

import org.apache.spark.security.GroupMappingServiceProvider
import org.apache.spark.util.{ResetSystemProperties, SparkConfWithEnv, Utils}

class DummyGroupMappingServiceProvider extends GroupMappingServiceProvider {

  val userGroups: Set[String] = Set[String]("group1", "group2", "group3")

  override def getGroups(username: String): Set[String] = {
    userGroups
  }
}

class SecurityManagerSuite extends SparkFunSuite with ResetSystemProperties {

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

  test("set security with conf for groups") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    conf.set("spark.ui.acls.enable", "true")
    conf.set("spark.ui.view.acls.groups", "group1,group2")
    // default ShellBasedGroupsMappingProvider is used to resolve user groups
    val securityManager = new SecurityManager(conf);
    // assuming executing user does not belong to group1,group2
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)

    val conf2 = new SparkConf
    conf2.set("spark.authenticate", "true")
    conf2.set("spark.authenticate.secret", "good")
    conf2.set("spark.ui.acls.enable", "true")
    conf2.set("spark.ui.view.acls.groups", "group1,group2")
    // explicitly specify a custom GroupsMappingServiceProvider
    conf2.set("spark.user.groups.mapping", "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager2 = new SecurityManager(conf2);
    // group4,group5 do not match
    assert(securityManager2.checkUIViewPermissions("user1") === true)
    assert(securityManager2.checkUIViewPermissions("user2") === true)

    val conf3 = new SparkConf
    conf3.set("spark.authenticate", "true")
    conf3.set("spark.authenticate.secret", "good")
    conf3.set("spark.ui.acls.enable", "true")
    conf3.set("spark.ui.view.acls.groups", "group4,group5")
    // explicitly specify a bogus GroupsMappingServiceProvider
    conf3.set("spark.user.groups.mapping", "BogusServiceProvider")

    val securityManager3 = new SecurityManager(conf3);
    // BogusServiceProvider cannot be loaded and an error is logged returning an empty group set
    assert(securityManager3.checkUIViewPermissions("user1") === false)
    assert(securityManager3.checkUIViewPermissions("user2") === false)
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

  test("set security with api for groups") {
    val conf = new SparkConf
    conf.set("spark.user.groups.mapping", "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    securityManager.setViewAclsGroups("group1,group2")

    // group1,group2 match
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user2") === true)

    // change groups so they do not match
    securityManager.setViewAclsGroups("group4,group5")
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)

    val conf2 = new SparkConf
    conf.set("spark.user.groups.mapping", "BogusServiceProvider")

    val securityManager2 = new SecurityManager(conf2)
    securityManager2.setAcls(true)
    securityManager2.setViewAclsGroups("group1,group2")

    // group1,group2 do not match because of BogusServiceProvider
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)

    // setting viewAclsGroups to empty should still not match because of BogusServiceProvider
    securityManager2.setViewAclsGroups("")
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)
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

  test("set security modify acls for groups") {
    val conf = new SparkConf
    conf.set("spark.user.groups.mapping", "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    securityManager.setModifyAclsGroups("group1,group2")

    // group1,group2 match
    assert(securityManager.checkModifyPermissions("user1") === true)
    assert(securityManager.checkModifyPermissions("user2") === true)

    // change groups so they do not match
    securityManager.setModifyAclsGroups("group4,group5")
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user2") === false)

    // change so they match again
    securityManager.setModifyAclsGroups("group2,group3")
    assert(securityManager.checkModifyPermissions("user1") === true)
    assert(securityManager.checkModifyPermissions("user2") === true)
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

  test("set security admin acls for groups") {
    val conf = new SparkConf
    conf.set("spark.admin.acls.groups", "group1")
    conf.set("spark.ui.view.acls.groups", "group2")
    conf.set("spark.modify.acls.groups", "group3")
    conf.set("spark.user.groups.mapping", "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)

    // group1,group2,group3 match
    assert(securityManager.checkModifyPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user1") === true)

    // change admin groups so they do not match. view and modify groups are set to admin groups
    securityManager.setAdminAclsGroups("group4,group5")
    // invoke the set ui and modify to propagate the changes
    securityManager.setViewAclsGroups("")
    securityManager.setModifyAclsGroups("")

    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user1") === false)

    // change modify groups so they match
    securityManager.setModifyAclsGroups("group3")
    assert(securityManager.checkModifyPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user1") === false)

    // change view groups so they match
    securityManager.setViewAclsGroups("group2")
    securityManager.setModifyAclsGroups("group4")
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user1") === true)

    // change modify and view groups so they do not match
    securityManager.setViewAclsGroups("group7")
    securityManager.setModifyAclsGroups("group8")
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user1") === false)
  }

  test("set security with * in acls") {
    val conf = new SparkConf
    conf.set("spark.ui.acls.enable", "true")
    conf.set("spark.admin.acls", "user1,user2")
    conf.set("spark.ui.view.acls", "*")
    conf.set("spark.modify.acls", "user4")

    val securityManager = new SecurityManager(conf)
    assert(securityManager.aclsEnabled() === true)

    // check for viewAcls with *
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user5") === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkModifyPermissions("user4") === true)
    assert(securityManager.checkModifyPermissions("user7") === false)
    assert(securityManager.checkModifyPermissions("user8") === false)

    // check for modifyAcls with *
    securityManager.setModifyAcls(Set("user4"), "*")
    assert(securityManager.checkModifyPermissions("user7") === true)
    assert(securityManager.checkModifyPermissions("user8") === true)

    securityManager.setAdminAcls("user1,user2")
    securityManager.setModifyAcls(Set("user1"), "user2")
    securityManager.setViewAcls(Set("user1"), "user2")
    assert(securityManager.checkUIViewPermissions("user5") === false)
    assert(securityManager.checkUIViewPermissions("user6") === false)
    assert(securityManager.checkModifyPermissions("user7") === false)
    assert(securityManager.checkModifyPermissions("user8") === false)

    // check for adminAcls with *
    securityManager.setAdminAcls("user1,*")
    securityManager.setModifyAcls(Set("user1"), "user2")
    securityManager.setViewAcls(Set("user1"), "user2")
    assert(securityManager.checkUIViewPermissions("user5") === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkModifyPermissions("user7") === true)
    assert(securityManager.checkModifyPermissions("user8") === true)
  }

  test("set security with * in acls for groups") {
    val conf = new SparkConf
    conf.set("spark.ui.acls.enable", "true")
    conf.set("spark.admin.acls.groups", "group4,group5")
    conf.set("spark.ui.view.acls.groups", "*")
    conf.set("spark.modify.acls.groups", "group6")

    val securityManager = new SecurityManager(conf)
    assert(securityManager.aclsEnabled() === true)

    // check for viewAclsGroups with *
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user2") === true)
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user2") === false)

    // check for modifyAcls with *
    securityManager.setModifyAclsGroups("*")
    securityManager.setViewAclsGroups("group6")
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)
    assert(securityManager.checkModifyPermissions("user1") === true)
    assert(securityManager.checkModifyPermissions("user2") === true)

    // check for adminAcls with *
    securityManager.setAdminAclsGroups("group9,*")
    securityManager.setModifyAclsGroups("group4,group5")
    securityManager.setViewAclsGroups("group6,group7")
    assert(securityManager.checkUIViewPermissions("user5") === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkModifyPermissions("user7") === true)
    assert(securityManager.checkModifyPermissions("user8") === true)
  }

  test("security for groups default behavior") {
    // no groups or userToGroupsMapper provided
    // this will default to the ShellBasedGroupsMappingProvider
    val conf = new SparkConf

    val securityManager = new SecurityManager(conf)
    securityManager.setAcls(true)

    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user1") === false)

    // set groups only
    securityManager.setAdminAclsGroups("group1,group2")
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user1") === false)
  }

  test("ssl on setup") {
    val conf = SSLSampleConfigs.sparkSSLConfig()
    val expectedAlgorithms = Set(
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
    "TLS_RSA_WITH_AES_256_CBC_SHA256",
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
    "SSL_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
    "SSL_RSA_WITH_AES_256_CBC_SHA256",
    "SSL_DHE_RSA_WITH_AES_256_CBC_SHA256",
    "SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    "SSL_DHE_RSA_WITH_AES_128_CBC_SHA256")

    val securityManager = new SecurityManager(conf)

    assert(securityManager.fileServerSSLOptions.enabled === true)

    assert(securityManager.sslSocketFactory.isDefined === true)
    assert(securityManager.hostnameVerifier.isDefined === true)

    assert(securityManager.fileServerSSLOptions.trustStore.isDefined === true)
    assert(securityManager.fileServerSSLOptions.trustStore.get.getName === "truststore")
    assert(securityManager.fileServerSSLOptions.keyStore.isDefined === true)
    assert(securityManager.fileServerSSLOptions.keyStore.get.getName === "keystore")
    assert(securityManager.fileServerSSLOptions.trustStorePassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.keyStorePassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.keyPassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.protocol === Some("TLSv1.2"))
    assert(securityManager.fileServerSSLOptions.enabledAlgorithms === expectedAlgorithms)
  }

  test("ssl off setup") {
    val file = File.createTempFile("SSLOptionsSuite", "conf", Utils.createTempDir())

    System.setProperty("spark.ssl.configFile", file.getAbsolutePath)
    val conf = new SparkConf()

    val securityManager = new SecurityManager(conf)

    assert(securityManager.fileServerSSLOptions.enabled === false)
    assert(securityManager.sslSocketFactory.isDefined === false)
    assert(securityManager.hostnameVerifier.isDefined === false)
  }

  test("missing secret authentication key") {
    val conf = new SparkConf().set("spark.authenticate", "true")
    intercept[IllegalArgumentException] {
      new SecurityManager(conf)
    }
  }

  test("secret authentication key") {
    val key = "very secret key"
    val conf = new SparkConf()
      .set(SecurityManager.SPARK_AUTH_CONF, "true")
      .set(SecurityManager.SPARK_AUTH_SECRET_CONF, key)
    assert(key === new SecurityManager(conf).getSecretKey())

    val keyFromEnv = "very secret key from env"
    val conf2 = new SparkConfWithEnv(Map(SecurityManager.ENV_AUTH_SECRET -> keyFromEnv))
      .set(SecurityManager.SPARK_AUTH_CONF, "true")
      .set(SecurityManager.SPARK_AUTH_SECRET_CONF, key)
    assert(keyFromEnv === new SecurityManager(conf2).getSecretKey())
  }

}

