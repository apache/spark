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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.security.PrivilegedExceptionAction
import java.util.Base64

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.launcher.SparkLauncher
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
    conf.set(NETWORK_AUTH_ENABLED, true)
    conf.set(AUTH_SECRET, "good")
    conf.set(ACLS_ENABLE, true)
    conf.set(UI_VIEW_ACLS, Seq("user1", "user2"))
    val securityManager = new SecurityManager(conf)
    assert(securityManager.isAuthenticationEnabled())
    assert(securityManager.aclsEnabled())
    assert(securityManager.checkUIViewPermissions("user1"))
    assert(securityManager.checkUIViewPermissions("user2"))
    assert(securityManager.checkUIViewPermissions("user3") === false)
  }

  test("set security with conf for groups") {
    val conf = new SparkConf
    conf.set(NETWORK_AUTH_ENABLED, true)
    conf.set(AUTH_SECRET, "good")
    conf.set(ACLS_ENABLE, true)
    conf.set(UI_VIEW_ACLS_GROUPS, Seq("group1", "group2"))
    // default ShellBasedGroupsMappingProvider is used to resolve user groups
    val securityManager = new SecurityManager(conf);
    // assuming executing user does not belong to group1,group2
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)

    val conf2 = new SparkConf
    conf2.set(NETWORK_AUTH_ENABLED, true)
    conf2.set(AUTH_SECRET, "good")
    conf2.set(ACLS_ENABLE, true)
    conf2.set(UI_VIEW_ACLS_GROUPS, Seq("group1", "group2"))
    // explicitly specify a custom GroupsMappingServiceProvider
    conf2.set(USER_GROUPS_MAPPING, "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager2 = new SecurityManager(conf2)
    // group4,group5 do not match
    assert(securityManager2.checkUIViewPermissions("user1"))
    assert(securityManager2.checkUIViewPermissions("user2"))

    val conf3 = new SparkConf
    conf3.set(NETWORK_AUTH_ENABLED, true)
    conf3.set(AUTH_SECRET, "good")
    conf3.set(ACLS_ENABLE, true)
    conf3.set(UI_VIEW_ACLS_GROUPS, Seq("group4", "group5"))
    // explicitly specify a bogus GroupsMappingServiceProvider
    conf3.set(USER_GROUPS_MAPPING, "BogusServiceProvider")

    val securityManager3 = new SecurityManager(conf3)
    // BogusServiceProvider cannot be loaded and an error is logged returning an empty group set
    assert(securityManager3.checkUIViewPermissions("user1") === false)
    assert(securityManager3.checkUIViewPermissions("user2") === false)
  }

  test("set security with api") {
    val conf = new SparkConf
    conf.set(UI_VIEW_ACLS, Seq("user1", "user2"))
    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled())
    securityManager.setAcls(false)
    assert(securityManager.aclsEnabled() === false)

    // acls are off so doesn't matter what view acls set to
    assert(securityManager.checkUIViewPermissions("user4"))

    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled())
    securityManager.setViewAcls(Set[String]("user5"), Seq("user6", "user7"))
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user5"))
    assert(securityManager.checkUIViewPermissions("user6"))
    assert(securityManager.checkUIViewPermissions("user7"))
    assert(securityManager.checkUIViewPermissions("user8") === false)
    assert(securityManager.checkUIViewPermissions(null))
  }

  test("set security with api for groups") {
    val conf = new SparkConf
    conf.set(USER_GROUPS_MAPPING, "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager = new SecurityManager(conf)
    securityManager.setAcls(true)
    securityManager.setViewAclsGroups(Seq("group1", "group2"))

    // group1,group2 match
    assert(securityManager.checkUIViewPermissions("user1"))
    assert(securityManager.checkUIViewPermissions("user2"))

    // change groups so they do not match
    securityManager.setViewAclsGroups(Seq("group4", "group5"))
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)

    val conf2 = new SparkConf
    conf.set(USER_GROUPS_MAPPING, "BogusServiceProvider")

    val securityManager2 = new SecurityManager(conf2)
    securityManager2.setAcls(true)
    securityManager2.setViewAclsGroups(Seq("group1", "group2"))

    // group1,group2 do not match because of BogusServiceProvider
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)

    // setting viewAclsGroups to empty should still not match because of BogusServiceProvider
    securityManager2.setViewAclsGroups(Nil)
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)
  }

  test("set security modify acls") {
    val conf = new SparkConf
    conf.set(MODIFY_ACLS, Seq("user1", "user2"))

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled())
    securityManager.setAcls(false)
    assert(securityManager.aclsEnabled() === false)

    // acls are off so doesn't matter what view acls set to
    assert(securityManager.checkModifyPermissions("user4"))

    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled())
    securityManager.setModifyAcls(Set("user5"), Seq("user6", "user7"))
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user5"))
    assert(securityManager.checkModifyPermissions("user6"))
    assert(securityManager.checkModifyPermissions("user7"))
    assert(securityManager.checkModifyPermissions("user8") === false)
    assert(securityManager.checkModifyPermissions(null))
  }

  test("set security modify acls for groups") {
    val conf = new SparkConf
    conf.set(USER_GROUPS_MAPPING, "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager = new SecurityManager(conf)
    securityManager.setAcls(true)
    securityManager.setModifyAclsGroups(Seq("group1", "group2"))

    // group1,group2 match
    assert(securityManager.checkModifyPermissions("user1"))
    assert(securityManager.checkModifyPermissions("user2"))

    // change groups so they do not match
    securityManager.setModifyAclsGroups(Seq("group4", "group5"))
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user2") === false)

    // change so they match again
    securityManager.setModifyAclsGroups(Seq("group2", "group3"))

    assert(securityManager.checkModifyPermissions("user1"))
    assert(securityManager.checkModifyPermissions("user2"))
  }

  test("set security admin acls") {
    val conf = new SparkConf
    conf.set(ADMIN_ACLS, Seq("user1", "user2"))
    conf.set(UI_VIEW_ACLS, Seq("user3"))
    conf.set(MODIFY_ACLS, Seq("user4"))

    val securityManager = new SecurityManager(conf)
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled())

    assert(securityManager.checkModifyPermissions("user1"))
    assert(securityManager.checkModifyPermissions("user2"))
    assert(securityManager.checkModifyPermissions("user4"))
    assert(securityManager.checkModifyPermissions("user3") === false)
    assert(securityManager.checkModifyPermissions("user5") === false)
    assert(securityManager.checkModifyPermissions(null))
    assert(securityManager.checkUIViewPermissions("user1"))
    assert(securityManager.checkUIViewPermissions("user2"))
    assert(securityManager.checkUIViewPermissions("user3"))
    assert(securityManager.checkUIViewPermissions("user4") === false)
    assert(securityManager.checkUIViewPermissions("user5") === false)
    assert(securityManager.checkUIViewPermissions(null))

    securityManager.setAdminAcls(Seq("user6"))
    securityManager.setViewAcls(Set[String]("user8"), Seq("user9"))
    securityManager.setModifyAcls(Set("user11"), Seq("user9"))
    assert(securityManager.checkAdminPermissions("user6"))
    assert(!securityManager.checkAdminPermissions("user8"))
    assert(securityManager.checkModifyPermissions("user6"))
    assert(securityManager.checkModifyPermissions("user11"))
    assert(securityManager.checkModifyPermissions("user9"))
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user4") === false)
    assert(securityManager.checkModifyPermissions(null))
    assert(securityManager.checkUIViewPermissions("user6"))
    assert(securityManager.checkUIViewPermissions("user8"))
    assert(securityManager.checkUIViewPermissions("user9"))
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user3") === false)
    assert(securityManager.checkUIViewPermissions(null))
  }

  test("set security admin acls for groups") {
    val conf = new SparkConf
    conf.set(ADMIN_ACLS_GROUPS, Seq("group1"))
    conf.set(UI_VIEW_ACLS_GROUPS, Seq("group2"))
    conf.set(MODIFY_ACLS_GROUPS, Seq("group3"))
    conf.set(USER_GROUPS_MAPPING, "org.apache.spark.DummyGroupMappingServiceProvider")

    val securityManager = new SecurityManager(conf)
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled())

    // group1,group2,group3 match
    assert(securityManager.checkAdminPermissions("user1"))
    assert(securityManager.checkModifyPermissions("user1"))
    assert(securityManager.checkUIViewPermissions("user1"))

    // change admin groups so they do not match. view and modify groups are set to admin groups
    securityManager.setAdminAclsGroups(Seq("group4", "group5"))
    // invoke the set ui and modify to propagate the changes
    securityManager.setViewAclsGroups(Nil)
    securityManager.setModifyAclsGroups(Nil)

    assert(!securityManager.checkAdminPermissions("user1"))
    assert(!securityManager.checkModifyPermissions("user1"))
    assert(!securityManager.checkUIViewPermissions("user1"))

    // change modify groups so they match
    securityManager.setModifyAclsGroups(Seq("group3"))
    assert(securityManager.checkModifyPermissions("user1"))
    assert(securityManager.checkUIViewPermissions("user1") === false)

    // change view groups so they match
    securityManager.setViewAclsGroups(Seq("group2"))
    securityManager.setModifyAclsGroups(Seq("group4"))
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user1"))

    // change modify and view groups so they do not match
    securityManager.setViewAclsGroups(Seq("group7"))
    securityManager.setModifyAclsGroups(Seq("group8"))
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user1") === false)
  }

  test("set security with * in acls") {
    val conf = new SparkConf
    conf.set(ACLS_ENABLE.key, "true")
    conf.set(ADMIN_ACLS, Seq("user1", "user2"))
    conf.set(UI_VIEW_ACLS, Seq("*"))
    conf.set(MODIFY_ACLS, Seq("user4"))

    val securityManager = new SecurityManager(conf)
    assert(securityManager.aclsEnabled())

    // check for viewAcls with *
    assert(securityManager.checkUIViewPermissions("user1"))
    assert(securityManager.checkUIViewPermissions("user5"))
    assert(securityManager.checkUIViewPermissions("user6"))
    assert(securityManager.checkModifyPermissions("user4"))
    assert(securityManager.checkModifyPermissions("user7") === false)
    assert(securityManager.checkModifyPermissions("user8") === false)

    // check for modifyAcls with *
    securityManager.setModifyAcls(Set("user4"), Seq("*"))
    assert(securityManager.checkModifyPermissions("user7"))
    assert(securityManager.checkModifyPermissions("user8"))

    securityManager.setAdminAcls(Seq("user1", "user2"))
    securityManager.setModifyAcls(Set("user1"), Seq("user2"))
    securityManager.setViewAcls(Set("user1"), Seq("user2"))
    assert(securityManager.checkUIViewPermissions("user5") === false)
    assert(securityManager.checkUIViewPermissions("user6") === false)
    assert(securityManager.checkModifyPermissions("user7") === false)
    assert(securityManager.checkModifyPermissions("user8") === false)

    // check for adminAcls with *
    securityManager.setAdminAcls(Seq("user1", "*"))
    securityManager.setModifyAcls(Set("user1"), Seq("user2"))
    securityManager.setViewAcls(Set("user1"), Seq("user2"))
    assert(securityManager.checkUIViewPermissions("user5"))
    assert(securityManager.checkUIViewPermissions("user6"))
    assert(securityManager.checkModifyPermissions("user7"))
    assert(securityManager.checkModifyPermissions("user8"))
  }

  test("set security with * in acls for groups") {
    val conf = new SparkConf
    conf.set(ACLS_ENABLE, true)
    conf.set(ADMIN_ACLS_GROUPS, Seq("group4", "group5"))
    conf.set(UI_VIEW_ACLS_GROUPS, Seq("*"))
    conf.set(MODIFY_ACLS_GROUPS, Seq("group6"))

    val securityManager = new SecurityManager(conf)
    assert(securityManager.aclsEnabled())

    // check for viewAclsGroups with *
    assert(securityManager.checkUIViewPermissions("user1"))
    assert(securityManager.checkUIViewPermissions("user2"))
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user2") === false)

    // check for modifyAcls with *
    securityManager.setModifyAclsGroups(Seq("*"))
    securityManager.setViewAclsGroups(Seq("group6"))
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user2") === false)
    assert(securityManager.checkModifyPermissions("user1"))
    assert(securityManager.checkModifyPermissions("user2"))

    // check for adminAcls with *
    securityManager.setAdminAclsGroups(Seq("group9", "*"))
    securityManager.setModifyAclsGroups(Seq("group4", "group5"))
    securityManager.setViewAclsGroups(Seq("group6", "group7"))
    assert(securityManager.checkUIViewPermissions("user5"))
    assert(securityManager.checkUIViewPermissions("user6"))
    assert(securityManager.checkModifyPermissions("user7"))
    assert(securityManager.checkModifyPermissions("user8"))
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
    securityManager.setAdminAclsGroups(Seq("group1", "group2"))
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user1") === false)
  }

  test("missing secret authentication key") {
    val conf = new SparkConf().set(NETWORK_AUTH_ENABLED, true)
    val mgr = new SecurityManager(conf)
    intercept[IllegalArgumentException] {
      mgr.getSecretKey()
    }
    intercept[IllegalArgumentException] {
      mgr.initializeAuth()
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

  test("use executor-specific secret file configuration.") {
    withSecretFile("driver-secret") { secretFileFromDriver =>
      withSecretFile("executor-secret") { secretFileFromExecutor =>
        val conf = new SparkConf()
          .setMaster("k8s://127.0.0.1")
          .set(AUTH_SECRET_FILE_DRIVER, Some(secretFileFromDriver.getAbsolutePath))
          .set(AUTH_SECRET_FILE_EXECUTOR, Some(secretFileFromExecutor.getAbsolutePath))
          .set(SecurityManager.SPARK_AUTH_CONF, "true")
        val mgr = new SecurityManager(conf, authSecretFileConf = AUTH_SECRET_FILE_EXECUTOR)
        assert(encodeFileAsBase64(secretFileFromExecutor) === mgr.getSecretKey())
      }
    }
  }

  test("secret file must be defined in both driver and executor") {
    val conf1 = new SparkConf()
      .set(AUTH_SECRET_FILE_DRIVER, Some("/tmp/driver-secret.txt"))
      .set(SecurityManager.SPARK_AUTH_CONF, "true")
    val mgr1 = new SecurityManager(conf1)
    intercept[IllegalArgumentException] {
      mgr1.initializeAuth()
    }

    val conf2 = new SparkConf()
      .set(AUTH_SECRET_FILE_EXECUTOR, Some("/tmp/executor-secret.txt"))
      .set(SecurityManager.SPARK_AUTH_CONF, "true")
    val mgr2 = new SecurityManager(conf2)
    intercept[IllegalArgumentException] {
      mgr2.initializeAuth()
    }
  }

  Seq("yarn", "local", "local[*]", "local[1,2]", "mesos://localhost:8080").foreach { master =>
    test(s"master $master cannot use file mounted secrets") {
      val conf = new SparkConf()
        .set(AUTH_SECRET_FILE, "/tmp/secret.txt")
        .set(SecurityManager.SPARK_AUTH_CONF, "true")
        .setMaster(master)
      intercept[IllegalArgumentException] {
        new SecurityManager(conf).getSecretKey()
      }
      intercept[IllegalArgumentException] {
        new SecurityManager(conf).initializeAuth()
      }
    }
  }

  // How is the secret expected to be generated and stored.
  object SecretTestType extends Enumeration {
    val MANUAL, AUTO, UGI, FILE = Value
  }

  import SecretTestType._

  Seq(
    ("yarn", UGI),
    ("local", UGI),
    ("local[*]", UGI),
    ("local[1, 2]", UGI),
    ("k8s://127.0.0.1", AUTO),
    ("k8s://127.0.1.1", FILE),
    ("local-cluster[2, 1, 1024]", MANUAL),
    ("invalid", MANUAL)
  ).foreach { case (master, secretType) =>
    test(s"secret key generation: master '$master'") {
      val conf = new SparkConf()
        .set(NETWORK_AUTH_ENABLED, true)
        .set(SparkLauncher.SPARK_MASTER, master)
      val mgr = new SecurityManager(conf)

      UserGroupInformation.createUserForTesting("authTest", Array()).doAs(
        new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            secretType match {
              case UGI =>
                mgr.initializeAuth()
                val creds = UserGroupInformation.getCurrentUser().getCredentials()
                val secret = creds.getSecretKey(SecurityManager.SECRET_LOOKUP_KEY)
                assert(secret != null)
                assert(new String(secret, UTF_8) === mgr.getSecretKey())

              case AUTO =>
                mgr.initializeAuth()
                val creds = UserGroupInformation.getCurrentUser().getCredentials()
                assert(creds.getSecretKey(SecurityManager.SECRET_LOOKUP_KEY) === null)

              case MANUAL =>
                intercept[IllegalArgumentException] {
                  mgr.initializeAuth()
                }
                intercept[IllegalArgumentException] {
                  mgr.getSecretKey()
                }

              case FILE =>
                withSecretFile() { secretFile =>
                  conf.set(AUTH_SECRET_FILE, secretFile.getAbsolutePath)
                  mgr.initializeAuth()
                  assert(encodeFileAsBase64(secretFile) === mgr.getSecretKey())
                }
            }
          }
        }
      )
    }
  }

  private def encodeFileAsBase64(secretFile: File) = {
    Base64.getEncoder.encodeToString(Files.readAllBytes(secretFile.toPath))
  }

  private def withSecretFile(contents: String = "test-secret")(f: File => Unit): Unit = {
    val secretDir = Utils.createTempDir("temp-secrets")
    val secretFile = new File(secretDir, "temp-secret.txt")
    Files.write(secretFile.toPath, contents.getBytes(UTF_8))
    try f(secretFile) finally {
      Utils.deleteRecursively(secretDir)
    }
  }
}

