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

package org.apache.spark.deploy.yarn

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.io.Text
import org.apache.hadoop.yarn.api.records.ApplicationAccessType
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.Matchers

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ResetSystemProperties, Utils}

class YarnSparkHadoopUtilSuite extends SparkFunSuite with Matchers with Logging
  with ResetSystemProperties {

  val hasBash =
    try {
      val exitCode = Runtime.getRuntime().exec(Array("bash", "--version")).waitFor()
      exitCode == 0
    } catch {
      case e: IOException =>
        false
    }

  if (!hasBash) {
    logWarning("Cannot execute bash, skipping bash tests.")
  }

  def bashTest(name: String)(fn: => Unit): Unit =
    if (hasBash) test(name)(fn) else ignore(name)(fn)

  bashTest("shell script escaping") {
    val scriptFile = File.createTempFile("script.", ".sh", Utils.createTempDir())
    val args = Array("arg1", "${arg.2}", "\"arg3\"", "'arg4'", "$arg5", "\\arg6")
    try {
      val argLine = args.map(a => YarnSparkHadoopUtil.escapeForShell(a)).mkString(" ")
      Files.write(("bash -c \"echo " + argLine + "\"").getBytes(StandardCharsets.UTF_8), scriptFile)
      scriptFile.setExecutable(true)

      val proc = Runtime.getRuntime().exec(Array(scriptFile.getAbsolutePath()))
      val out = new String(ByteStreams.toByteArray(proc.getInputStream())).trim()
      val err = new String(ByteStreams.toByteArray(proc.getErrorStream()))
      val exitCode = proc.waitFor()
      exitCode should be (0)
      out should be (args.mkString(" "))
    } finally {
      scriptFile.delete()
    }
  }

  test("Yarn configuration override") {
    val key = "yarn.nodemanager.hostname"
    val default = new YarnConfiguration()

    val sparkConf = new SparkConf()
      .set("spark.hadoop." + key, "someHostName")
    val yarnConf = new YarnSparkHadoopUtil().newConfiguration(sparkConf)

    yarnConf.getClass() should be (classOf[YarnConfiguration])
    yarnConf.get(key) should not be default.get(key)
  }


  test("test getApplicationAclsForYarn acls on") {

    // spark acls on, just pick up default user
    val sparkConf = new SparkConf()
    sparkConf.set("spark.acls.enable", "true")

    val securityMgr = new SecurityManager(sparkConf)
    val acls = YarnSparkHadoopUtil.getApplicationAclsForYarn(securityMgr)

    val viewAcls = acls.get(ApplicationAccessType.VIEW_APP)
    val modifyAcls = acls.get(ApplicationAccessType.MODIFY_APP)

    viewAcls match {
      case Some(vacls) =>
        val aclSet = vacls.split(',').map(_.trim).toSet
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      case None =>
        fail()
    }
    modifyAcls match {
      case Some(macls) =>
        val aclSet = macls.split(',').map(_.trim).toSet
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      case None =>
        fail()
    }
  }

  test("test getApplicationAclsForYarn acls on and specify users") {

    // default spark acls are on and specify acls
    val sparkConf = new SparkConf()
    sparkConf.set("spark.acls.enable", "true")
    sparkConf.set("spark.ui.view.acls", "user1,user2")
    sparkConf.set("spark.modify.acls", "user3,user4")

    val securityMgr = new SecurityManager(sparkConf)
    val acls = YarnSparkHadoopUtil.getApplicationAclsForYarn(securityMgr)

    val viewAcls = acls.get(ApplicationAccessType.VIEW_APP)
    val modifyAcls = acls.get(ApplicationAccessType.MODIFY_APP)

    viewAcls match {
      case Some(vacls) =>
        val aclSet = vacls.split(',').map(_.trim).toSet
        assert(aclSet.contains("user1"))
        assert(aclSet.contains("user2"))
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      case None =>
        fail()
    }
    modifyAcls match {
      case Some(macls) =>
        val aclSet = macls.split(',').map(_.trim).toSet
        assert(aclSet.contains("user3"))
        assert(aclSet.contains("user4"))
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      case None =>
        fail()
    }

  }

  test("check different hadoop utils based on env variable") {
    try {
      System.setProperty("SPARK_YARN_MODE", "true")
      assert(SparkHadoopUtil.get.getClass === classOf[YarnSparkHadoopUtil])
      System.setProperty("SPARK_YARN_MODE", "false")
      assert(SparkHadoopUtil.get.getClass === classOf[SparkHadoopUtil])
    } finally {
      System.clearProperty("SPARK_YARN_MODE")
    }
  }



  // This test needs to live here because it depends on isYarnMode returning true, which can only
  // happen in the YARN module.
  test("security manager token generation") {
    try {
      System.setProperty("SPARK_YARN_MODE", "true")
      val initial = SparkHadoopUtil.get
        .getSecretKeyFromUserCredentials(SecurityManager.SECRET_LOOKUP_KEY)
      assert(initial === null || initial.length === 0)

      val conf = new SparkConf()
        .set(SecurityManager.SPARK_AUTH_CONF, "true")
        .set(SecurityManager.SPARK_AUTH_SECRET_CONF, "unused")
      val sm = new SecurityManager(conf)

      val generated = SparkHadoopUtil.get
        .getSecretKeyFromUserCredentials(SecurityManager.SECRET_LOOKUP_KEY)
      assert(generated != null)
      val genString = new Text(generated).toString()
      assert(genString != "unused")
      assert(sm.getSecretKey() === genString)
    } finally {
      // removeSecretKey() was only added in Hadoop 2.6, so instead we just set the secret
      // to an empty string.
      SparkHadoopUtil.get.addSecretKeyToUserCredentials(SecurityManager.SECRET_LOOKUP_KEY, "")
      System.clearProperty("SPARK_YARN_MODE")
    }
  }

}
