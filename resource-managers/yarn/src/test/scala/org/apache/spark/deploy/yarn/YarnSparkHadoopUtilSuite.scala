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
import org.apache.hadoop.yarn.api.records.ApplicationAccessType
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.UI._
import org.apache.spark.util.{ResetSystemProperties, Utils}

class YarnSparkHadoopUtilSuite extends SparkFunSuite with Matchers with ResetSystemProperties {

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
    val sparkConf = new SparkConf()
      .set("spark.hadoop." + key, "someHostName")
    val yarnConf = new YarnConfiguration(SparkHadoopUtil.get.newConfiguration(sparkConf))
    yarnConf.get(key) should be ("someHostName")
  }


  test("test getApplicationAclsForYarn acls on") {

    // spark acls on, just pick up default user
    val sparkConf = new SparkConf()
    sparkConf.set(ACLS_ENABLE, true)

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
    sparkConf.set(ACLS_ENABLE, true)
    sparkConf.set(UI_VIEW_ACLS, Seq("user1", "user2"))
    sparkConf.set(MODIFY_ACLS, Seq("user3", "user4"))

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

  test("SPARK-35672: test replaceEnvVars in Unix mode") {
    Map(
      "F_O_O$FOO$BAR" -> "F_O_OBAR",
      "$FOO" -> "BAR",
      "$F_O_O$FOO" -> "BarBAR",
      "${FOO}" -> "BAR",
      "$FOO.baz$BAR" -> "BAR.baz",
      "{{FOO}}" -> "BAR",
      "{{FOO}}$FOO" -> "BARBAR",
      "%FOO%" -> "%FOO%",
      """\$FOO\\\$FOO\${FOO}\\$FOO\\\\""" -> """$FOO\$FOO${FOO}\BAR\\"""
    ).foreach { case (input, expected) =>
      withClue(s"input string `$input`: ") {
        val replaced = YarnSparkHadoopUtil
          .replaceEnvVars(input, Map("F_O_O" -> "Bar", "FOO" -> "BAR"), isWindows = false)
        assert(replaced === expected)
      }
    }
  }

  test("SPARK-35672: test replaceEnvVars in Windows mode") {
    Map(
      "Foo%FOO%%BAR%" -> "FooBAR",
      "%FOO%" -> "BAR",
      "%F_O_O%%FOO%" -> "BarBAR",
      "{{FOO}}%FOO%" -> "BARBAR",
      "$FOO" -> "$FOO",
      "${FOO}" -> "${FOO}",
      "%%FOO%%%FOO%%%%%%FOO%" -> "%FOO%BAR%%BAR",
      "%FOO%^^^%FOO^%^FOO^^^^%FOO%" -> "BAR^%FOO%^FOO^^BAR"
    ).foreach { case (input, expected) =>
      withClue(s"input string `$input`: ") {
        val replaced = YarnSparkHadoopUtil
          .replaceEnvVars(input, Map("F_O_O" -> "Bar", "FOO" -> "BAR"), isWindows = true)
        assert(replaced === expected)
      }
    }
  }

}
