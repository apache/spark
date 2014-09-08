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

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.{FunSuite, Matchers}

import org.apache.hadoop.yarn.api.records.ApplicationAccessType

import org.apache.spark.{Logging, SecurityManager, SparkConf}


class YarnSparkHadoopUtilSuite extends FunSuite with Matchers with Logging {

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

  def bashTest(name: String)(fn: => Unit) =
    if (hasBash) test(name)(fn) else ignore(name)(fn)

  bashTest("shell script escaping") {
    val scriptFile = File.createTempFile("script.", ".sh")
    val args = Array("arg1", "${arg.2}", "\"arg3\"", "'arg4'", "$arg5", "\\arg6")
    try {
      val argLine = args.map(a => YarnSparkHadoopUtil.escapeForShell(a)).mkString(" ")
      Files.write(("bash -c \"echo " + argLine + "\"").getBytes(), scriptFile)
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
      case Some(vacls) => {
        val aclSet = vacls.split(',').map(_.trim).toSet
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      }
      case None => {
        fail()
      }
    }
    modifyAcls match {
      case Some(macls) => {
        val aclSet = macls.split(',').map(_.trim).toSet
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      }
      case None => {
        fail()
      }
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
      case Some(vacls) => {
        val aclSet = vacls.split(',').map(_.trim).toSet
        assert(aclSet.contains("user1"))
        assert(aclSet.contains("user2"))
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      }
      case None => {
        fail()
      }
    }
    modifyAcls match {
      case Some(macls) => {
        val aclSet = macls.split(',').map(_.trim).toSet
        assert(aclSet.contains("user3"))
        assert(aclSet.contains("user4"))
        assert(aclSet.contains(System.getProperty("user.name", "invalid")))
      }
      case None => {
        fail()
      }
    }

  }
}
