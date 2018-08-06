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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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
    val sparkConf = new SparkConf()
      .set("spark.hadoop." + key, "someHostName")
    val yarnConf = new YarnConfiguration(SparkHadoopUtil.get.newConfiguration(sparkConf))
    yarnConf.get(key) should be ("someHostName")
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

  test("SPARK-24149: retrieve all namenodes from HDFS") {
    val sparkConf = new SparkConf()
    val basicFederationConf = new Configuration()
    basicFederationConf.set("fs.defaultFS", "hdfs://localhost:8020")
    basicFederationConf.set("dfs.nameservices", "ns1,ns2")
    basicFederationConf.set("dfs.namenode.rpc-address.ns1", "localhost:8020")
    basicFederationConf.set("dfs.namenode.rpc-address.ns2", "localhost:8021")
    val basicFederationExpected = Set(
      new Path("hdfs://localhost:8020").getFileSystem(basicFederationConf),
      new Path("hdfs://localhost:8021").getFileSystem(basicFederationConf))
    val basicFederationResult = YarnSparkHadoopUtil.hadoopFSsToAccess(
      sparkConf, basicFederationConf)
    basicFederationResult should be (basicFederationExpected)

    // when viewfs is enabled, namespaces are handled by it, so we don't need to take care of them
    val viewFsConf = new Configuration()
    viewFsConf.addResource(basicFederationConf)
    viewFsConf.set("fs.defaultFS", "viewfs://clusterX/")
    viewFsConf.set("fs.viewfs.mounttable.clusterX.link./home", "hdfs://localhost:8020/")
    val viewFsExpected = Set(new Path("viewfs://clusterX/").getFileSystem(viewFsConf))
    YarnSparkHadoopUtil.hadoopFSsToAccess(sparkConf, viewFsConf) should be (viewFsExpected)

    // invalid config should not throw NullPointerException
    val invalidFederationConf = new Configuration()
    invalidFederationConf.addResource(basicFederationConf)
    invalidFederationConf.unset("dfs.namenode.rpc-address.ns2")
    val invalidFederationExpected = Set(
      new Path("hdfs://localhost:8020").getFileSystem(invalidFederationConf))
    val invalidFederationResult = YarnSparkHadoopUtil.hadoopFSsToAccess(
      sparkConf, invalidFederationConf)
    invalidFederationResult should be (invalidFederationExpected)

    // no namespaces defined, ie. old case
    val noFederationConf = new Configuration()
    noFederationConf.set("fs.defaultFS", "hdfs://localhost:8020")
    val noFederationExpected = Set(
      new Path("hdfs://localhost:8020").getFileSystem(noFederationConf))
    val noFederationResult = YarnSparkHadoopUtil.hadoopFSsToAccess(sparkConf, noFederationConf)
    noFederationResult should be (noFederationExpected)

    // federation and HA enabled
    val federationAndHAConf = new Configuration()
    federationAndHAConf.set("fs.defaultFS", "hdfs://clusterXHA")
    federationAndHAConf.set("dfs.nameservices", "clusterXHA,clusterYHA")
    federationAndHAConf.set("dfs.ha.namenodes.clusterXHA", "x-nn1,x-nn2")
    federationAndHAConf.set("dfs.ha.namenodes.clusterYHA", "y-nn1,y-nn2")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterXHA.x-nn1", "localhost:8020")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterXHA.x-nn2", "localhost:8021")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterYHA.y-nn1", "localhost:8022")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterYHA.y-nn2", "localhost:8023")
    federationAndHAConf.set("dfs.client.failover.proxy.provider.clusterXHA",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    federationAndHAConf.set("dfs.client.failover.proxy.provider.clusterYHA",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    val federationAndHAExpected = Set(
      new Path("hdfs://clusterXHA").getFileSystem(federationAndHAConf),
      new Path("hdfs://clusterYHA").getFileSystem(federationAndHAConf))
    val federationAndHAResult = YarnSparkHadoopUtil.hadoopFSsToAccess(
      sparkConf, federationAndHAConf)
    federationAndHAResult should be (federationAndHAExpected)
  }
}
