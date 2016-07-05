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

package org.apache.spark.deploy.yarn.token

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.Matchers
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}

class HDFSTokenProviderSuite extends SparkFunSuite with PrivateMethodTester with Matchers {
  private val _nnsToAccess = PrivateMethod[Set[Path]]('nnsToAccess)
  private val _getTokenRenewer = PrivateMethod[String]('getTokenRenewer)

  private var hdfsTokenProvider: HDFSTokenProvider = null
  private var sparkConf: SparkConf = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkConf = new SparkConf()
    hdfsTokenProvider = ConfigurableTokenManager.hdfsTokenProvider(sparkConf)
  }

  override def afterAll(): Unit = {
    ConfigurableTokenManager.configurableTokenManager(sparkConf).stop()
    super.afterAll()
  }

  private def nnsToAccess(hdfsTokenProvider: HDFSTokenProvider): Set[Path] = {
    hdfsTokenProvider invokePrivate _nnsToAccess()
  }

  private def getTokenRenewer(hdfsTokenProvider: HDFSTokenProvider, conf: Configuration): String = {
    hdfsTokenProvider invokePrivate _getTokenRenewer(conf)
  }

  test("check access nns empty") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "")
    hdfsTokenProvider.setNameNodesToAccess(sparkConf, Set())
    nnsToAccess(hdfsTokenProvider) should be (Set())
  }

  test("check access nns unset") {
    val sparkConf = new SparkConf()
    hdfsTokenProvider.setNameNodesToAccess(sparkConf, Set())
    nnsToAccess(hdfsTokenProvider) should be (Set())
  }

  test("check access nns") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "hdfs://nn1:8032")
    hdfsTokenProvider.setNameNodesToAccess(sparkConf, Set())
    nnsToAccess(hdfsTokenProvider) should be (Set(new Path("hdfs://nn1:8032")))
  }

  test("check access nns space") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "hdfs://nn1:8032, ")
    hdfsTokenProvider.setNameNodesToAccess(sparkConf, Set())
    nnsToAccess(hdfsTokenProvider) should be (Set(new Path("hdfs://nn1:8032")))
  }

  test("check access two nns") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "hdfs://nn1:8032,hdfs://nn2:8032")
    hdfsTokenProvider.setNameNodesToAccess(sparkConf, Set())
    nnsToAccess(hdfsTokenProvider) should be
      (Set(new Path("hdfs://nn1:8032"), new Path("hdfs://nn2:8032")))
  }

  test("check token renewer") {
    val hadoopConf = new Configuration()
    hadoopConf.set("yarn.resourcemanager.address", "myrm:8033")
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/myrm:8032@SPARKTEST.COM")
    val renewer = getTokenRenewer(hdfsTokenProvider, hadoopConf)
    renewer should be ("yarn/myrm:8032@SPARKTEST.COM")
  }

  test("check token renewer default") {
    val hadoopConf = new Configuration()
    val caught =
      intercept[SparkException] {
        getTokenRenewer(hdfsTokenProvider, hadoopConf)
      }
    assert(caught.getMessage === "Can't get Master Kerberos principal for use as renewer")
  }
}
