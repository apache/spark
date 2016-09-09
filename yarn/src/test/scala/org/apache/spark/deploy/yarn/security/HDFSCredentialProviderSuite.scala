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

package org.apache.spark.deploy.yarn.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, PrivateMethodTester}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}

class HDFSCredentialProviderSuite
    extends SparkFunSuite
    with PrivateMethodTester
    with Matchers {
  private val _getTokenRenewer = PrivateMethod[String]('getTokenRenewer)

  private def getTokenRenewer(
      hdfsCredentialProvider: HDFSCredentialProvider, conf: Configuration): String = {
    hdfsCredentialProvider invokePrivate _getTokenRenewer(conf)
  }

  private var hdfsCredentialProvider: HDFSCredentialProvider = null

  override def beforeAll() {
    super.beforeAll()

    if (hdfsCredentialProvider == null) {
      hdfsCredentialProvider = new HDFSCredentialProvider()
    }
  }

  override def afterAll() {
    if (hdfsCredentialProvider != null) {
      hdfsCredentialProvider = null
    }

    super.afterAll()
  }

  test("check token renewer") {
    val hadoopConf = new Configuration()
    hadoopConf.set("yarn.resourcemanager.address", "myrm:8033")
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/myrm:8032@SPARKTEST.COM")
    val renewer = getTokenRenewer(hdfsCredentialProvider, hadoopConf)
    renewer should be ("yarn/myrm:8032@SPARKTEST.COM")
  }

  test("check token renewer default") {
    val hadoopConf = new Configuration()
    val caught =
      intercept[SparkException] {
        getTokenRenewer(hdfsCredentialProvider, hadoopConf)
      }
    assert(caught.getMessage === "Can't get Master Kerberos principal for use as renewer")
  }
}
