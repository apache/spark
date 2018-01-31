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
import org.apache.hadoop.security.Credentials
import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil

class YARNHadoopDelegationTokenManagerSuite extends SparkFunSuite with Matchers {
  private var credentialManager: YARNHadoopDelegationTokenManager = null
  private var sparkConf: SparkConf = null
  private var hadoopConf: Configuration = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkConf = new SparkConf()
    hadoopConf = new Configuration()
  }

  test("Correctly loads credential providers") {
    credentialManager = new YARNHadoopDelegationTokenManager(
      sparkConf,
      hadoopConf,
      conf => YarnSparkHadoopUtil.hadoopFSsToAccess(sparkConf, conf))

    credentialManager.credentialProviders.get("yarn-test") should not be (None)
  }
}

class YARNTestCredentialProvider extends ServiceCredentialProvider {
  override def serviceName: String = "yarn-test"

  override def credentialsRequired(conf: Configuration): Boolean = true

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = None
}
