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

package org.apache.spark.deploy.security

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.util.Utils

private class ExceptionThrowingDelegationTokenProvider extends HadoopDelegationTokenProvider {
  ExceptionThrowingDelegationTokenProvider.constructed = true
  throw new IllegalArgumentException

  override def serviceName: String = "throw"

  override def delegationTokensRequired(
    sparkConf: SparkConf,
    hadoopConf: Configuration): Boolean = throw new IllegalArgumentException

  override def obtainDelegationTokens(
    hadoopConf: Configuration,
    sparkConf: SparkConf,
    creds: Credentials): Option[Long] = throw new IllegalArgumentException
}

private object ExceptionThrowingDelegationTokenProvider {
  var constructed = false
}

class HadoopDelegationTokenManagerSuite extends SparkFunSuite {
  private val hadoopConf = new Configuration()

  test("default configuration") {
    ExceptionThrowingDelegationTokenProvider.constructed = false
    val manager = new HadoopDelegationTokenManager(new SparkConf(false), hadoopConf, null)
    assert(manager.isProviderLoaded("hadoopfs"))
    assert(manager.isProviderLoaded("hbase"))
    // This checks that providers are loaded independently and they have no effect on each other
    assert(ExceptionThrowingDelegationTokenProvider.constructed)
    assert(!manager.isProviderLoaded("throw"))
  }

  test("disable hadoopfs credential provider") {
    val sparkConf = new SparkConf(false).set("spark.security.credentials.hadoopfs.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(!manager.isProviderLoaded("hadoopfs"))
  }

  test("using deprecated configurations") {
    val sparkConf = new SparkConf(false)
      .set("spark.yarn.security.tokens.hadoopfs.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(!manager.isProviderLoaded("hadoopfs"))
    assert(manager.isProviderLoaded("hbase"))
  }

  test("SPARK-29082: do not fail if current user does not have credentials") {
    // SparkHadoopUtil overrides the UGI configuration during initialization. That normally
    // happens early in the Spark application, but here it may affect the test depending on
    // how it's run, so force its initialization.
    SparkHadoopUtil.get

    var kdc: MiniKdc = null
    try {
      // UserGroupInformation.setConfiguration needs default kerberos realm which can be set in
      // krb5.conf. MiniKdc sets "java.security.krb5.conf" in start and removes it when stop called.
      val kdcDir = Utils.createTempDir()
      val kdcConf = MiniKdc.createConf()
      kdc = new MiniKdc(kdcConf, kdcDir)
      kdc.start()

      val krbConf = new Configuration()
      krbConf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos")

      UserGroupInformation.setConfiguration(krbConf)
      val manager = new HadoopDelegationTokenManager(new SparkConf(false), krbConf, null)
      val testImpl = new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          assert(UserGroupInformation.isSecurityEnabled())
          val creds = new Credentials()
          manager.obtainDelegationTokens(creds)
          assert(creds.numberOfTokens() === 0)
          assert(creds.numberOfSecretKeys() === 0)
        }
      }

      val realUser = UserGroupInformation.createUserForTesting("realUser", Array.empty)
      realUser.doAs(testImpl)

      val proxyUser = UserGroupInformation.createProxyUserForTesting("proxyUser", realUser,
        Array.empty)
      proxyUser.doAs(testImpl)
    } finally {
      if (kdc != null) {
        kdc.stop()
      }
      UserGroupInformation.reset()
    }
  }
}
