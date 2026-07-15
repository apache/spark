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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network.NETWORK_CRYPTO_ENABLED
import org.apache.spark.security.HadoopDelegationTokenProvider

private class TestNonKerberosTokenProvider extends HadoopDelegationTokenProvider {
  override def serviceName: String = "test-direct"

  override def delegationTokensRequired(
      sparkConf: SparkConf, hadoopConf: Configuration): Boolean = true

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    creds.addSecretKey(new Text("test.direct.credential"), "test-token".getBytes)
    Some(System.currentTimeMillis() + 3600000L)
  }
}

private class TestDisabledProvider extends HadoopDelegationTokenProvider {
  override def serviceName: String = "test-disabled"

  override def delegationTokensRequired(
      sparkConf: SparkConf, hadoopConf: Configuration): Boolean = false

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    // scalastyle:off throwerror
    throw new AssertionError("Should not be called when delegationTokensRequired is false")
    // scalastyle:on throwerror
  }
}

private class TestFailingProvider extends HadoopDelegationTokenProvider {
  override def serviceName: String = "test-failing"

  override def delegationTokensRequired(
      sparkConf: SparkConf, hadoopConf: Configuration): Boolean = true

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    throw new RuntimeException("Simulated provider failure")
  }
}

class NonKerberosCredentialsSuite extends SparkFunSuite {
  private val hadoopConf = new Configuration()

  private def baseConf: SparkConf = new SparkConf(false)
    .set(CREDENTIALS_DIRECT_PROVIDERS_ENABLED, true)
    .set(NETWORK_CRYPTO_ENABLED, true)

  test("renewalEnabled returns true when config is enabled") {
    val manager = new HadoopDelegationTokenManager(baseConf, hadoopConf, null)
    assert(manager.renewalEnabled)
  }

  test("renewalEnabled returns false when config is disabled and no Kerberos") {
    val sparkConf = new SparkConf(false)
      .set(NETWORK_CRYPTO_ENABLED, true)
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(!manager.renewalEnabled)
  }

  test("providers are called without Kerberos when config is enabled") {
    val manager = new HadoopDelegationTokenManager(baseConf, hadoopConf, null)

    val creds = new Credentials()
    manager.obtainDelegationTokens(creds)

    assert(creds.getSecretKey(new Text("test.direct.credential")) != null)
    assert(new String(creds.getSecretKey(new Text("test.direct.credential"))) === "test-token")
  }

  test("providers with delegationTokensRequired=false are not called") {
    val manager = new HadoopDelegationTokenManager(baseConf, hadoopConf, null)

    val creds = new Credentials()
    manager.obtainDelegationTokens(creds)

    assert(creds.getSecretKey(new Text("test.direct.credential")) != null)
  }

  test("provider failure does not prevent other providers from running") {
    val sparkConf = baseConf
      .set("spark.security.credentials.test-failing.enabled", "true")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)

    val creds = new Credentials()
    manager.obtainDelegationTokens(creds)

    assert(creds.getSecretKey(new Text("test.direct.credential")) != null)
    assert(new String(creds.getSecretKey(new Text("test.direct.credential"))) === "test-token")
  }

  test("individual provider can be disabled via per-service config") {
    val sparkConf = baseConf
      .set("spark.security.credentials.test-direct.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)

    assert(!manager.isProviderLoaded("test-direct"))
  }

  test("fails if network crypto is not enabled") {
    val sparkConf = new SparkConf(false)
      .set(CREDENTIALS_DIRECT_PROVIDERS_ENABLED, true)

    val e = intercept[IllegalArgumentException] {
      new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    }
    assert(e.getMessage.contains("spark.network.crypto.enabled must be true"))
  }
}
