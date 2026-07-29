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
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network.NETWORK_CRYPTO_ENABLED
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.security.HadoopDelegationTokenProvider

private class TestNonKerberosTokenProvider extends HadoopDelegationTokenProvider {
  override def serviceName: String = "test-direct"

  override def delegationTokensRequired(
      sparkConf: SparkConf, hadoopConf: Configuration): Boolean =
    sparkConf.get(DIRECT_CREDENTIAL_PROVIDERS_ENABLED)

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
      sparkConf: SparkConf, hadoopConf: Configuration): Boolean =
    sparkConf.get(DIRECT_CREDENTIAL_PROVIDERS_ENABLED)

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    throw new RuntimeException("Simulated provider failure")
  }
}

// Adds a credential but reports no expiry (returns None), mimicking providers such as
// HBaseDelegationTokenProvider. This exercises the nextRenewal == Long.MaxValue case where
// credentials were nevertheless obtained.
private class TestNoExpiryProvider extends HadoopDelegationTokenProvider {
  override def serviceName: String = "test-noexpiry"

  override def delegationTokensRequired(
      sparkConf: SparkConf, hadoopConf: Configuration): Boolean =
    sparkConf.get(DIRECT_CREDENTIAL_PROVIDERS_ENABLED)

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    creds.addSecretKey(new Text("test.noexpiry.credential"), "noexpiry-token".getBytes)
    None
  }
}

class NonKerberosCredentialsSuite extends SparkFunSuite {
  private val hadoopConf = new Configuration()

  private def baseConf: SparkConf = new SparkConf(false)
    .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
    .set(NETWORK_AUTH_ENABLED, true)
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

  test("fails if no RPC encryption is enabled") {
    val sparkConf = new SparkConf(false)
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)

    val e = intercept[IllegalArgumentException] {
      new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    }
    assert(e.getMessage.contains("RPC channel encryption"))
  }

  test("accepts SASL encryption as sufficient") {
    val sparkConf = new SparkConf(false)
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(SASL_ENCRYPTION_ENABLED, true)
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(manager.renewalEnabled)
  }

  test("accepts SSL RPC encryption as sufficient") {
    val sparkConf = new SparkConf(false)
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set("spark.ssl.rpc.enabled", "true")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(manager.renewalEnabled)
  }

  test("start() obtains tokens and sends UpdateDelegationTokens to schedulerRef") {
    val mockRef = mock(classOf[RpcEndpointRef])
    val manager = new HadoopDelegationTokenManager(baseConf, hadoopConf, mockRef)

    try {
      val tokens = manager.start()
      assert(tokens != null)

      val captor = ArgumentCaptor.forClass(classOf[Any])
      verify(mockRef).send(captor.capture())
      val msg = captor.getValue.asInstanceOf[UpdateDelegationTokens]

      val creds = SparkHadoopUtil.get.deserialize(msg.tokens)
      assert(creds.getSecretKey(new Text("test.direct.credential")) != null)
      assert(new String(creds.getSecretKey(new Text("test.direct.credential"))) === "test-token")
    } finally {
      manager.stop()
    }
  }

  test("start() does not send empty credentials when all direct providers fail") {
    val mockRef = mock(classOf[RpcEndpointRef])
    // Disable both succeeding providers so the failing provider is the sole active one,
    // producing a total failure with no credentials obtained.
    val sparkConf = baseConf
      .set("spark.security.credentials.test-direct.enabled", "false")
      .set("spark.security.credentials.test-noexpiry.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, mockRef)

    try {
      // On total failure, obtainTokensAndScheduleRenewal throws so updateTokensTask() skips
      // distributing empty credentials and schedules a retry instead. start() returns null
      // and no UpdateDelegationTokens message is ever sent to the executors.
      val tokens = manager.start()
      assert(tokens == null)
      verify(mockRef, never()).send(any())
    } finally {
      manager.stop()
    }
  }

  test("start() sends partial credentials when some providers succeed without expiry") {
    val mockRef = mock(classOf[RpcEndpointRef])
    // Disable the provider that reports an expiry, leaving a provider that adds a credential
    // but returns None (nextRenewal stays Long.MaxValue) alongside the failing one. Even
    // though a provider failed and no expiry was reported, the obtained credential must still
    // be distributed rather than discarded as a spurious total failure.
    val sparkConf = baseConf
      .set("spark.security.credentials.test-direct.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, mockRef)

    try {
      val tokens = manager.start()
      assert(tokens != null)

      val captor = ArgumentCaptor.forClass(classOf[Any])
      verify(mockRef).send(captor.capture())
      val msg = captor.getValue.asInstanceOf[UpdateDelegationTokens]

      val creds = SparkHadoopUtil.get.deserialize(msg.tokens)
      assert(creds.getSecretKey(new Text("test.noexpiry.credential")) != null)
      assert(new String(creds.getSecretKey(new Text("test.noexpiry.credential")))
        === "noexpiry-token")
    } finally {
      manager.stop()
    }
  }
}
