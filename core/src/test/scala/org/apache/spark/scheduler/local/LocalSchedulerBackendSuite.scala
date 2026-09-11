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

package org.apache.spark.scheduler.local

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import java.util.Base64

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.deploy.security.UserCredentialManager
import org.apache.spark.internal.config._

/**
 * Tests that [[LocalSchedulerBackend]] starts a `UserCredentialManager` in local mode when OIDC
 * credential propagation is enabled, for parity with `HadoopDelegationTokenManager` (which
 * `LocalSchedulerBackend` already runs via `createTokenManager()`). This is the SPARK-59296
 * follow-up: before it, OIDC was a no-op in local mode.
 */
class LocalSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext {

  private var tokenFile: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tokenFile = File.createTempFile("oidc-token-", ".jwt")
    tokenFile.deleteOnExit()
    // A real (unsigned) JWT with the claims FileTokenIngestor requires: sub + iss (+ exp).
    Files.write(tokenFile.toPath, makeJwt().getBytes(StandardCharsets.UTF_8))
  }

  override def afterEach(): Unit = {
    try {
      if (tokenFile != null) tokenFile.delete()
    } finally {
      super.afterEach()
    }
  }

  /** Build a minimal unsigned JWT (header.payload) that FileTokenIngestor can parse. */
  private def makeJwt(): String = {
    val enc = Base64.getUrlEncoder.withoutPadding()
    val header = enc.encodeToString(
      """{"alg":"none","typ":"JWT"}""".getBytes(StandardCharsets.UTF_8))
    val exp = Instant.now().plusSeconds(300).getEpochSecond
    val payload = enc.encodeToString(
      s"""{"sub":"test-user","iss":"https://issuer.example.com","exp":$exp}"""
        .getBytes(StandardCharsets.UTF_8))
    s"$header.$payload"
  }

  private def oidcConf(enabled: Boolean): SparkConf = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("LocalSchedulerBackendSuite")
      .set(SECURITY_OIDC_ENABLED, enabled)
      // Short intervals so any renewal that fires during the test is cheap.
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 5000L)
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 1000L)
    if (enabled) {
      conf
        .set(SECURITY_OIDC_IDENTITY_TOKEN_FILE, tokenFile.getAbsolutePath)
        .set("spark.security.oidc.provider.fake",
          "org.apache.spark.security.FakeCredentialProvider")
    }
    conf
  }

  test("LocalSchedulerBackend runs OIDC selection and resolution in local mode") {
    sc = new SparkContext(oidcConf(enabled = true))

    // The scheduler backend in local mode is a LocalSchedulerBackend.
    assert(sc.schedulerBackend.isInstanceOf[LocalSchedulerBackend],
      "local[1] should use LocalSchedulerBackend")

    // Selection phase ran on the driver (SparkContext) even in local mode: the provider's
    // declared spark.hadoop.* property reached the driver's Hadoop Configuration (prefix
    // stripped), and its non-Hadoop spark.* property reached SparkConf.
    assert(sc.hadoopConfiguration.get("fs.fake.credentials.provider") ===
      "org.apache.spark.security.FakeExecutorCredentialProvider",
      "selection phase should wire the provider into the driver's Hadoop Configuration")
    assert(sc.getConf.get("spark.fake.credentials.enabled") === "true",
      "selection phase should apply non-Hadoop provider properties too")

    // A loader was retained for reuse by the resolution phase.
    assert(sc.userCredentialProviderLoader.isDefined,
      "SparkContext should retain the selection-phase loader when OIDC is enabled")

    // Resolution phase ran in LocalSchedulerBackend: credentials were acquired and stored in
    // the shared SparkEnv credential store (the same one tasks read from in local mode).
    val stored = SparkEnv.get.userCredentials.get()
    assert(stored != null,
      "LocalSchedulerBackend should acquire and store OIDC credentials in local mode")
    assert(stored.version >= 1L, "stored credentials should carry a version >= 1")
    val creds = UserCredentialManager.deserializeUserCredentials(stored.bytes)
    assert(creds.forScheme("fake").isPresent,
      "stored credentials should contain the 'fake' scheme resolved by FakeCredentialProvider")
  }

  test("LocalSchedulerBackend is a no-op for OIDC when disabled") {
    sc = new SparkContext(oidcConf(enabled = false))

    assert(sc.schedulerBackend.isInstanceOf[LocalSchedulerBackend])
    assert(sc.userCredentialProviderLoader.isEmpty,
      "no loader should be allocated when OIDC is disabled")
    assert(SparkEnv.get.userCredentials.get() == null,
      "no OIDC credentials should be stored when OIDC is disabled")
    // The provider's declared property must not have been applied.
    assert(sc.hadoopConfiguration.get("fs.fake.credentials.provider") == null,
      "no provider wiring should be applied when OIDC is disabled")
  }
}
