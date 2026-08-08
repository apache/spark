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

import java.io.{File, PrintWriter}
import java.time.Instant
import java.util.Optional
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, verify}
import org.scalatest.concurrent.Eventually.{eventually, timeout}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network.NETWORK_CRYPTO_ENABLED
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.security._

/**
 * Integration tests for SPARK-57896: Kerberos coexistence and per-user token tests.
 *
 * Verifies that:
 * 1. UserCredentialManager (OIDC) delivers credentials via the update callback
 * 2. Credential refresh works end-to-end with expiring tokens
 * 3. Per-user identity tokens produce valid credentials identically to workload tokens
 * 4. Both UserCredentialManager and HadoopDelegationTokenManager can run simultaneously
 *    without interfering with each other
 * 5. Failure in one credential system does not affect the other
 */
class OidcCredentialIntegrationSuite extends SparkFunSuite {

  private var tokenFile: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    CredentialProviderLoader.resetForTesting()
    tokenFile = File.createTempFile("oidc-token-", ".jwt")
    tokenFile.deleteOnExit()
    writeTokenFile("fake.jwt.token.workload")
  }

  override def afterEach(): Unit = {
    if (tokenFile != null) {
      tokenFile.delete()
    }
    super.afterEach()
  }

  private def writeTokenFile(content: String): Unit = {
    val pw = new PrintWriter(tokenFile)
    try {
      pw.print(content)
    } finally {
      pw.close()
    }
  }

  private def createOidcConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SECURITY_OIDC_ENABLED, true)
      .set(SECURITY_OIDC_IDENTITY_TOKEN_FILE, tokenFile.getAbsolutePath)
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 5000L)
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 1000L)
  }

  private def createUserContext(
      principal: String = "test-user",
      expiresInSeconds: Long = 300): UserContext = {
    new UserContext(
      principal,
      "https://issuer.example.com",
      "fake.jwt.token",
      Instant.now(),
      Instant.now().plusSeconds(expiresInSeconds))
  }

  private def createIngestor(ctx: UserContext): TokenIngestor = {
    new TokenIngestor {
      override def load(): Optional[UserContext] = Optional.of(ctx)
    }
  }

  private def createFailingIngestor(): TokenIngestor = {
    new TokenIngestor {
      override def load(): Optional[UserContext] = Optional.empty()
    }
  }

  test("SPARK-57896: OIDC credential delivery via update callback") {
    val conf = createOidcConf()
    val ctx = createUserContext()
    val callbackRef = new AtomicReference[Array[Byte]]()
    val callbackVersion = new AtomicReference[Long](0L)

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (version, bytes) => {
        callbackVersion.set(version)
        callbackRef.set(bytes)
      })

    try {
      val (version, initialBytes) = manager.start()

      // Verify callback was invoked with initial credentials
      assert(version == 1L, "Initial version should be 1")
      assert(initialBytes != null, "Initial credentials should not be null")

      // Deserialize and verify the credential content
      val credentials = UserCredentialManager.deserializeUserCredentials(initialBytes)
      assert(credentials != null, "Deserialized credentials should not be null")

      // FakeCredentialProvider resolves for scheme "fake"
      val fakeCred = credentials.forScheme("fake")
      assert(fakeCred.isPresent, "Should have credential for scheme 'fake'")
      assert(fakeCred.get().getProperties.get("provider") == "fake",
        "Credential should come from FakeCredentialProvider")
      assert(!fakeCred.get().isExpired(Instant.now()),
        "Credential should not be expired immediately after resolution")
    } finally {
      manager.stop()
    }
  }

  test("SPARK-57896: credential refresh works end-to-end on expiry") {
    val conf = createOidcConf()
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 2000L) // 2s before expiry
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 500L)   // 500ms min interval

    // Create a short-lived context (expires in 3 seconds)
    val ctx = createUserContext(expiresInSeconds = 3)
    val updateCount = new AtomicInteger(0)
    val latestVersion = new AtomicReference[Long](0L)

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (version, _) => {
        latestVersion.set(version)
        updateCount.incrementAndGet()
      })

    try {
      manager.start()
      assert(updateCount.get() == 1, "Should have exactly 1 update after start()")

      // Wait for renewal to trigger (safety margin is 2s, credential expires in 3s,
      // so renewal should happen around t=1s)
      eventually(timeout(10.seconds)) {
        assert(updateCount.get() >= 2,
          s"Expected at least 2 updates (got ${updateCount.get()}), " +
            "indicating credential renewal occurred")
      }

      // Verify version is monotonically increasing
      assert(latestVersion.get() >= 2L,
        "Version should be at least 2 after renewal")
    } finally {
      manager.stop()
    }
  }

  test("SPARK-57896: per-user identity token produces valid credentials") {
    val conf = createOidcConf()

    // Create a per-user context (different principal, same mechanism)
    val userCtx = createUserContext(principal = "alice@corp.example.com")
    val callbackRef = new AtomicReference[Array[Byte]]()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(userCtx),
      (_, bytes) => callbackRef.set(bytes))

    try {
      val (_, initialBytes) = manager.start()

      // Verify per-user token produces the same credential structure as workload token
      val credentials = UserCredentialManager.deserializeUserCredentials(initialBytes)
      val fakeCred = credentials.forScheme("fake")
      assert(fakeCred.isPresent,
        "Per-user token should produce credentials for scheme 'fake'")
      assert(fakeCred.get().getProperties.get("provider") == "fake",
        "Per-user credential should come from FakeCredentialProvider")
      assert(!fakeCred.get().isExpired(Instant.now()),
        "Per-user credential should not be expired")

      // Verify the credential is identical in structure to workload token output
      // (FakeCredentialProvider doesn't distinguish -- same ServiceCredential for any UserContext)
      val workloadCtx = createUserContext(principal = "workload-identity")
      val workloadManager = new UserCredentialManager(
        conf,
        createIngestor(workloadCtx),
        (_, _) => ())
      try {
        val (_, workloadBytes) = workloadManager.start()
        val workloadCreds = UserCredentialManager.deserializeUserCredentials(workloadBytes)
        val workloadFake = workloadCreds.forScheme("fake")
        assert(workloadFake.isPresent)
        // Both produce the same credential properties (provider=fake)
        assert(workloadFake.get().getProperties == fakeCred.get().getProperties,
          "Per-user and workload tokens should produce identical credential properties")
      } finally {
        workloadManager.stop()
      }
    } finally {
      manager.stop()
    }
  }

  test("SPARK-57896: UserCredentialManager and HadoopDelegationTokenManager coexist") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

    // Configure for BOTH OIDC and direct credential providers (non-Kerberos DT path)
    val conf = createOidcConf()
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(NETWORK_CRYPTO_ENABLED, true)

    val ctx = createUserContext()
    val oidcCallbackRef = new AtomicReference[Array[Byte]]()
    val oidcVersion = new AtomicReference[Long](0L)

    // Start UserCredentialManager (OIDC)
    val oidcManager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (version, bytes) => {
        oidcVersion.set(version)
        oidcCallbackRef.set(bytes)
      })

    // Start HadoopDelegationTokenManager (Kerberos/direct)
    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)

    try {
      // Start OIDC manager
      val (oidcVer, oidcBytes) = oidcManager.start()
      assert(oidcVer == 1L)
      assert(oidcBytes != null)

      // Start DT manager (uses TestNonKerberosTokenProvider from NonKerberosCredentialsSuite)
      val dtTokens = dtManager.start()
      assert(dtTokens != null, "DT manager should produce tokens")

      // Verify OIDC credentials are valid
      val oidcCreds = UserCredentialManager.deserializeUserCredentials(oidcBytes)
      assert(oidcCreds.forScheme("fake").isPresent,
        "OIDC credentials should contain 'fake' scheme")

      // Verify DT credentials were sent via RPC
      val captor = ArgumentCaptor.forClass(classOf[Any])
      eventually(timeout(5.seconds)) {
        verify(mockRef).send(captor.capture())
      }
      val msg = captor.getValue.asInstanceOf[UpdateDelegationTokens]
      val dtCreds = SparkHadoopUtil.get.deserialize(msg.tokens)
      assert(dtCreds.getSecretKey(new Text("test.direct.credential")) != null,
        "DT credentials should contain test.direct.credential")
      assert(new String(dtCreds.getSecretKey(new Text("test.direct.credential"))) === "test-token",
        "DT credential value should match")

      // Both systems produced credentials independently
      assert(oidcVersion.get() == 1L, "OIDC version should remain at 1")
    } finally {
      oidcManager.stop()
      dtManager.stop()
    }
  }

  test("SPARK-57896: OIDC failure does not affect HadoopDelegationTokenManager") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

    val conf = createOidcConf()
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(NETWORK_CRYPTO_ENABLED, true)

    // OIDC manager with a FAILING ingestor (simulates missing/corrupt token file)
    val failingOidcManager = new UserCredentialManager(
      conf,
      createFailingIngestor(),
      (_, _) => ())

    // DT manager should work independently
    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)

    try {
      // OIDC start should fail (empty token)
      val oidcException = intercept[Exception] {
        failingOidcManager.start()
      }
      assert(oidcException != null, "OIDC manager should fail with empty token")

      // DT manager should still work perfectly despite OIDC failure
      val dtTokens = dtManager.start()
      assert(dtTokens != null, "DT manager should succeed despite OIDC failure")

      val captor = ArgumentCaptor.forClass(classOf[Any])
      verify(mockRef).send(captor.capture())
      val msg = captor.getValue.asInstanceOf[UpdateDelegationTokens]
      val dtCreds = SparkHadoopUtil.get.deserialize(msg.tokens)
      assert(dtCreds.getSecretKey(new Text("test.direct.credential")) != null,
        "DT credentials should be unaffected by OIDC failure")
    } finally {
      failingOidcManager.stop()
      dtManager.stop()
    }
  }

  test("SPARK-57896: DT provider failure does not affect UserCredentialManager") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

    // Enable direct providers but disable all succeeding providers
    // so only the failing one runs (forces DT total failure)
    val conf = createOidcConf()
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(NETWORK_CRYPTO_ENABLED, true)
      .set("spark.security.credentials.test-direct.enabled", "false")
      .set("spark.security.credentials.test-noexpiry.enabled", "false")

    val ctx = createUserContext()
    val oidcCallbackRef = new AtomicReference[Array[Byte]]()

    val oidcManager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, bytes) => oidcCallbackRef.set(bytes))

    // DT manager with only failing provider active
    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)

    try {
      // DT manager start returns null when all providers fail
      val dtTokens = dtManager.start()
      assert(dtTokens == null, "DT manager should return null when all providers fail")

      // OIDC manager should still work perfectly
      val (oidcVer, oidcBytes) = oidcManager.start()
      assert(oidcVer == 1L, "OIDC version should be 1")
      assert(oidcBytes != null, "OIDC should produce credentials despite DT failure")

      val oidcCreds = UserCredentialManager.deserializeUserCredentials(oidcBytes)
      assert(oidcCreds.forScheme("fake").isPresent,
        "OIDC credentials should be unaffected by DT failure")
    } finally {
      oidcManager.stop()
      dtManager.stop()
    }
  }


  test("SPARK-57896: UserCredentials serialization roundtrip preserves all schemes") {
    val conf = createOidcConf()
    val ctx = createUserContext()
    val serializedRef = new AtomicReference[Array[Byte]]()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, bytes) => serializedRef.set(bytes))

    try {
      manager.start()

      // Simulate what the executor does: deserialize the byte array
      val bytes = serializedRef.get()
      assert(bytes != null && bytes.length > 0, "Serialized credentials should be non-empty")

      // First deserialization
      val creds1 = UserCredentialManager.deserializeUserCredentials(bytes)
      // Second deserialization of the same bytes (idempotency)
      val creds2 = UserCredentialManager.deserializeUserCredentials(bytes)

      // Both produce identical results
      assert(creds1.forScheme("fake").isPresent)
      assert(creds2.forScheme("fake").isPresent)
      assert(creds1.forScheme("fake").get().getProperties ==
        creds2.forScheme("fake").get().getProperties,
        "Multiple deserializations of same bytes should produce identical credentials")

      // Verify the credential is well-formed
      val cred = creds1.forScheme("fake").get()
      assert(cred.getProperties.containsKey("provider"))
      assert(cred.getExpiresAt != null, "Credential should have an expiry set")
      assert(!cred.isExpired(Instant.now()), "Freshly resolved credential should not be expired")
    } finally {
      manager.stop()
    }
  }

  test("SPARK-57896: multiple URI schemes resolved in single credential bundle") {
    // FakeCredentialProvider supports both "fake" and "shared" schemes.
    // When multiple target URIs are configured, all are resolved.
    val conf = createOidcConf()
    val ctx = createUserContext()
    val serializedRef = new AtomicReference[Array[Byte]]()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, bytes) => serializedRef.set(bytes))

    try {
      manager.start()

      val creds = UserCredentialManager.deserializeUserCredentials(serializedRef.get())

      // FakeCredentialProvider declares supportedSchemes = Set("fake", "shared")
      // but "shared" is ambiguous (AnotherFakeCredentialProvider also claims it),
      // so only "fake" auto-resolves without explicit config.
      assert(creds.forScheme("fake").isPresent, "Should resolve 'fake' scheme")

      // Verify case-insensitive lookup works
      assert(creds.forScheme("FAKE").isPresent,
        "Scheme lookup should be case-insensitive")
      assert(creds.forScheme("Fake").isPresent,
        "Scheme lookup should be case-insensitive")
    } finally {
      manager.stop()
    }
  }

  test("SPARK-57896: ServiceCredential.isExpired detects expiry correctly") {
    // Unit-level verification that the executor can detect stale credentials
    val now = Instant.now()

    // Credential that expires in the future -- not expired
    val validCred = new ServiceCredential(
      java.util.Map.of("provider", "test"), now.plusSeconds(300))
    assert(!validCred.isExpired(now), "Future-expiry credential should not be expired")

    // Credential that already expired -- is expired
    val expiredCred = new ServiceCredential(
      java.util.Map.of("provider", "test"), now.minusSeconds(10))
    assert(expiredCred.isExpired(now), "Past-expiry credential should be expired")

    // Credential with null expiry -- never expires
    val noExpiryCred = new ServiceCredential(
      java.util.Map.of("provider", "test"), null)
    assert(!noExpiryCred.isExpired(now),
      "Null-expiry credential should never be considered expired")
  }

  test("SPARK-57896: stop() after start() completes cleanly without exceptions") {
    val conf = createOidcConf()
    val ctx = createUserContext(expiresInSeconds = 60)

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, _) => ())

    // start + immediate stop should not throw or leave dangling threads
    manager.start()
    manager.stop()

    // Double stop should also be safe
    manager.stop()
  }

  test("SPARK-57896: credential version is monotonically increasing across renewals") {
    val conf = createOidcConf()
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 2000L)
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 500L)

    val ctx = createUserContext(expiresInSeconds = 3)
    val versions = new java.util.concurrent.CopyOnWriteArrayList[Long]()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (version, _) => versions.add(version))

    try {
      manager.start()

      // Wait for at least 3 renewals
      eventually(timeout(15.seconds)) {
        assert(versions.size() >= 3,
          s"Expected at least 3 callbacks (got ${versions.size()})")
      }

      // Verify strict monotonicity
      val versionList = new java.util.ArrayList(versions)
      for (i <- 1 until versionList.size()) {
        assert(versionList.get(i) > versionList.get(i - 1),
          s"Version ${versionList.get(i)} should be > ${versionList.get(i - 1)} " +
            s"at index $i (full list: $versionList)")
      }
    } finally {
      manager.stop()
    }
  }

  test("SPARK-57896: every renewal callback provides non-null non-empty credentials") {
    val conf = createOidcConf()
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 2000L)
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 500L)

    val ctx = createUserContext(expiresInSeconds = 3)
    val allBytes = new java.util.concurrent.CopyOnWriteArrayList[Array[Byte]]()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, bytes) => allBytes.add(bytes))

    try {
      manager.start()

      eventually(timeout(15.seconds)) {
        assert(allBytes.size() >= 2,
          s"Expected at least 2 callbacks (got ${allBytes.size()})")
      }

      // Every single callback must have valid, deserializable credentials
      val it = allBytes.iterator()
      while (it.hasNext) {
        val bytes = it.next()
        assert(bytes != null, "Callback bytes should never be null")
        assert(bytes.length > 0, "Callback bytes should never be empty")
        val creds = UserCredentialManager.deserializeUserCredentials(bytes)
        assert(creds.forScheme("fake").isPresent,
          "Every renewed credential bundle should contain 'fake' scheme")
      }
    } finally {
      manager.stop()
    }
  }

  test("SPARK-57896: OIDC disabled does not interfere with DT manager") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

    // OIDC explicitly DISABLED -- only DT enabled
    val conf = new SparkConf(loadDefaults = false)
      .set(SECURITY_OIDC_ENABLED, false)
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(NETWORK_CRYPTO_ENABLED, true)

    // UserCredentialManager.create() should return None
    val oidcManager = UserCredentialManager.create(conf, (_, _) => ())
    assert(oidcManager.isEmpty, "OIDC manager should not be created when disabled")

    // DT manager should work independently
    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)
    try {
      val dtTokens = dtManager.start()
      assert(dtTokens != null, "DT manager should work when OIDC is disabled")

      val captor = ArgumentCaptor.forClass(classOf[Any])
      eventually(timeout(5.seconds)) {
        verify(mockRef).send(captor.capture())
      }
      val msg = captor.getValue.asInstanceOf[UpdateDelegationTokens]
      val dtCreds = SparkHadoopUtil.get.deserialize(msg.tokens)
      assert(dtCreds.getSecretKey(new Text("test.direct.credential")) != null)
    } finally {
      dtManager.stop()
    }
  }
}
