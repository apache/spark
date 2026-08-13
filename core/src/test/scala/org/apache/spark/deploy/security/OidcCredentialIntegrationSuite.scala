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

import java.io.File
import java.nio.file.Files
import java.time.Instant
import java.util.Optional
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, verify}
import org.scalatest.concurrent.Eventually.{eventually, timeout}

import org.apache.spark.{SparkConf, SparkFunSuite, VersionedCredentials}
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
 * 6. TaskDescription credentials are applied to the executor store with version guard
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
    try {
      if (tokenFile != null) tokenFile.delete()
    } finally {
      CredentialProviderLoader.resetForTesting()
      super.afterEach()
    }
  }

  private def writeTokenFile(content: String): Unit = {
    Files.writeString(tokenFile.toPath, content)
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
    val now = Instant.now()
    new UserContext(
      principal,
      "https://issuer.example.com",
      "fake.jwt.token",
      now,
      now.plusSeconds(expiresInSeconds))
  }

  private def createIngestor(ctx: UserContext): TokenIngestor = {
    new TokenIngestor {
      override def load(): Optional[UserContext] = Optional.of(ctx)
    }
  }

  private def createFreshExpiryIngestor(expiresInSeconds: Long): TokenIngestor = {
    new TokenIngestor {
      override def load(): Optional[UserContext] =
        Optional.of(createUserContext(expiresInSeconds = expiresInSeconds))
    }
  }

  private def createFailingIngestor(): TokenIngestor = {
    new TokenIngestor {
      override def load(): Optional[UserContext] = Optional.empty()
    }
  }

  test("OIDC credential delivery via update callback") {
    val conf = createOidcConf()
    val ctx = createUserContext()
    val callbackRef = new AtomicReference[Array[Byte]]()
    val callbackVersion = new AtomicLong(0L)

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (version, bytes) => {
        callbackVersion.set(version)
        callbackRef.set(bytes)
      })

    try {
      val (version, initialBytes) = manager.start()

      assert(version == 1L, "Initial version should be 1")
      assert(initialBytes != null, "Initial credentials should not be null")

      val credentials = UserCredentialManager.deserializeUserCredentials(initialBytes)
      assert(credentials != null, "Deserialized credentials should not be null")

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

  test("credential refresh works end-to-end on expiry") {
    val conf = createOidcConf()
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 2000L)
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 500L)

    val updateCount = new AtomicInteger(0)
    val latestVersion = new AtomicLong(0L)

    // Return a fresh UserContext on each load() so each renewal gets a genuinely
    // new expiry rather than spinning on an already-expired token.
    val manager = new UserCredentialManager(
      conf,
      createFreshExpiryIngestor(expiresInSeconds = 3),
      (version, _) => {
        latestVersion.set(version)
        updateCount.incrementAndGet()
      })

    try {
      manager.start()
      assert(updateCount.get() == 1, "Should have exactly 1 update after start()")

      eventually(timeout(15.seconds)) {
        assert(updateCount.get() >= 2,
          s"Expected at least 2 updates (got ${updateCount.get()}), " +
            "indicating credential renewal occurred")
      }

      assert(latestVersion.get() >= 2L,
        "Version should be at least 2 after renewal")
    } finally {
      manager.stop()
    }
  }

  test("per-user identity token produces valid credentials") {
    val conf = createOidcConf()

    val userCtx = createUserContext(principal = "alice@corp.example.com")
    val callbackRef = new AtomicReference[Array[Byte]]()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(userCtx),
      (_, bytes) => callbackRef.set(bytes))

    try {
      val (_, initialBytes) = manager.start()

      val credentials = UserCredentialManager.deserializeUserCredentials(initialBytes)
      val fakeCred = credentials.forScheme("fake")
      assert(fakeCred.isPresent,
        "Per-user token should produce credentials for scheme 'fake'")
      assert(fakeCred.get().getProperties.get("provider") == "fake",
        "Per-user credential should come from FakeCredentialProvider")
      assert(!fakeCred.get().isExpired(Instant.now()),
        "Per-user credential should not be expired")

      // Verify the credential is identical in structure to workload token output
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
        assert(workloadFake.get().getProperties == fakeCred.get().getProperties,
          "Per-user and workload tokens should produce identical credential properties")
      } finally {
        workloadManager.stop()
      }
    } finally {
      manager.stop()
    }
  }

  test("UserCredentialManager and HadoopDelegationTokenManager coexist") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

    val conf = createOidcConf()
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(NETWORK_CRYPTO_ENABLED, true)

    val ctx = createUserContext()
    val oidcCallbackRef = new AtomicReference[Array[Byte]]()
    val oidcVersion = new AtomicLong(0L)

    val oidcManager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (version, bytes) => {
        oidcVersion.set(version)
        oidcCallbackRef.set(bytes)
      })

    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)

    try {
      val (oidcVer, oidcBytes) = oidcManager.start()
      assert(oidcVer == 1L)
      assert(oidcBytes != null)

      val dtTokens = dtManager.start()
      assert(dtTokens != null, "DT manager should produce tokens")

      val oidcCreds = UserCredentialManager.deserializeUserCredentials(oidcBytes)
      assert(oidcCreds.forScheme("fake").isPresent,
        "OIDC credentials should contain 'fake' scheme")

      // HadoopDelegationTokenManager.start() is synchronous -- verify directly
      val captor = ArgumentCaptor.forClass(classOf[Any])
      verify(mockRef).send(captor.capture())
      val msg = captor.getValue.asInstanceOf[UpdateDelegationTokens]
      val dtCreds = SparkHadoopUtil.get.deserialize(msg.tokens)
      assert(dtCreds.getSecretKey(new Text("test.direct.credential")) != null,
        "DT credentials should contain test.direct.credential")
      assert(new String(dtCreds.getSecretKey(new Text("test.direct.credential"))) === "test-token",
        "DT credential value should match")

      assert(oidcVersion.get() == 1L, "OIDC version should remain at 1")
    } finally {
      oidcManager.stop()
      dtManager.stop()
    }
  }

  test("OIDC failure does not affect HadoopDelegationTokenManager") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

    val conf = createOidcConf()
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(NETWORK_CRYPTO_ENABLED, true)

    val failingOidcManager = new UserCredentialManager(
      conf,
      createFailingIngestor(),
      (_, _) => ())

    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)

    try {
      // OIDC start should fail with IllegalStateException (missing token)
      val oidcException = intercept[IllegalStateException] {
        failingOidcManager.start()
      }
      assert(oidcException.getMessage.contains(
        "identity token file is missing or malformed"))

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

  test("DT provider failure does not affect UserCredentialManager") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

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

    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)

    try {
      val dtTokens = dtManager.start()
      assert(dtTokens == null, "DT manager should return null when all providers fail")

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

  test("deserialization idempotency preserves credential content") {
    val conf = createOidcConf()
    val ctx = createUserContext()
    val serializedRef = new AtomicReference[Array[Byte]]()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, bytes) => serializedRef.set(bytes))

    try {
      manager.start()

      val bytes = serializedRef.get()
      assert(bytes != null && bytes.length > 0, "Serialized credentials should be non-empty")

      val creds1 = UserCredentialManager.deserializeUserCredentials(bytes)
      val creds2 = UserCredentialManager.deserializeUserCredentials(bytes)

      assert(creds1.forScheme("fake").isPresent)
      assert(creds2.forScheme("fake").isPresent)
      assert(creds1.forScheme("fake").get().getProperties ==
        creds2.forScheme("fake").get().getProperties,
        "Multiple deserializations of same bytes should produce identical credentials")

      val cred = creds1.forScheme("fake").get()
      assert(cred.getProperties.containsKey("provider"))
      assert(cred.getExpiresAt != null, "Credential should have an expiry set")
      assert(!cred.isExpired(Instant.now()), "Freshly resolved credential should not be expired")
    } finally {
      manager.stop()
    }
  }

  test("case-insensitive scheme lookup in credential bundle") {
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
      assert(creds.forScheme("FAKE").isPresent,
        "Scheme lookup should be case-insensitive")
      assert(creds.forScheme("Fake").isPresent,
        "Scheme lookup should be case-insensitive")
    } finally {
      manager.stop()
    }
  }

  test("stop() after start() completes cleanly without exceptions") {
    val conf = createOidcConf()
    val ctx = createUserContext(expiresInSeconds = 60)

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, _) => ())

    manager.start()
    manager.stop()

    // Double stop should also be safe
    manager.stop()
  }

  test("credential version is monotonically increasing across renewals") {
    val conf = createOidcConf()
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 2000L)
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 500L)

    val versions = new java.util.concurrent.CopyOnWriteArrayList[Long]()

    val manager = new UserCredentialManager(
      conf,
      createFreshExpiryIngestor(expiresInSeconds = 3),
      (version, _) => versions.add(version))

    try {
      manager.start()

      eventually(timeout(15.seconds)) {
        assert(versions.size() >= 3,
          s"Expected at least 3 callbacks (got ${versions.size()})")
      }

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

  test("every renewal callback provides non-null non-empty credentials") {
    val conf = createOidcConf()
      .set(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN, 2000L)
      .set(SECURITY_OIDC_RENEWAL_MIN_INTERVAL, 500L)

    val allBytes = new java.util.concurrent.CopyOnWriteArrayList[Array[Byte]]()

    val manager = new UserCredentialManager(
      conf,
      createFreshExpiryIngestor(expiresInSeconds = 3),
      (_, bytes) => allBytes.add(bytes))

    try {
      manager.start()

      eventually(timeout(15.seconds)) {
        assert(allBytes.size() >= 2,
          s"Expected at least 2 callbacks (got ${allBytes.size()})")
      }

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

  test("OIDC disabled does not interfere with DT manager") {
    val hadoopConf = new Configuration()
    val mockRef = mock(classOf[RpcEndpointRef])

    val conf = new SparkConf(loadDefaults = false)
      .set(SECURITY_OIDC_ENABLED, false)
      .set(DIRECT_CREDENTIAL_PROVIDERS_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(NETWORK_CRYPTO_ENABLED, true)

    val oidcManager = UserCredentialManager.create(conf, (_, _) => ())
    assert(oidcManager.isEmpty, "OIDC manager should not be created when disabled")

    val dtManager = new HadoopDelegationTokenManager(conf, hadoopConf, mockRef)
    try {
      val dtTokens = dtManager.start()
      assert(dtTokens != null, "DT manager should work when OIDC is disabled")

      val captor = ArgumentCaptor.forClass(classOf[Any])
      verify(mockRef).send(captor.capture())
      val msg = captor.getValue.asInstanceOf[UpdateDelegationTokens]
      val dtCreds = SparkHadoopUtil.get.deserialize(msg.tokens)
      assert(dtCreds.getSecretKey(new Text("test.direct.credential")) != null)
    } finally {
      dtManager.stop()
    }
  }

  test("credential serialization roundtrip through VersionedCredentials store") {
    val conf = createOidcConf()
    val ctx = createUserContext()

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      (_, _) => ())

    try {
      val (version, credentialBytes) = manager.start()
      assert(version == 1L)

      // Set up the credential store exactly as CoarseGrainedSchedulerBackend does
      val store = new AtomicReference[VersionedCredentials]()
      VersionedCredentials.updateIfNewer(store, version, credentialBytes)

      // Exercise the EXACT expression from TaskSetManager.scala line 607:
      //   Option(env.userCredentials.get()).map(vc => (vc.version, vc.bytes))
      // This is what TaskSetManager reads when constructing TaskDescription.
      val credentialTuple: Option[(Long, Array[Byte])] =
        Option(store.get()).map(vc => (vc.version, vc.bytes))

      assert(credentialTuple.isDefined,
        "Credential store should produce Some when credentials are set")
      assert(credentialTuple.get._1 == 1L,
        "TaskDescription should carry version 1")
      assert(credentialTuple.get._2 === credentialBytes,
        "TaskDescription should carry the credential bytes from the store")

      // Verify the bytes are valid credentials end-to-end
      val creds = UserCredentialManager.deserializeUserCredentials(credentialTuple.get._2)
      assert(creds.forScheme("fake").isPresent,
        "Credentials from TaskDescription should contain 'fake' scheme")

      // Update the store to version 2 (simulating a renewal)
      val v2Bytes = UserCredentialManager.serializeUserCredentials(
        new UserCredentials(java.util.Map.of("fake",
          new ServiceCredential(java.util.Map.of("provider", "fake-v2"),
            Instant.now().plusSeconds(300)))))
      VersionedCredentials.updateIfNewer(store, 2L, v2Bytes)

      // Read again -- same expression as TaskSetManager line 607
      val credentialTupleV2: Option[(Long, Array[Byte])] =
        Option(store.get()).map(vc => (vc.version, vc.bytes))

      assert(credentialTupleV2.get._1 == 2L,
        "After renewal, TaskDescription should carry version 2")

      val credsV2 = UserCredentialManager.deserializeUserCredentials(credentialTupleV2.get._2)
      assert(credsV2.forScheme("fake").get().getProperties.get("provider") == "fake-v2",
        "TaskDescription should carry renewed credentials")

      // Verify empty store produces None (no credentials yet scenario)
      val emptyStore = new AtomicReference[VersionedCredentials]()
      val emptyTuple: Option[(Long, Array[Byte])] =
        Option(emptyStore.get()).map(vc => (vc.version, vc.bytes))
      assert(emptyTuple.isEmpty,
        "Empty store should produce None for TaskDescription.userCredentials")
    } finally {
      manager.stop()
    }
  }
}
