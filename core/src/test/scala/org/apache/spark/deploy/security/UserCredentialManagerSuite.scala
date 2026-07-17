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

import java.time.Instant
import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.security._

class UserCredentialManagerSuite extends SparkFunSuite {

  private def createSparkConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SECURITY_CREDENTIALS_ENABLED, true)
      .set(SECURITY_CREDENTIALS_IDENTITY_TOKEN_FILE, "/tmp/fake-token")
      .set(SECURITY_CREDENTIALS_RENEWAL_SAFETY_MARGIN, 5000L) // 5s for tests
      .set(SECURITY_CREDENTIALS_RENEWAL_MIN_INTERVAL, 1000L)  // 1s for tests
  }

  private def createUserContext(
      expiresInSeconds: Long = 300): UserContext = {
    new UserContext(
      "test-user",
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

  test("start() acquires initial credentials and invokes callback") {
    val conf = createSparkConf()
    val ctx = createUserContext()
    val callbackRef = new AtomicReference[Array[Byte]]()

    // Use CredentialProviderLoader with the FakeCredentialProvider from ServiceLoader
    // FakeCredentialProvider supports scheme "fake"
    conf.set("spark.security.credentials.provider.fake",
      "org.apache.spark.security.FakeCredentialProvider")

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      bytes => callbackRef.set(bytes))

    try {
      val result = manager.start()
      assert(result != null, "start() should return serialized credentials")
      assert(callbackRef.get() != null, "callback should have been invoked")

      // Verify deserialization
      val creds = UserCredentialManager.deserializeUserCredentials(result)
      assert(creds.forScheme("fake").isPresent,
        "Should have credentials for 'fake' scheme")
      assert(creds.forScheme("fake").get().getProperties.get("provider") === "fake")
    } finally {
      manager.stop()
    }
  }

  test("start() throws when TokenIngestor returns empty (fail-fast)") {
    val conf = createSparkConf()
    conf.set("spark.security.credentials.provider.fake",
      "org.apache.spark.security.FakeCredentialProvider")

    val manager = new UserCredentialManager(
      conf,
      createFailingIngestor(),
      _ => ())

    try {
      val ex = intercept[IllegalStateException] {
        manager.start()
      }
      assert(ex.getMessage.contains("identity token file is missing or malformed"))
    } finally {
      manager.stop()
    }
  }

  test("serialization round-trip of UserCredentials") {
    val props = new util.HashMap[String, String]()
    props.put("fs.s3a.access.key", "AKIAEXAMPLE")
    props.put("fs.s3a.secret.key", "secret123")
    props.put("fs.s3a.session.token", "token456")
    val cred = new ServiceCredential(props, Instant.now().plusSeconds(3600))

    val credsMap = new util.HashMap[String, ServiceCredential]()
    credsMap.put("s3a", cred)
    val original = new UserCredentials(credsMap)

    val conf = createSparkConf()
    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())

    try {
      val serialized = manager.serializeUserCredentials(original)
      val deserialized = UserCredentialManager.deserializeUserCredentials(serialized)

      assert(deserialized.forScheme("s3a").isPresent)
      val restored = deserialized.forScheme("s3a").get()
      assert(restored.getProperties.get("fs.s3a.access.key") === "AKIAEXAMPLE")
      assert(restored.getProperties.get("fs.s3a.secret.key") === "secret123")
      assert(restored.getProperties.get("fs.s3a.session.token") === "token456")
      assert(restored.getExpiresAt === cred.getExpiresAt)
    } finally {
      manager.stop()
    }
  }

  test("deserialization rejects unauthorized classes (ObjectInputFilter)") {
    // Serialize an arbitrary object (not UserCredentials)
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(new java.util.ArrayList[String]())
    oos.close()

    val ex = intercept[Exception] {
      UserCredentialManager.deserializeUserCredentials(bos.toByteArray)
    }
    // Should fail due to ObjectInputFilter rejecting the class
    assert(ex != null)
  }

  test("computeRenewalDelay respects safetyMargin and minInterval") {
    val conf = createSparkConf()
      .set(SECURITY_CREDENTIALS_RENEWAL_SAFETY_MARGIN, 10000L) // 10s
      .set(SECURITY_CREDENTIALS_RENEWAL_MIN_INTERVAL, 5000L)   // 5s

    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())
    try {
      // Token expires in 60s, credential expires in 30s
      // Expected: min(60s, 30s) - 10s = 20s
      val ctx = createUserContext(expiresInSeconds = 60)
      val credExpiry = Some(Instant.now().plusSeconds(30))
      val delay = manager.computeRenewalDelay(ctx, credExpiry)

      // Allow 1s tolerance for timing
      assert(delay >= 19000 && delay <= 21000,
        s"Expected ~20000ms, got ${delay}ms")
    } finally {
      manager.stop()
    }
  }

  test("computeRenewalDelay uses minInterval when expiry is very close") {
    val conf = createSparkConf()
      .set(SECURITY_CREDENTIALS_RENEWAL_SAFETY_MARGIN, 10000L)
      .set(SECURITY_CREDENTIALS_RENEWAL_MIN_INTERVAL, 5000L)

    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())
    try {
      // Token expires in 5s, safetyMargin is 10s -> computed delay would be negative
      // Should be bounded by minInterval (5s)
      val ctx = createUserContext(expiresInSeconds = 5)
      val credExpiry = Some(Instant.now().plusSeconds(5))
      val delay = manager.computeRenewalDelay(ctx, credExpiry)

      assert(delay === 5000L, s"Expected minInterval (5000ms), got ${delay}ms")
    } finally {
      manager.stop()
    }
  }

  test("computeRenewalDelay uses identity token expiry when credential has no expiry") {
    val conf = createSparkConf()
      .set(SECURITY_CREDENTIALS_RENEWAL_SAFETY_MARGIN, 10000L) // 10s
      .set(SECURITY_CREDENTIALS_RENEWAL_MIN_INTERVAL, 5000L)   // 5s

    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())
    try {
      // Token expires in 60s, no credential expiry
      // Expected: 60s - 10s = 50s
      val ctx = createUserContext(expiresInSeconds = 60)
      val delay = manager.computeRenewalDelay(ctx, None)

      assert(delay >= 49000 && delay <= 51000,
        s"Expected ~50000ms, got ${delay}ms")
    } finally {
      manager.stop()
    }
  }

  test("computeRenewalDelay returns default when no expiry information") {
    val conf = createSparkConf()
    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())
    try {
      // UserContext with null expiresAt
      val ctx = new UserContext(
        "test-user", "https://issuer.example.com", "fake.jwt.token",
        Instant.now(), null)
      val delay = manager.computeRenewalDelay(ctx, None)

      // Should be DEFAULT_RENEWAL_INTERVAL_NO_EXPIRY_MS = 7 minutes = 420000ms
      assert(delay === 420000L, s"Expected 420000ms (7 min), got ${delay}ms")
    } finally {
      manager.stop()
    }
  }

  test("computeBackoffDelay increases exponentially") {
    val conf = createSparkConf()
      .set(SECURITY_CREDENTIALS_RENEWAL_MIN_INTERVAL, 1000L)

    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())
    try {
      val failuresField = classOf[UserCredentialManager].getDeclaredField("consecutiveFailures")
      failuresField.setAccessible(true)
      val atomicFailures = failuresField.get(manager)
        .asInstanceOf[java.util.concurrent.atomic.AtomicInteger]

      // First failure: base = minInterval * 2^0 = 1000ms
      atomicFailures.set(1)
      val delay1 = manager.computeBackoffDelay()
      assert(delay1 >= 1000 && delay1 <= 1200,
        s"First backoff should be ~1000-1100ms, got ${delay1}ms")

      // Second failure: base = minInterval * 2^1 = 2000ms
      atomicFailures.set(2)
      val delay2 = manager.computeBackoffDelay()
      assert(delay2 >= 2000 && delay2 <= 2300,
        s"Second backoff should be ~2000-2200ms, got ${delay2}ms")

      // Third failure: base = minInterval * 2^2 = 4000ms
      atomicFailures.set(3)
      val delay3 = manager.computeBackoffDelay()
      assert(delay3 >= 4000 && delay3 <= 4500,
        s"Third backoff should be ~4000-4400ms, got ${delay3}ms")
    } finally {
      manager.stop()
    }
  }

  test("computeBackoffDelay is capped at maxBackoffMs") {
    val conf = createSparkConf()
      .set(SECURITY_CREDENTIALS_RENEWAL_MIN_INTERVAL, 1000L)

    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())
    try {
      val failuresField = classOf[UserCredentialManager].getDeclaredField("consecutiveFailures")
      failuresField.setAccessible(true)
      val atomicFailures = failuresField.get(manager)
        .asInstanceOf[java.util.concurrent.atomic.AtomicInteger]

      // Many failures: should be capped at 10 minutes (600000ms)
      atomicFailures.set(20)
      val delay = manager.computeBackoffDelay()
      assert(delay <= 660000L, // 600000 + 10% jitter
        s"Backoff should be capped at ~600000ms + jitter, got ${delay}ms")
    } finally {
      manager.stop()
    }
  }

  test("computeBackoffDelay handles zero consecutiveFailures defensively") {
    val conf = createSparkConf()
      .set(SECURITY_CREDENTIALS_RENEWAL_MIN_INTERVAL, 1000L)

    val manager = new UserCredentialManager(conf, createFailingIngestor(), _ => ())
    try {
      val failuresField = classOf[UserCredentialManager].getDeclaredField("consecutiveFailures")
      failuresField.setAccessible(true)
      val atomicFailures = failuresField.get(manager)
        .asInstanceOf[java.util.concurrent.atomic.AtomicInteger]

      // Edge case: 0 failures (should not happen in practice, but defensive)
      atomicFailures.set(0)
      val delay = manager.computeBackoffDelay()
      // shiftAmount = max(0, min(0-1, 6)) = max(0, -1) = 0
      // baseDelay = 1000 * 2^0 = 1000
      assert(delay >= 1000 && delay <= 1200,
        s"With 0 failures, backoff should be ~1000ms, got ${delay}ms")
    } finally {
      manager.stop()
    }
  }

  test("UserCredentialManager.create returns None when disabled") {
    val conf = new SparkConf(loadDefaults = false)
      .set(SECURITY_CREDENTIALS_ENABLED, false)

    val result = UserCredentialManager.create(conf, _ => ())
    assert(result.isEmpty)
  }

  test("UserCredentialManager.create returns Some when enabled with valid config") {
    val conf = createSparkConf()
    val result = UserCredentialManager.create(conf, _ => ())
    assert(result.isDefined)
  }

  test("UserCredentialManager.create throws when enabled without token file") {
    val conf = new SparkConf(loadDefaults = false)
      .set(SECURITY_CREDENTIALS_ENABLED, true)
    // Deliberately not setting SECURITY_CREDENTIALS_IDENTITY_TOKEN_FILE

    val ex = intercept[IllegalArgumentException] {
      UserCredentialManager.create(conf, _ => ())
    }
    assert(ex.getMessage.contains("spark.security.credentials.identityToken.file"))
  }

  test("renewal is scheduled after successful credential acquisition") {
    val conf = createSparkConf()
    conf.set("spark.security.credentials.provider.fake",
      "org.apache.spark.security.FakeCredentialProvider")
    val ctx = createUserContext(expiresInSeconds = 60)
    var callbackCount = 0

    val manager = new UserCredentialManager(
      conf,
      createIngestor(ctx),
      _ => { callbackCount += 1 })

    try {
      val result = manager.start()
      assert(result != null)
      assert(callbackCount === 1, "callback should be invoked once on start")
    } finally {
      manager.stop()
    }
  }

  test("stop() after start() does not throw") {
    val conf = createSparkConf()
    conf.set("spark.security.credentials.provider.fake",
      "org.apache.spark.security.FakeCredentialProvider")
    val ctx = createUserContext()

    val manager = new UserCredentialManager(conf, createIngestor(ctx), _ => ())
    manager.start()
    // Should not throw
    manager.stop()
  }
}
