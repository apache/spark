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

package org.apache.spark.security.aws;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.apache.spark.SparkEnv;
import org.apache.spark.VersionedCredentials;
import org.apache.spark.security.ServiceCredential;
import org.apache.spark.security.UserCredentials;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SparkOidcAwsCredentialsProvider}.
 *
 * <p>Unit tests mock SparkEnv.get() to inject controlled credential store state.
 * End-to-end tests use real serialization/deserialization to verify the full path.
 */
public class SparkOidcAwsCredentialsProviderSuite {

  private MockedStatic<SparkEnv> sparkEnvMock;
  private SparkEnv mockEnv;
  private AtomicReference<VersionedCredentials> credentialStore;

  @BeforeEach
  void setUp() {
    mockEnv = mock(SparkEnv.class);
    credentialStore = new AtomicReference<>();
    when(mockEnv.userCredentials()).thenReturn(credentialStore);

    sparkEnvMock = mockStatic(SparkEnv.class);
    sparkEnvMock.when(SparkEnv::get).thenReturn(mockEnv);
  }

  @AfterEach
  void tearDown() {
    sparkEnvMock.close();
  }

  // =========================================================================
  // Happy path
  // =========================================================================

  @Test
  void testResolveCredentialsReturnsValidSessionCredentials() {
    populateStore("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "FwoGZXIvYXdzEBYaDH...", 1L);

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    AwsCredentials creds = provider.resolveCredentials();

    assertInstanceOf(AwsSessionCredentials.class, creds);
    AwsSessionCredentials session = (AwsSessionCredentials) creds;
    assertEquals("AKIAIOSFODNN7EXAMPLE", session.accessKeyId());
    assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", session.secretAccessKey());
    assertEquals("FwoGZXIvYXdzEBYaDH...", session.sessionToken());
  }

  @Test
  void testResolveCredentialsAlwaysReadsFreshFromStore() {
    // Populate v1
    populateStore("key-v1", "secret-v1", "token-v1", 1L);

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();

    // First call returns v1
    AwsSessionCredentials creds1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-v1", creds1.accessKeyId());

    // Update store to v2 (simulates credential refresh)
    populateStore("key-v2", "secret-v2", "token-v2", 2L);

    // Second call returns v2 -- no caching
    AwsSessionCredentials creds2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-v2", creds2.accessKeyId());
    assertEquals("secret-v2", creds2.secretAccessKey());
    assertEquals("token-v2", creds2.sessionToken());
  }

  @Test
  void testResolveCredentialsNeverReturnsStaleAfterRefresh() {
    // Start with v1
    populateStore("key-old", "secret-old", "token-old", 1L);
    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    provider.resolveCredentials(); // consume v1

    // Refresh to v2 (simulates RPC UpdateUserCredentials)
    populateStore("key-new", "secret-new", "token-new", 2L);

    // Multiple subsequent calls all return v2
    for (int i = 0; i < 10; i++) {
      AwsSessionCredentials creds = (AwsSessionCredentials) provider.resolveCredentials();
      assertEquals("key-new", creds.accessKeyId(),
          "Call " + i + " returned stale credentials");
    }
  }

  // =========================================================================
  // Error cases
  // =========================================================================

  @Test
  void testThrowsWhenSparkEnvIsNull() {
    sparkEnvMock.when(SparkEnv::get).thenReturn(null);

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        provider::resolveCredentials);

    assertTrue(ex.getMessage().contains("SparkEnv is not available"));
  }

  @Test
  void testThrowsWhenCredentialStoreIsEmpty() {
    // Store is null (no credentials delivered yet)
    credentialStore.set(null);

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        provider::resolveCredentials);

    assertTrue(ex.getMessage().contains("No credentials available"));
    assertTrue(ex.getMessage().contains("spark.security.oidc.enabled=true"));
  }

  @Test
  void testThrowsWhenS3aSchemeNotPresent() {
    // Populate with a credential that has a different scheme (e.g., "hdfs")
    ServiceCredential hdfsCred = new ServiceCredential(
        Map.of("some.key", "some.value"), Instant.now().plusSeconds(3600));
    UserCredentials credentials = new UserCredentials(Map.of("hdfs", hdfsCred));
    byte[] bytes = serializeCredentials(credentials);
    credentialStore.set(new VersionedCredentials(1L, bytes));

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        provider::resolveCredentials);

    assertTrue(ex.getMessage().contains("No credential found for scheme 's3a'"));
    assertTrue(ex.getMessage().contains("AwsStsCredentialProvider"));
  }

  @Test
  void testThrowsWhenAccessKeyMissing() {
    // Credential with secret and token but no access key
    ServiceCredential incompleteCred = new ServiceCredential(
        Map.of("fs.s3a.secret.key", "secret", "fs.s3a.session.token", "token"),
        Instant.now().plusSeconds(3600));
    UserCredentials credentials = new UserCredentials(Map.of("s3a", incompleteCred));
    byte[] bytes = serializeCredentials(credentials);
    credentialStore.set(new VersionedCredentials(1L, bytes));

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        provider::resolveCredentials);

    assertTrue(ex.getMessage().contains("missing required properties"));
  }

  @Test
  void testThrowsWhenSecretKeyMissing() {
    ServiceCredential incompleteCred = new ServiceCredential(
        Map.of("fs.s3a.access.key", "access", "fs.s3a.session.token", "token"),
        Instant.now().plusSeconds(3600));
    UserCredentials credentials = new UserCredentials(Map.of("s3a", incompleteCred));
    byte[] bytes = serializeCredentials(credentials);
    credentialStore.set(new VersionedCredentials(1L, bytes));

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        provider::resolveCredentials);
    assertTrue(ex.getMessage().contains("missing required properties"));
  }

  @Test
  void testThrowsWhenSessionTokenMissing() {
    ServiceCredential incompleteCred = new ServiceCredential(
        Map.of("fs.s3a.access.key", "access", "fs.s3a.secret.key", "secret"),
        Instant.now().plusSeconds(3600));
    UserCredentials credentials = new UserCredentials(Map.of("s3a", incompleteCred));
    byte[] bytes = serializeCredentials(credentials);
    credentialStore.set(new VersionedCredentials(1L, bytes));

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        provider::resolveCredentials);
    assertTrue(ex.getMessage().contains("missing required properties"));
  }

  // =========================================================================
  // End-to-end: real serialization roundtrip
  // =========================================================================

  @Test
  void testEndToEndSerializationRoundtrip() {
    // Simulate the full driver->executor path:
    // 1. Driver creates ServiceCredential with S3A properties
    // 2. Driver wraps in UserCredentials
    // 3. Driver serializes via UserCredentialManager.serializeUserCredentials
    //    (Java ObjectOutputStream)
    // 4. Bytes stored in VersionedCredentials
    // 5. Executor reads via SparkOidcAwsCredentialsProvider.resolveCredentials()

    String expectedAccessKey = "AKIAI44QH8DHBEXAMPLE";
    String expectedSecretKey = "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY";
    String expectedToken = "AQoDYXdzEJr...<very-long-session-token>...";

    ServiceCredential s3aCred = new ServiceCredential(Map.of(
        "fs.s3a.access.key", expectedAccessKey,
        "fs.s3a.secret.key", expectedSecretKey,
        "fs.s3a.session.token", expectedToken
    ), Instant.now().plusSeconds(3600));

    UserCredentials userCreds = new UserCredentials(Map.of("s3a", s3aCred));
    byte[] serialized = serializeCredentials(userCreds);

    // Place in store (as executor would receive via TaskDescription)
    credentialStore.set(new VersionedCredentials(42L, serialized));

    // Resolve -- full path
    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    AwsSessionCredentials result = (AwsSessionCredentials) provider.resolveCredentials();

    assertEquals(expectedAccessKey, result.accessKeyId());
    assertEquals(expectedSecretKey, result.secretAccessKey());
    assertEquals(expectedToken, result.sessionToken());
  }

  @Test
  void testEndToEndVersionGuardWithMultipleUpdates() {
    // Simulate credential refresh cycle:
    // v1 arrives via TaskDescription, v2 arrives via RPC, stale v1 arrives again

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();

    // v1 arrives
    populateStore("key-v1", "secret-v1", "token-v1", 1L);
    AwsSessionCredentials r1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-v1", r1.accessKeyId());

    // v2 arrives (credential refresh from driver)
    populateStoreWithVersionGuard("key-v2", "secret-v2", "token-v2", 2L);
    AwsSessionCredentials r2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-v2", r2.accessKeyId());

    // Stale v1 arrives (delayed TaskDescription) -- version guard rejects it
    populateStoreWithVersionGuard("key-v1-stale", "secret-v1-stale", "token-v1-stale", 1L);
    AwsSessionCredentials r3 = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-v2", r3.accessKeyId(), "Stale v1 should not overwrite v2");
  }

  @Test
  void testEndToEndSchemeNormalizationByUserCredentials() {
    // UserCredentials constructor normalizes scheme keys to lowercase.
    // This test verifies our provider works correctly with that normalized store.
    ServiceCredential s3aCred = new ServiceCredential(Map.of(
        "fs.s3a.access.key", "key-normalized",
        "fs.s3a.secret.key", "secret-normalized",
        "fs.s3a.session.token", "token-normalized"
    ), Instant.now().plusSeconds(3600));

    // UserCredentials normalizes "s3a" to lowercase internally
    UserCredentials userCreds = new UserCredentials(Map.of("s3a", s3aCred));
    byte[] serialized = serializeCredentials(userCreds);
    credentialStore.set(new VersionedCredentials(1L, serialized));

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    AwsSessionCredentials result = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-normalized", result.accessKeyId());
  }

  @Test
  void testEndToEndWithExpiredCredentialStillReturns() {
    // Expired credentials should still be returned -- the provider does NOT check expiry.
    // S3A will get a 403 and retry, triggering another resolveCredentials() which
    // should by then have fresh creds from the renewal loop.
    ServiceCredential expiredCred = new ServiceCredential(Map.of(
        "fs.s3a.access.key", "key-expired",
        "fs.s3a.secret.key", "secret-expired",
        "fs.s3a.session.token", "token-expired"
    ), Instant.now().minusSeconds(3600)); // expired 1 hour ago

    UserCredentials userCreds = new UserCredentials(Map.of("s3a", expiredCred));
    byte[] serialized = serializeCredentials(userCreds);
    credentialStore.set(new VersionedCredentials(1L, serialized));

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    AwsSessionCredentials result = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-expired", result.accessKeyId());
  }

  @Test
  void testEndToEndMultipleSchemesBundled() {
    // UserCredentials can have multiple schemes -- we only read s3a
    ServiceCredential s3aCred = new ServiceCredential(Map.of(
        "fs.s3a.access.key", "s3a-key",
        "fs.s3a.secret.key", "s3a-secret",
        "fs.s3a.session.token", "s3a-token"
    ), Instant.now().plusSeconds(3600));
    ServiceCredential abfsCred = new ServiceCredential(
        Map.of("fs.azure.account.key", "azure-key"),
        Instant.now().plusSeconds(3600));

    UserCredentials userCreds = new UserCredentials(Map.of(
        "s3a", s3aCred,
        "abfs", abfsCred
    ));
    byte[] serialized = serializeCredentials(userCreds);
    credentialStore.set(new VersionedCredentials(1L, serialized));

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    AwsSessionCredentials result = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("s3a-key", result.accessKeyId());
    assertEquals("s3a-secret", result.secretAccessKey());
    assertEquals("s3a-token", result.sessionToken());
  }

  @Test
  void testImplementsAwsCredentialsProviderInterface() {
    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    assertInstanceOf(software.amazon.awssdk.auth.credentials.AwsCredentialsProvider.class,
        provider);
  }

  // =========================================================================
  // Concurrency: thread-safety of resolveCredentials
  // =========================================================================

  @Test
  void testRapidStoreUpdatesReturnLatestVersion() throws InterruptedException {
    // Note: Mockito mockStatic is scoped to the declaring thread by default.
    // We test concurrency by rapidly updating the store between sequential calls,
    // verifying atomic read consistency from a single thread (which is what the
    // real AtomicReference guarantees for the multi-threaded executor case).
    populateStore("key-initial", "secret-initial", "token-initial", 1L);
    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();

    // Rapidly alternate between reading and updating
    for (int i = 0; i < 100; i++) {
      AwsSessionCredentials creds = (AwsSessionCredentials) provider.resolveCredentials();
      // Every read must return a complete, non-null credential set
      assertNotNull(creds.accessKeyId());
      assertNotNull(creds.secretAccessKey());
      assertNotNull(creds.sessionToken());

      // Update store mid-loop (simulates concurrent RPC updates)
      populateStore("key-" + i, "secret-" + i, "token-" + i, (long) (i + 2));
    }

    // Final read must return the latest version
    AwsSessionCredentials finalCreds = (AwsSessionCredentials) provider.resolveCredentials();
    assertEquals("key-99", finalCreds.accessKeyId());
  }

  // =========================================================================
  // Auto-config integration (verifies contract with AwsStsCredentialProvider)
  // =========================================================================

  @Test
  void testClassNameMatchesAutoConfigValue() {
    AwsStsCredentialProvider stsProvider = new AwsStsCredentialProvider();
    stsProvider.init(Map.of(
        "spark.security.oidc.aws.roleArn", "arn:aws:iam::123456789012:role/test",
        "spark.security.oidc.aws.region", "us-east-1"));
    Map<String, String> props = stsProvider.additionalSparkProperties();
    assertEquals(
        SparkOidcAwsCredentialsProvider.class.getName(),
        props.get("spark.hadoop.fs.s3a.aws.credentials.provider"));
    stsProvider.close();
  }

  // =========================================================================
  // Cache behavior: same version returns same instance (no re-deserialization)
  // =========================================================================

  @Test
  void testCacheHitReturnsSameInstanceWhenVersionUnchanged() {
    populateStore("key-1", "secret-1", "token-1", 1L);

    SparkOidcAwsCredentialsProvider provider = new SparkOidcAwsCredentialsProvider();
    AwsCredentials result1 = provider.resolveCredentials();
    AwsCredentials result2 = provider.resolveCredentials();

    // Same cached instance proves deserialization was skipped on second call
    assertSame(result1, result2);
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private void populateStore(String accessKey, String secretKey, String sessionToken,
      long version) {
    ServiceCredential s3aCred = new ServiceCredential(Map.of(
        "fs.s3a.access.key", accessKey,
        "fs.s3a.secret.key", secretKey,
        "fs.s3a.session.token", sessionToken
    ), Instant.now().plusSeconds(3600));
    UserCredentials userCreds = new UserCredentials(Map.of("s3a", s3aCred));
    byte[] bytes = serializeCredentials(userCreds);
    credentialStore.set(new VersionedCredentials(version, bytes));
  }

  private void populateStoreWithVersionGuard(String accessKey, String secretKey,
      String sessionToken, long version) {
    ServiceCredential s3aCred = new ServiceCredential(Map.of(
        "fs.s3a.access.key", accessKey,
        "fs.s3a.secret.key", secretKey,
        "fs.s3a.session.token", sessionToken
    ), Instant.now().plusSeconds(3600));
    UserCredentials userCreds = new UserCredentials(Map.of("s3a", s3aCred));
    byte[] bytes = serializeCredentials(userCreds);
    VersionedCredentials.updateIfNewer(credentialStore, version, bytes);
  }

  /**
   * Serialize UserCredentials to bytes using Java ObjectOutputStream.
   * This mirrors UserCredentialManager.serializeUserCredentials which is
   * package-private to org.apache.spark.deploy.security.
   */
  private static byte[] serializeCredentials(UserCredentials credentials) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(credentials);
        oos.flush();
      }
      return bos.toByteArray();
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to serialize UserCredentials", e);
    }
  }
}
