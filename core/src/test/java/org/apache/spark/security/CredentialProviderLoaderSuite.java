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

package org.apache.spark.security;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CredentialProviderLoader} covering ServiceLoader discovery,
 * single-candidate resolution, ambiguity handling, explicit selection, and error cases.
 */
public class CredentialProviderLoaderSuite {

  @BeforeEach
  public void setUp() {
    CredentialProviderLoader.resetForTesting();
  }

  @Test
  public void testServiceLoaderDiscoversFakeProviders() {
    // The "fake" scheme is supported only by FakeCredentialProvider (single candidate).
    // If discovery works, providerFor should find it.
    Map<String, String> conf = Map.of();
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent(), "ServiceLoader should discover FakeCredentialProvider");
    assertInstanceOf(FakeCredentialProvider.class, result.get());
  }

  @Test
  public void testSingleCandidateSchemeResolvesWithNoConf() {
    // "fake" is supported only by FakeCredentialProvider
    Map<String, String> conf = Map.of();
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent());
    assertInstanceOf(FakeCredentialProvider.class, result.get());
  }

  @Test
  public void testSharedSchemeWithNoConfThrowsAmbiguity() {
    // "shared" is supported by both FakeCredentialProvider and AnotherFakeCredentialProvider
    Map<String, String> conf = Map.of();
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CredentialProviderLoader.providerFor("shared", conf));
    assertTrue(e.getMessage().contains("Multiple credential providers"),
        "Should mention multiple providers: " + e.getMessage());
    assertTrue(e.getMessage().contains("shared"),
        "Should mention the scheme: " + e.getMessage());
    assertTrue(e.getMessage().contains("spark.security.credentials.provider.shared"),
        "Should mention the config key: " + e.getMessage());
    assertTrue(e.getMessage().contains(FakeCredentialProvider.class.getName()),
        "Should list FakeCredentialProvider: " + e.getMessage());
    assertTrue(e.getMessage().contains(AnotherFakeCredentialProvider.class.getName()),
        "Should list AnotherFakeCredentialProvider: " + e.getMessage());
  }

  @Test
  public void testEmptyStringConfTreatedAsUnsetThrowsAmbiguity() {
    // An empty-string value for the explicit provider conf key should be equivalent to unset,
    // meaning the ambiguity error is still raised for multi-candidate schemes.
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.security.credentials.provider.shared", "");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CredentialProviderLoader.providerFor("shared", conf));
    assertTrue(e.getMessage().contains("Multiple credential providers"),
        "Empty conf value should behave as unset: " + e.getMessage());
  }

  @Test
  public void testSharedSchemeWithExplicitConfSelectsFake() {
    Map<String, String> conf = Map.of(
        "spark.security.credentials.provider.shared",
        FakeCredentialProvider.class.getName());
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("shared", conf);
    assertTrue(result.isPresent());
    assertInstanceOf(FakeCredentialProvider.class, result.get());
  }

  @Test
  public void testSharedSchemeWithExplicitConfSelectsAnother() {
    Map<String, String> conf = Map.of(
        "spark.security.credentials.provider.shared",
        AnotherFakeCredentialProvider.class.getName());
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("shared", conf);
    assertTrue(result.isPresent());
    assertInstanceOf(AnotherFakeCredentialProvider.class, result.get());
  }

  @Test
  public void testConfNamingUnknownClassThrowsClearError() {
    Map<String, String> conf = Map.of(
        "spark.security.credentials.provider.fake",
        "com.example.NonExistentProvider");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CredentialProviderLoader.providerFor("fake", conf));
    assertTrue(e.getMessage().contains("com.example.NonExistentProvider"),
        "Should mention the configured class: " + e.getMessage());
    assertTrue(e.getMessage().contains("fake"),
        "Should mention the scheme: " + e.getMessage());
    assertTrue(e.getMessage().contains(FakeCredentialProvider.class.getName()),
        "Should list available candidate(s): " + e.getMessage());
  }

  @Test
  public void testConfNamingNonSupportingClassThrowsClearError() {
    // AnotherFakeCredentialProvider does NOT support "fake" scheme
    Map<String, String> conf = Map.of(
        "spark.security.credentials.provider.fake",
        AnotherFakeCredentialProvider.class.getName());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CredentialProviderLoader.providerFor("fake", conf));
    assertTrue(e.getMessage().contains(AnotherFakeCredentialProvider.class.getName()),
        "Should mention the configured class: " + e.getMessage());
    assertTrue(e.getMessage().contains("fake"),
        "Should mention the scheme: " + e.getMessage());
    assertTrue(e.getMessage().contains(FakeCredentialProvider.class.getName()),
        "Should list available candidate(s): " + e.getMessage());
  }

  @Test
  public void testSingleCandidateWithCorrectExplicitConfSelectsIt() {
    // "fake" is supported only by FakeCredentialProvider; conf names the correct class.
    Map<String, String> conf = Map.of(
        "spark.security.credentials.provider.fake",
        FakeCredentialProvider.class.getName());
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent());
    assertInstanceOf(FakeCredentialProvider.class, result.get());
  }

  @Test
  public void testSingleCandidateWithWrongExplicitConfThrowsClearError() {
    // "fake" is supported only by FakeCredentialProvider but conf names a different class.
    // This validates that explicit conf is enforced even for single-candidate schemes.
    Map<String, String> conf = Map.of(
        "spark.security.credentials.provider.fake",
        "org.apache.spark.security.SomeOtherProvider");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CredentialProviderLoader.providerFor("fake", conf));
    assertTrue(e.getMessage().contains("fake"),
        "Should mention the scheme: " + e.getMessage());
    assertTrue(e.getMessage().contains("org.apache.spark.security.SomeOtherProvider"),
        "Should mention the configured class: " + e.getMessage());
    assertTrue(e.getMessage().contains(FakeCredentialProvider.class.getName()),
        "Should list the available candidate: " + e.getMessage());
  }

  @Test
  public void testUnknownSchemeReturnsEmpty() {
    Map<String, String> conf = Map.of();
    Optional<CredentialProvider> result =
        CredentialProviderLoader.providerFor("nonexistent", conf);
    assertFalse(result.isPresent(), "Unknown scheme should return empty");
  }

  @Test
  public void testInitConfIsInvokedOnSelectedProvider() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.security.credentials.endpoint", "https://sts.example.com");
    conf.put("spark.security.credentials.roleArn", "arn:aws:iam::123456:role/test");

    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent());
    FakeCredentialProvider fake = (FakeCredentialProvider) result.get();
    assertNotNull(fake.getInitConf(), "init() should have been called");
    assertEquals("https://sts.example.com",
        fake.getInitConf().get("spark.security.credentials.endpoint"));
    assertEquals("arn:aws:iam::123456:role/test",
        fake.getInitConf().get("spark.security.credentials.roleArn"));
  }

  @Test
  public void testProviderInitializedExactlyOnce() {
    // Call providerFor twice for the same scheme and assert:
    // (a) the SAME provider instance is returned
    // (b) init was invoked EXACTLY ONCE
    Map<String, String> conf1 = new HashMap<>();
    conf1.put("spark.security.credentials.tag", "first-call");
    Map<String, String> conf2 = new HashMap<>();
    conf2.put("spark.security.credentials.tag", "second-call");

    Optional<CredentialProvider> result1 = CredentialProviderLoader.providerFor("fake", conf1);
    Optional<CredentialProvider> result2 = CredentialProviderLoader.providerFor("fake", conf2);

    assertTrue(result1.isPresent());
    assertTrue(result2.isPresent());
    assertSame(result1.get(), result2.get(),
        "providerFor should return the same cached instance");

    FakeCredentialProvider fake = (FakeCredentialProvider) result1.get();
    assertEquals(1, fake.getInitCount(),
        "init() should be called exactly once (first-conf-wins)");
    assertEquals("first-call", fake.getInitConf().get("spark.security.credentials.tag"),
        "First call's conf should win");
  }

  @Test
  public void testNullSupportedSchemesThrowsClearError() {
    // Inject a provider that returns null from supportedSchemes() to verify the guard.
    CredentialProvider nullSchemesProvider = new CredentialProvider() {
      @Override
      public void init(Map<String, String> conf) {}

      @Override
      public Set<String> supportedSchemes() {
        return null;
      }

      @Override
      public ServiceCredential resolve(UserContext user, URI target) {
        return null;
      }
    };
    CredentialProviderLoader.setProvidersForTesting(
        List.of(nullSchemesProvider));

    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> CredentialProviderLoader.providerFor("anything", Map.of()));
    assertTrue(e.getMessage().contains("returned null from supportedSchemes()"),
        "Should have a clear null-schemes message: " + e.getMessage());
  }

  @Test
  public void testResolveReturnsExpectedServiceCredential() throws Exception {
    Map<String, String> conf = Map.of();
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent());

    UserContext user = new UserContext(
        "testuser", "https://idp.example.com", "token", Instant.now(), null);
    URI target = URI.create("fake://bucket/path");
    ServiceCredential cred = result.get().resolve(user, target);

    assertNotNull(cred);
    assertEquals("fake", cred.getProperties().get("provider"));
    assertNotNull(cred.getExpiresAt(), "expiresAt should be set");
  }

  @Test
  public void testResolveSentinelThrowsCredentialResolutionException() {
    Map<String, String> conf = Map.of();
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent());

    UserContext user = new UserContext(
        "testuser", "https://idp.example.com", "token", Instant.now(), null);
    URI errorTarget = URI.create("fake://error.example.com/path");

    CredentialResolutionException e = assertThrows(CredentialResolutionException.class,
        () -> result.get().resolve(user, errorTarget));
    assertTrue(e.getMessage().contains("error.example.com"),
        "Exception should reference the target: " + e.getMessage());
  }

  @Test
  public void testSchemeNormalizationIsCaseInsensitive() {
    // "FAKE" should resolve the same as "fake"
    Map<String, String> conf = Map.of();
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("FAKE", conf);
    assertTrue(result.isPresent());
    assertInstanceOf(FakeCredentialProvider.class, result.get());
  }

  @Test
  public void testExplicitSelectionWithUppercaseSchemeNormalizesConfKey() {
    // The conf key uses normalized (lowercase) scheme
    Map<String, String> conf = Map.of(
        "spark.security.credentials.provider.shared",
        FakeCredentialProvider.class.getName());
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("SHARED", conf);
    assertTrue(result.isPresent());
    assertInstanceOf(FakeCredentialProvider.class, result.get());
  }

  @Test
  public void testNullSchemeThrowsNPE() {
    NullPointerException e = assertThrows(NullPointerException.class,
        () -> CredentialProviderLoader.providerFor(null, Map.of()));
    assertTrue(e.getMessage().contains("scheme must not be null"),
        "Should have a clear message: " + e.getMessage());
  }

  @Test
  public void testNullConfThrowsNPE() {
    NullPointerException e = assertThrows(NullPointerException.class,
        () -> CredentialProviderLoader.providerFor("fake", null));
    assertTrue(e.getMessage().contains("conf must not be null"),
        "Should have a clear message: " + e.getMessage());
  }

  @Test
  public void testSuggestedTtlDefaultValue() {
    Map<String, String> conf = Map.of();
    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent());
    assertEquals(Duration.ofMinutes(15), result.get().suggestedTtl());
  }

  @Test
  public void testInitConfScopedToCredentialsKeysOnly() {
    // Verify that init() receives only spark.security.credentials.* keys,
    // and foreign secrets from other subsystems are NOT leaked to providers.
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.security.credentials.provider.fake",
        FakeCredentialProvider.class.getName());
    conf.put("spark.security.credentials.endpoint", "https://sts.example.com");
    conf.put("spark.authenticate.secret", "TOPSECRET");
    conf.put("spark.ssl.keyPassword", "keypass");

    Optional<CredentialProvider> result = CredentialProviderLoader.providerFor("fake", conf);
    assertTrue(result.isPresent());
    FakeCredentialProvider fake = (FakeCredentialProvider) result.get();
    Map<String, String> initConf = fake.getInitConf();
    assertNotNull(initConf, "init() should have been called");

    // Credentials keys should be present
    assertEquals("https://sts.example.com",
        initConf.get("spark.security.credentials.endpoint"));
    assertTrue(initConf.containsKey("spark.security.credentials.provider.fake"),
        "Provider selection key should be included (it starts with the prefix)");

    // Foreign secrets must NOT be present
    assertFalse(initConf.containsKey("spark.authenticate.secret"),
        "Foreign secret should not leak to provider init()");
    assertFalse(initConf.containsKey("spark.ssl.keyPassword"),
        "Foreign secret should not leak to provider init()");
    assertFalse(initConf.values().contains("TOPSECRET"),
        "Foreign secret value should not leak to provider init()");
  }

  @Test
  public void testEmptySchemeThrowsIllegalArgument() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CredentialProviderLoader.providerFor("", Map.of()));
    assertEquals("scheme must not be empty", e.getMessage());
  }

  @Test
  public void testInitRetryAfterFailure() {
    // A provider whose init() throws on the first call then succeeds on the second.
    // First providerFor should propagate the failure; second providerFor should retry
    // init() and succeed.
    CredentialProvider failOnceThenSucceed = new CredentialProvider() {
      private int initAttempts = 0;
      private boolean initialized = false;

      @Override
      public void init(Map<String, String> conf) {
        initAttempts++;
        if (initAttempts == 1) {
          throw new RuntimeException("Simulated transient init failure");
        }
        initialized = true;
      }

      @Override
      public Set<String> supportedSchemes() {
        return Set.of("retryscheme");
      }

      @Override
      public ServiceCredential resolve(UserContext user, URI target) {
        return new ServiceCredential(Map.of("ok", "true"), Instant.now().plusSeconds(60));
      }

      @Override
      public String toString() {
        return "initAttempts=" + initAttempts + ",initialized=" + initialized;
      }
    };

    CredentialProviderLoader.setProvidersForTesting(List.of(failOnceThenSucceed));

    // First call: init() throws, providerFor should propagate
    Map<String, String> conf = Map.of();
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> CredentialProviderLoader.providerFor("retryscheme", conf));
    assertTrue(e.getMessage().contains("Simulated transient init failure"));

    // Second call: init() should be retried and succeed
    Optional<CredentialProvider> result =
        CredentialProviderLoader.providerFor("retryscheme", conf);
    assertTrue(result.isPresent(), "Second providerFor should succeed after init retry");

    // Verify init was called exactly twice (proving the retry)
    String state = result.get().toString();
    assertTrue(state.contains("initAttempts=2"),
        "init() should have been invoked twice: " + state);
    assertTrue(state.contains("initialized=true"),
        "Provider should be initialized after retry: " + state);
  }
}
