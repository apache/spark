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

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.StsException;

import org.apache.spark.security.CredentialProvider;
import org.apache.spark.security.CredentialResolutionException;
import org.apache.spark.security.ServiceCredential;
import org.apache.spark.security.UserContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AwsStsCredentialProvider}.
 */
public class AwsStsCredentialProviderSuite {

  private static String previousAwsRegion;

  /**
   * Set the aws.region system property so that the AWS SDK's
   * DefaultAwsRegionProviderChain can resolve a region on any environment
   * (including CI runners with no AWS configuration). This does NOT affect
   * AwsStsCredentialProvider.resolveRegion() which reads only from the conf Map.
   */
  @BeforeAll
  static void setUpClass() {
    previousAwsRegion = System.getProperty("aws.region");
    System.setProperty("aws.region", "us-east-1");
  }

  @AfterAll
  static void tearDownClass() {
    if (previousAwsRegion == null) {
      System.clearProperty("aws.region");
    } else {
      System.setProperty("aws.region", previousAwsRegion);
    }
  }

  /** Suite-level field closed by tearDown to avoid resource leaks from real StsClients. */
  private AwsStsCredentialProvider provider;

  @AfterEach
  void tearDown() {
    if (provider != null) {
      provider.close();
      provider = null;
    }
  }

  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String TEST_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test-payload";
  private static final String TEST_PRINCIPAL = "user@example.com";
  private static final String TEST_ISSUER = "https://idp.example.com";
  private static final URI TEST_TARGET = URI.create("s3a://my-bucket/data/file.parquet");

  // ========== ServiceLoader Discovery ==========

  @Test
  public void testServiceLoaderDiscovery() {
    ServiceLoader<CredentialProvider> loader = ServiceLoader.load(CredentialProvider.class);
    boolean found = false;
    for (CredentialProvider provider : loader) {
      if (provider instanceof AwsStsCredentialProvider) {
        found = true;
        break;
      }
    }
    assertTrue(found, "AwsStsCredentialProvider should be discoverable via ServiceLoader");
  }

  // ========== init() ==========

  @Test
  public void testMissingRoleArnThrowsIllegalArgumentException() {
    Map<String, String> conf = new HashMap<>();

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> provider.init(conf));
    assertTrue(ex.getMessage().contains(AwsStsCredentialProvider.CONF_ROLE_ARN));
  }

  @Test
  public void testBlankRoleArnThrowsIllegalArgumentException() {
    Map<String, String> conf = new HashMap<>();
    conf.put(AwsStsCredentialProvider.CONF_ROLE_ARN, "   ");

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> provider.init(conf));
    assertTrue(ex.getMessage().contains(AwsStsCredentialProvider.CONF_ROLE_ARN));
  }

  @Test
  public void testInitWithInvalidDurationSeconds() {
    assertInitThrowsForConfig(AwsStsCredentialProvider.CONF_DURATION_SECONDS, "not-a-number");
  }

  @Test
  public void testInitWithZeroDurationSecondsThrowsIllegalArgumentException() {
    IllegalArgumentException ex = assertInitThrowsForConfig(
        AwsStsCredentialProvider.CONF_DURATION_SECONDS, "0");
    assertTrue(ex.getMessage().contains("900"));
    assertTrue(ex.getMessage().contains("43200"));
  }

  @Test
  public void testInitWithNegativeDurationSecondsThrowsIllegalArgumentException() {
    IllegalArgumentException ex = assertInitThrowsForConfig(
        AwsStsCredentialProvider.CONF_DURATION_SECONDS, "-100");
    assertTrue(ex.getMessage().contains("900"));
  }

  @Test
  public void testInitWithDurationBelowMinimumThrowsIllegalArgumentException() {
    IllegalArgumentException ex = assertInitThrowsForConfig(
        AwsStsCredentialProvider.CONF_DURATION_SECONDS, "899");
    assertTrue(ex.getMessage().contains("900"));
    assertTrue(ex.getMessage().contains("43200"));
  }

  @Test
  public void testInitWithDurationAboveMaximumThrowsIllegalArgumentException() {
    IllegalArgumentException ex = assertInitThrowsForConfig(
        AwsStsCredentialProvider.CONF_DURATION_SECONDS, "43201");
    assertTrue(ex.getMessage().contains("900"));
    assertTrue(ex.getMessage().contains("43200"));
  }

  @Test
  public void testInitWithMinimumValidDurationSucceeds() {
    Map<String, String> conf = confWithRoleArn();
    conf.put(AwsStsCredentialProvider.CONF_DURATION_SECONDS, "900");

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    assertNotNull(provider.resolvedConfig());
    assertEquals(900, provider.resolvedConfig().durationSeconds);
  }

  @Test
  public void testInitWithMaximumValidDurationSucceeds() {
    Map<String, String> conf = confWithRoleArn();
    conf.put(AwsStsCredentialProvider.CONF_DURATION_SECONDS, "43200");

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    assertNotNull(provider.resolvedConfig());
    assertEquals(43200, provider.resolvedConfig().durationSeconds);
  }

  @Test
  public void testReInitializationThrowsIllegalStateException() {
    Map<String, String> conf = confWithRoleArn();

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> provider.init(conf));
    assertTrue(ex.getMessage().contains("already initialized"));
  }

  @Test
  public void testInitWithEndpointAndRegionResolvesCorrectly() {
    Map<String, String> conf = confWithRoleArn();
    conf.put(AwsStsCredentialProvider.CONF_STS_ENDPOINT, "http://localhost:9000");
    conf.put(AwsStsCredentialProvider.CONF_REGION, "us-west-2");

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    AwsStsCredentialProvider.ResolvedConfig cfg = provider.resolvedConfig();
    assertNotNull(cfg);
    assertEquals(Region.of("us-west-2"), cfg.resolvedRegion);
    assertEquals(URI.create("http://localhost:9000"), cfg.endpointOverride);
    assertNotNull(cfg.stsClient);
  }

  @Test
  public void testInitWithEndpointNoRegionUsesDefault() {
    Map<String, String> conf = confWithRoleArn();
    conf.put(AwsStsCredentialProvider.CONF_STS_ENDPOINT, "http://localhost:9000");

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    AwsStsCredentialProvider.ResolvedConfig cfg = provider.resolvedConfig();
    assertNotNull(cfg);
    assertEquals(Region.of("us-east-1"), cfg.resolvedRegion);
    assertEquals(URI.create("http://localhost:9000"), cfg.endpointOverride);
  }

  @Test
  public void testInitWithNeitherEndpointNorRegion() {
    Map<String, String> conf = confWithRoleArn();

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    AwsStsCredentialProvider.ResolvedConfig cfg = provider.resolvedConfig();
    assertNotNull(cfg);
    assertNull(cfg.resolvedRegion);
    assertNull(cfg.endpointOverride);
  }

  @Test
  public void testInitWithMalformedEndpointThrowsIllegalArgumentException() {
    Map<String, String> conf = confWithRoleArn();
    conf.put(AwsStsCredentialProvider.CONF_STS_ENDPOINT, "not a valid uri^[]");

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> provider.init(conf));
    assertTrue(ex.getMessage().contains(AwsStsCredentialProvider.CONF_STS_ENDPOINT),
        "Error should mention the config key");
    assertTrue(ex.getMessage().contains("not a valid uri^[]"),
        "Error should mention the bad value");
    assertNotNull(ex.getCause(), "Original IllegalArgumentException should be preserved");
  }

  @Test
  public void testInitTrimsConfigValues() {
    Map<String, String> conf = new HashMap<>();
    conf.put(AwsStsCredentialProvider.CONF_ROLE_ARN, "  " + TEST_ROLE_ARN + "  ");
    conf.put(AwsStsCredentialProvider.CONF_STS_ENDPOINT, "  http://localhost:9000  ");
    conf.put(AwsStsCredentialProvider.CONF_REGION, "  us-west-2  ");

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    AwsStsCredentialProvider.ResolvedConfig cfg = provider.resolvedConfig();
    assertNotNull(cfg);
    assertEquals(TEST_ROLE_ARN, cfg.roleArn);
    assertEquals(Region.of("us-west-2"), cfg.resolvedRegion);
    assertEquals(URI.create("http://localhost:9000"), cfg.endpointOverride);
  }

  @Test
  public void testInitTrimsRoleSessionName() throws CredentialResolutionException {
    Instant expiration = Instant.now().plusSeconds(3600);
    StsClient mockSts = createMockStsClient("AK", "SK", "ST", expiration);

    // Use init() with a padded sessionName to exercise the trim path
    Map<String, String> conf = new HashMap<>();
    conf.put(AwsStsCredentialProvider.CONF_ROLE_ARN, TEST_ROLE_ARN);
    conf.put(AwsStsCredentialProvider.CONF_SESSION_NAME, "  my-session  ");
    conf.put(AwsStsCredentialProvider.CONF_STS_ENDPOINT, "http://localhost:9000");

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    // Verify the stored config has the trimmed value
    assertEquals("my-session", provider.resolvedConfig().roleSessionName);

    // Now resolve() with a separate provider that uses the test constructor
    // so we can capture the STS request via mock
    String sessionName = provider.resolvedConfig().roleSessionName;
    provider.close();
    provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, sessionName, null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));
    provider.resolve(user, TEST_TARGET);

    ArgumentCaptor<AssumeRoleWithWebIdentityRequest> captor =
        ArgumentCaptor.forClass(AssumeRoleWithWebIdentityRequest.class);
    verify(mockSts).assumeRoleWithWebIdentity(captor.capture());

    // The session name in the request must be trimmed, not "  my-session  "
    assertEquals("my-session", captor.getValue().roleSessionName());
  }

  // ========== resolve() ==========

  @Test
  public void testSuccessfulResolve() throws CredentialResolutionException {
    Instant expiration = Instant.now().plusSeconds(3600);
    StsClient mockSts = createMockStsClient("AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "FwoGZXIvY...token", expiration);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", 3600);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    ServiceCredential credential = provider.resolve(user, TEST_TARGET);

    assertNotNull(credential);
    assertEquals("AKIAIOSFODNN7EXAMPLE", credential.getProperties().get("fs.s3a.access.key"));
    assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        credential.getProperties().get("fs.s3a.secret.key"));
    assertEquals("FwoGZXIvY...token", credential.getProperties().get("fs.s3a.session.token"));
    assertEquals(expiration, credential.getExpiresAt());
    assertEquals(3, credential.getProperties().size());

    // Verify the STS request was built correctly
    ArgumentCaptor<AssumeRoleWithWebIdentityRequest> captor =
        ArgumentCaptor.forClass(AssumeRoleWithWebIdentityRequest.class);
    verify(mockSts).assumeRoleWithWebIdentity(captor.capture());

    AssumeRoleWithWebIdentityRequest request = captor.getValue();
    assertEquals(TEST_ROLE_ARN, request.roleArn());
    assertEquals(TEST_TOKEN, request.webIdentityToken());
    assertEquals("test-session", request.roleSessionName());
    assertEquals(3600, request.durationSeconds());
  }

  @Test
  public void testStsFailureWrappedInCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    StsException stsException = (StsException) StsException.builder()
        .message("Access denied for role")
        .build();
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenThrow(stsException);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, TEST_TARGET));

    // Verify the exception wraps the STS exception
    assertEquals(stsException, ex.getCause());
    assertTrue(ex.getMessage().contains(TEST_ROLE_ARN));
    assertTrue(ex.getMessage().contains("AssumeRoleWithWebIdentity"));

    // SECURITY: Verify that the raw token is NOT in any exception message
    assertFalse(ex.getMessage().contains(TEST_TOKEN),
        "Raw token must never appear in exception messages");
    assertFalse(ex.getCause().getMessage() != null
        && ex.getCause().getMessage().contains(TEST_TOKEN),
        "Raw token must never appear in cause exception messages");
  }

  @Test
  public void testTokenRedactedFromStsErrorMessage() {
    StsClient mockSts = mock(StsClient.class);
    // Simulate STS echoing the token back in an error message
    StsException stsException = (StsException) StsException.builder()
        .message("Invalid identity token: " + TEST_TOKEN)
        .build();
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenThrow(stsException);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, TEST_TARGET));

    // SECURITY: The wrapped message must NOT contain the raw token
    assertFalse(ex.getMessage().contains(TEST_TOKEN),
        "Raw token must be redacted from exception messages even if STS echoed it");
    assertTrue(ex.getMessage().contains("[REDACTED]"),
        "Token should be replaced with [REDACTED]");

    // SECURITY: The cause exception message must also NOT contain the raw token
    assertNotNull(ex.getCause(), "Cause should be a sanitized wrapper exception");
    assertFalse(ex.getCause().getMessage().contains(TEST_TOKEN),
        "Raw token must be redacted from cause exception message");
  }

  @Test
  public void testCustomSessionNameAndDurationPropagateToRequest()
      throws CredentialResolutionException {
    Instant expiration = Instant.now().plusSeconds(900);
    StsClient mockSts = createMockStsClient("AK", "SK", "ST", expiration);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "custom-session-name", 900);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    provider.resolve(user, TEST_TARGET);

    ArgumentCaptor<AssumeRoleWithWebIdentityRequest> captor =
        ArgumentCaptor.forClass(AssumeRoleWithWebIdentityRequest.class);
    verify(mockSts).assumeRoleWithWebIdentity(captor.capture());

    AssumeRoleWithWebIdentityRequest request = captor.getValue();
    assertEquals("custom-session-name", request.roleSessionName());
    assertEquals(900, request.durationSeconds());
  }

  @Test
  public void testNoDurationSecondsInRequestWhenNotConfigured()
      throws CredentialResolutionException {
    Instant expiration = Instant.now().plusSeconds(3600);
    StsClient mockSts = createMockStsClient("AK", "SK", "ST", expiration);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    provider.resolve(user, TEST_TARGET);

    ArgumentCaptor<AssumeRoleWithWebIdentityRequest> captor =
        ArgumentCaptor.forClass(AssumeRoleWithWebIdentityRequest.class);
    verify(mockSts).assumeRoleWithWebIdentity(captor.capture());

    // durationSeconds should be null when not configured
    assertNull(captor.getValue().durationSeconds());
  }

  @Test
  public void testNullUserContextThrowsCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(null, TEST_TARGET));
  }

  @Test
  public void testNullTargetThrowsCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, null));
    assertTrue(ex.getMessage().contains("Target URI must not be null"));
  }

  @Test
  public void testNullRawTokenThrowsCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    UserContext user = mock(UserContext.class);
    when(user.getRawToken()).thenReturn(null);

    assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, TEST_TARGET));
  }

  @Test
  public void testBlankRawTokenThrowsCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, "   ",
        Instant.now(), Instant.now().plusSeconds(300));

    assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, TEST_TARGET));
  }

  @Test
  public void testResolveBeforeInitThrowsCredentialResolutionException() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, TEST_TARGET));

    assertTrue(ex.getMessage().contains("resolve() called before init()"));
  }

  @Test
  public void testNullCredentialsResponseThrowsCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    AssumeRoleWithWebIdentityResponse response = AssumeRoleWithWebIdentityResponse.builder()
        .credentials((Credentials) null)
        .build();
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenReturn(response);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, TEST_TARGET));

    assertTrue(ex.getMessage().contains("incomplete credentials"));
    assertTrue(ex.getMessage().contains(TEST_ROLE_ARN));
    assertFalse(ex.getMessage().contains(TEST_TOKEN),
        "Raw token must never appear in exception messages");
  }

  @Test
  public void testMissingSessionTokenThrowsCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    Credentials creds = Credentials.builder()
        .accessKeyId("AKID")
        .secretAccessKey("SECRET")
        .sessionToken(null)
        .expiration(Instant.now().plusSeconds(3600))
        .build();
    AssumeRoleWithWebIdentityResponse response = AssumeRoleWithWebIdentityResponse.builder()
        .credentials(creds)
        .build();
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenReturn(response);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> provider.resolve(user, TEST_TARGET));

    assertTrue(ex.getMessage().contains("incomplete credentials"));
    assertTrue(ex.getMessage().contains(TEST_ROLE_ARN));
    assertFalse(ex.getMessage().contains(TEST_TOKEN),
        "Raw token must never appear in exception messages");
  }

  // ========== supportedSchemes() ==========

  @Test
  public void testSupportedSchemesContainsS3a() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    Set<String> schemes = provider.supportedSchemes();
    assertTrue(schemes.contains("s3a"));
    assertEquals(1, schemes.size());
  }

  // ========== suggestedTtl() ==========

  @Test
  public void testSuggestedTtlWithDurationSeconds() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", 1800);

    assertEquals(Duration.ofSeconds(1800), provider.suggestedTtl());
  }

  @Test
  public void testSuggestedTtlDefaultsTo15Minutes() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    assertEquals(Duration.ofMinutes(15), provider.suggestedTtl());
  }

  // ========== Session name derivation ==========

  @Test
  public void testDefaultSessionNameDerivedFromPrincipal()
      throws CredentialResolutionException {
    Instant expiration = Instant.now().plusSeconds(3600);
    StsClient mockSts = createMockStsClient("AK", "SK", "ST", expiration);

    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, null, null);

    UserContext user = new UserContext("alice@corp.example.com", TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    provider.resolve(user, TEST_TARGET);

    ArgumentCaptor<AssumeRoleWithWebIdentityRequest> captor =
        ArgumentCaptor.forClass(AssumeRoleWithWebIdentityRequest.class);
    verify(mockSts).assumeRoleWithWebIdentity(captor.capture());

    String sessionName = captor.getValue().roleSessionName();
    assertNotNull(sessionName);
    assertFalse(sessionName.isBlank());
    assertTrue(sessionName.contains("alice"));
    // @ is valid in STS session names and should be preserved
    assertTrue(sessionName.contains("@"));
  }

  @Test
  public void testSanitizeSessionName() {
    // {input, expected}
    String[][] cases = {
        // truncates to 64
        {"a".repeat(70), "a".repeat(64)},
        // replaces invalid chars
        {"user name!#$%", "user-name----"},
        // preserves valid chars
        {"user_+=,.@-test", "user_+=,.@-test"},
        // falls back for short result
        {"x", "spark-oidc"},
        // falls back when all invalid
        {"!", "spark-oidc"},
        // replaces backslash
        {"DOMAIN\\user", "DOMAIN-user"},
    };

    for (String[] c : cases) {
      assertEquals(c[1], AwsStsCredentialProvider.sanitizeSessionName(c[0]),
          "input: " + c[0]);
    }
  }

  // ========== close() ==========

  @Test
  public void testCloseShutsStsClient() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "session", null);

    provider.close();

    verify(mockSts).close();
  }

  @Test
  public void testCloseBeforeInitDoesNotThrow() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    // Should not throw even when config is null
    provider.close();
  }

  // ========== Deep cause-chain token redaction (Item 1) ==========

  @Test
  public void testTokenInCauseChainIsRedacted() {
    StsClient mockSts = mock(StsClient.class);
    // Build a cause chain where the INNER cause contains the raw token,
    // but the top-level message does NOT.
    RuntimeException innerCause = new RuntimeException(
        "Token validation failed: " + TEST_TOKEN);
    StsException stsException = (StsException) StsException.builder()
        .message("Access denied for role")
        .cause(innerCause)
        .build();
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenThrow(stsException);

    AwsStsCredentialProvider p = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> p.resolve(user, TEST_TARGET));

    // Top-level message must not contain the raw token
    assertFalse(ex.getMessage().contains(TEST_TOKEN),
        "Raw token must not appear in top-level exception message");

    // Walk the entire cause chain and assert the token is absent everywhere
    Throwable current = ex.getCause();
    while (current != null) {
      if (current.getMessage() != null) {
        assertFalse(current.getMessage().contains(TEST_TOKEN),
            "Raw token must not appear in any cause message, but found in: "
                + current.getClass().getSimpleName());
      }
      current = current.getCause();
    }
  }

  // ========== Session name validation (Item 3) ==========

  @Test
  public void testInitWithValidCustomSessionName() {
    Map<String, String> conf = confWithRoleArn();
    conf.put(AwsStsCredentialProvider.CONF_SESSION_NAME, "valid_session+=,.@-name");

    provider = new AwsStsCredentialProvider();
    provider.init(conf);

    assertEquals("valid_session+=,.@-name", provider.resolvedConfig().roleSessionName);
  }

  @Test
  public void testInitWithInvalidSessionNameContainingSpace() {
    IllegalArgumentException ex = assertInitThrowsForConfig(
        AwsStsCredentialProvider.CONF_SESSION_NAME, "bad session");
    assertTrue(ex.getMessage().contains("bad session"),
        "Error must echo the bad value");
  }

  @Test
  public void testInitWithInvalidSessionNameContainingQuestionMark() {
    assertInitThrowsForConfig(AwsStsCredentialProvider.CONF_SESSION_NAME, "bad?name");
  }

  @Test
  public void testInitWithSessionNameTooShort() {
    IllegalArgumentException ex = assertInitThrowsForConfig(
        AwsStsCredentialProvider.CONF_SESSION_NAME, "x");
    assertTrue(ex.getMessage().contains("x"));
  }

  @Test
  public void testInitWithSessionNameTooLong() {
    assertInitThrowsForConfig(AwsStsCredentialProvider.CONF_SESSION_NAME, "a".repeat(65));
  }

  // ========== Session name: non-ASCII rejection (regression) ==========

  @Test
  public void testInitRejectsSessionNameWithAccentedChar() {
    // U+00E9 (accented e) is valid under \w but NOT valid in STS session names.
    // This test would PASS (incorrectly) under the buggy \w pattern and must
    // FAIL (correctly) under the explicit ASCII pattern.
    assertInitThrowsForConfig(AwsStsCredentialProvider.CONF_SESSION_NAME, "café");
  }

  @Test
  public void testInitRejectsSessionNameWithCjkChar() {
    // U+4E16 (CJK ideograph) is valid under \w but NOT valid in STS session names.
    // Regression test for the ASCII-only fix.
    assertInitThrowsForConfig(AwsStsCredentialProvider.CONF_SESSION_NAME, "session世");
  }

  // ========== close() idempotency ==========

  @Test
  public void testDoubleCloseIsIdempotent() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider p = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", null);

    // First close should succeed normally
    p.close();
    // Second close must be a no-op (no exception, no double-close on the client)
    p.close();

    // Verify the underlying client was closed exactly once
    verify(mockSts).close();
  }

  @Test
  public void testResolveAfterCloseThrowsWithClosedMessage() {
    StsClient mockSts = mock(StsClient.class);
    AwsStsCredentialProvider p = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", null);
    p.close();

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> p.resolve(user, TEST_TARGET));
    assertTrue(ex.getMessage().contains("closed"),
        "Message should indicate the provider is closed");
  }

  // ========== causeChainStream cycle protection ==========

  @Test
  public void testCauseChainCycleDoesNotLoop() throws Exception {
    // Construct a circular cause chain via reflection: nodeA -> nodeB -> nodeA.
    // Java's Throwable.initCause() prevents self-causation and re-initialization,
    // so we use Field.set to create the cycle.
    RuntimeException nodeA = new RuntimeException("nodeA");
    RuntimeException nodeB = new RuntimeException("nodeB", nodeA);
    // nodeA.cause is currently the sentinel (this), which means "not yet set" in
    // OpenJDK's implementation. Force it to nodeB to create the cycle.
    java.lang.reflect.Field causeField = Throwable.class.getDeclaredField("cause");
    causeField.setAccessible(true);
    causeField.set(nodeA, nodeB);

    // Now we have nodeA -> nodeB -> nodeA (a cycle).
    // Simulate what resolve() does: call causeChainContainsToken and verify termination.
    StsClient mockSts = mock(StsClient.class);
    StsException stsException = (StsException) StsException.builder()
        .message("some error")
        .cause(nodeA)
        .build();
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenThrow(stsException);

    AwsStsCredentialProvider p = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", null);

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    // This must terminate (no infinite loop) and throw CredentialResolutionException
    assertThrows(CredentialResolutionException.class,
        () -> p.resolve(user, TEST_TARGET));
  }

  // ========== resolve() wraps SDK IllegalStateException (Item 5) ==========

  @Test
  public void testResolveWrapsSdkIllegalStateExceptionAsCredentialResolutionException() {
    StsClient mockSts = mock(StsClient.class);
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenThrow(new IllegalStateException("client has been closed"));

    AwsStsCredentialProvider p = new AwsStsCredentialProvider(
        mockSts, TEST_ROLE_ARN, "test-session", null);
    // Do NOT call p.close() -- keep the provider's closed flag false so that
    // resolve() reaches the STS call, where the mock throws IllegalStateException.
    // This exercises the catch (IllegalStateException e) branch in resolve().

    UserContext user = new UserContext(TEST_PRINCIPAL, TEST_ISSUER, TEST_TOKEN,
        Instant.now(), Instant.now().plusSeconds(300));

    CredentialResolutionException ex = assertThrows(CredentialResolutionException.class,
        () -> p.resolve(user, TEST_TARGET));

    assertTrue(ex.getMessage().contains("closed"),
        "Message should indicate the STS client has been closed");
    assertFalse(ex.getMessage().contains(TEST_TOKEN),
        "Raw token must never appear in exception messages");

    // Verify the cause is the original IllegalStateException from the SDK client
    assertTrue(ex.getCause() instanceof IllegalStateException,
        "Cause should be the IllegalStateException thrown by the STS client");
    assertEquals("client has been closed", ex.getCause().getMessage());

    // Verify the mock was actually invoked (proving the closed-flag short-circuit
    // was NOT hit and the test truly exercises the ISE catch branch)
    verify(mockSts).assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class));
  }

  // ========== Helpers ==========

  /** Creates a config map with {@link #TEST_ROLE_ARN} pre-set. */
  private Map<String, String> confWithRoleArn() {
    Map<String, String> conf = new HashMap<>();
    conf.put(AwsStsCredentialProvider.CONF_ROLE_ARN, TEST_ROLE_ARN);
    return conf;
  }

  /**
   * Asserts that {@code init()} throws {@link IllegalArgumentException} whose message
   * contains the given config key. Returns the exception for additional assertions.
   */
  private IllegalArgumentException assertInitThrowsForConfig(String key, String value) {
    Map<String, String> conf = confWithRoleArn();
    conf.put(key, value);
    AwsStsCredentialProvider p = new AwsStsCredentialProvider();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> p.init(conf));
    assertTrue(ex.getMessage().contains(key),
        "Error message must contain the config key '" + key + "' but was: " + ex.getMessage());
    return ex;
  }

  private StsClient createMockStsClient(String accessKeyId, String secretAccessKey,
      String sessionToken, Instant expiration) {
    StsClient mockSts = mock(StsClient.class);
    Credentials creds = Credentials.builder()
        .accessKeyId(accessKeyId)
        .secretAccessKey(secretAccessKey)
        .sessionToken(sessionToken)
        .expiration(expiration)
        .build();
    AssumeRoleWithWebIdentityResponse response = AssumeRoleWithWebIdentityResponse.builder()
        .credentials(creds)
        .build();
    when(mockSts.assumeRoleWithWebIdentity(any(AssumeRoleWithWebIdentityRequest.class)))
        .thenReturn(response);
    return mockSts;
  }

  // ========== additionalSparkProperties() ==========

  @Test
  public void testAdditionalSparkPropertiesReturnsS3aProviderMapping() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    Map<String, String> props = provider.additionalSparkProperties();
    assertEquals(1, props.size());
    assertEquals(
        "org.apache.spark.security.aws.SparkOidcAwsCredentialsProvider",
        props.get("spark.hadoop.fs.s3a.aws.credentials.provider"));
  }

  @Test
  public void testAdditionalSparkPropertiesKeyIncludesSparkHadoopPrefix() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    Map<String, String> props = provider.additionalSparkProperties();
    props.keySet().forEach(key ->
        assertTrue(key.startsWith("spark.hadoop."),
            "Key must include spark.hadoop. prefix: " + key));
  }

  @Test
  public void testAdditionalSparkPropertiesIsUnmodifiable() {
    AwsStsCredentialProvider provider = new AwsStsCredentialProvider();
    Map<String, String> props = provider.additionalSparkProperties();
    assertThrows(UnsupportedOperationException.class,
        () -> props.put("foo", "bar"));
  }
}
