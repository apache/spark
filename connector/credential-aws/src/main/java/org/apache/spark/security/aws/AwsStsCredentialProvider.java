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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import org.apache.spark.security.CredentialProvider;
import org.apache.spark.security.CredentialResolutionException;
import org.apache.spark.security.ServiceCredential;
import org.apache.spark.security.UserContext;

/**
 * A {@link CredentialProvider} that exchanges an OIDC identity token for temporary
 * AWS credentials via the STS {@code AssumeRoleWithWebIdentity} API.
 * <p>
 * The returned {@link ServiceCredential} contains S3A-compatible Hadoop configuration
 * properties ({@code fs.s3a.access.key}, {@code fs.s3a.secret.key},
 * {@code fs.s3a.session.token}) that can be propagated to executors for accessing
 * S3-compatible storage.
 * <p>
 * <b>Configuration keys</b> (passed via {@code spark.security.oidc.*}):
 * <ul>
 *   <li>{@code spark.security.oidc.aws.roleArn} (required) -- the ARN of the IAM role
 *       to assume</li>
 *   <li>{@code spark.security.oidc.aws.sessionName} (optional) -- the role session name;
 *       defaults to a value derived from the user's principal or "spark-oidc"</li>
 *   <li>{@code spark.security.oidc.aws.durationSeconds} (optional) -- credential duration
 *       in seconds (900-43200); if unset, STS uses the role's default maximum</li>
 *   <li>{@code spark.security.oidc.aws.region} (optional) -- the AWS region for the STS
 *       endpoint; defaults to us-east-1 when only {@code stsEndpoint} is set. When neither
 *       {@code region} nor {@code stsEndpoint} is configured, the STS client falls back to
 *       the AWS SDK default region resolution (AWS_REGION / AWS_DEFAULT_REGION environment
 *       variables, then the ~/.aws/config profile).</li>
 *   <li>{@code spark.security.oidc.aws.stsEndpoint} (optional) -- a custom STS endpoint URL
 *       for non-AWS environments (MinIO, Ceph, LocalStack, etc.)</li>
 * </ul>
 * <p>
 * <b>Security note:</b> The OIDC raw token is never included in log messages or
 * exception messages. It is passed directly to the STS API call and discarded.
 *
 * @since 4.4.0
 */
public class AwsStsCredentialProvider implements CredentialProvider {

  // Configuration key constants
  static final String CONF_ROLE_ARN = "spark.security.oidc.aws.roleArn";
  static final String CONF_SESSION_NAME = "spark.security.oidc.aws.sessionName";
  static final String CONF_DURATION_SECONDS = "spark.security.oidc.aws.durationSeconds";
  static final String CONF_REGION = "spark.security.oidc.aws.region";
  static final String CONF_STS_ENDPOINT = "spark.security.oidc.aws.stsEndpoint";

  /** Minimum duration allowed by STS AssumeRoleWithWebIdentity (15 minutes). */
  static final int MIN_DURATION_SECONDS = 900;
  /** Maximum duration allowed by STS AssumeRoleWithWebIdentity (12 hours). */
  static final int MAX_DURATION_SECONDS = 43200;

  private static final String DEFAULT_REGION = "us-east-1";
  private static final String DEFAULT_SESSION_NAME = "spark-oidc";

  /**
   * Precompiled pattern matching characters that are NOT valid in STS session names.
   * Valid characters are: alphanumeric, underscore, plus, equals, comma, period, at, hyphen.
   */
  private static final Pattern SESSION_NAME_INVALID_CHARS =
      Pattern.compile("[^a-zA-Z0-9_+=,.@\\-]");

  /**
   * Precompiled pattern for validating a configured STS session name.
   * Must match {@code [a-zA-Z0-9_+=,.@-]{2,64}} per AWS STS documentation.
   * Uses an explicit ASCII character class rather than {@code \w} to avoid
   * accepting non-ASCII characters (e.g. accented letters, CJK) that AWS STS
   * would reject.
   */
  private static final Pattern SESSION_NAME_VALID_PATTERN =
      Pattern.compile("[a-zA-Z0-9_+=,.@\\-]{2,64}");

  /**
   * Immutable configuration holder that is safely published via the volatile
   * {@link #config} field. All fields are set once during construction and are
   * final, ensuring correct visibility across threads after init() completes.
   */
  static final class ResolvedConfig {
    final String roleArn;
    final String roleSessionName;
    final Integer durationSeconds;
    final Region resolvedRegion;
    final URI endpointOverride;
    final StsClient stsClient;

    ResolvedConfig(String roleArn, String roleSessionName, Integer durationSeconds,
        Region resolvedRegion, URI endpointOverride, StsClient stsClient) {
      this.roleArn = roleArn;
      this.roleSessionName = roleSessionName;
      this.durationSeconds = durationSeconds;
      this.resolvedRegion = resolvedRegion;
      this.endpointOverride = endpointOverride;
      this.stsClient = stsClient;
    }
  }

  /** Safely published via volatile write in init(); read in resolve()/suggestedTtl(). */
  private volatile ResolvedConfig config;

  /** Guards against double-close and allows resolve() to fail fast after close(). */
  private volatile boolean closed = false;

  /**
   * Default no-arg constructor used by {@link java.util.ServiceLoader}.
   */
  public AwsStsCredentialProvider() {
    // ServiceLoader requires a public no-arg constructor
  }

  /**
   * Package-private constructor for testing with an injected STS client.
   * <p>
   * This constructor is visible for testing only; production code must use the
   * no-arg constructor followed by {@link #init(Map)}.
   *
   * @param stsClient the STS client to use (must not be null)
   * @param roleArn the IAM role ARN (must not be null or blank)
   * @param roleSessionName the session name (may be null for default)
   * @param durationSeconds the credential duration in seconds (may be null)
   */
  AwsStsCredentialProvider(StsClient stsClient, String roleArn, String roleSessionName,
      Integer durationSeconds) {
    this.config = new ResolvedConfig(roleArn, roleSessionName, durationSeconds,
        null, null, stsClient);
  }

  @Override
  public void init(Map<String, String> conf) {
    if (this.config != null) {
      throw new IllegalStateException("AwsStsCredentialProvider is already initialized");
    }

    String roleArn = conf.get(CONF_ROLE_ARN);
    if (roleArn != null) {
      roleArn = roleArn.trim();
    }
    if (roleArn == null || roleArn.isBlank()) {
      throw new IllegalArgumentException(
          "Configuration key '" + CONF_ROLE_ARN + "' is required but was not set. "
              + "Specify the ARN of the IAM role to assume via AssumeRoleWithWebIdentity.");
    }

    String roleSessionName = conf.get(CONF_SESSION_NAME);
    if (roleSessionName != null) {
      roleSessionName = roleSessionName.trim();
    }
    if (roleSessionName != null && roleSessionName.isBlank()) {
      roleSessionName = null;
    }
    if (roleSessionName != null && !SESSION_NAME_VALID_PATTERN.matcher(roleSessionName).matches()) {
      throw new IllegalArgumentException(
          "Configuration key '" + CONF_SESSION_NAME
              + "' must match [a-zA-Z0-9_+=,.@-]{2,64}, got: "
              + roleSessionName);
    }
    String regionStr = conf.get(CONF_REGION);
    if (regionStr != null) {
      regionStr = regionStr.trim();
    }
    String stsEndpoint = conf.get(CONF_STS_ENDPOINT);
    if (stsEndpoint != null) {
      stsEndpoint = stsEndpoint.trim();
    }

    Integer durationSeconds = null;
    String durationStr = conf.get(CONF_DURATION_SECONDS);
    if (durationStr != null && !durationStr.isBlank()) {
      try {
        durationSeconds = Integer.parseInt(durationStr.trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Configuration key '" + CONF_DURATION_SECONDS
                + "' must be a valid integer, got: " + durationStr, e);
      }
      if (durationSeconds < MIN_DURATION_SECONDS || durationSeconds > MAX_DURATION_SECONDS) {
        throw new IllegalArgumentException(
            "Configuration key '" + CONF_DURATION_SECONDS + "' must be between "
                + MIN_DURATION_SECONDS + " and " + MAX_DURATION_SECONDS
                + " seconds, got: " + durationSeconds);
      }
    }

    // Resolve the region and endpoint before building the client
    Region resolvedRegion = resolveRegion(regionStr, stsEndpoint);
    URI endpointOverride = resolveEndpoint(stsEndpoint);

    StsClient stsClient = buildStsClient(resolvedRegion, endpointOverride);

    // Single volatile write publishes all configuration atomically
    this.config = new ResolvedConfig(roleArn, roleSessionName, durationSeconds,
        resolvedRegion, endpointOverride, stsClient);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    ResolvedConfig cfg = this.config;
    if (cfg != null && cfg.stsClient != null) {
      cfg.stsClient.close();
    }
  }

  /**
   * Resolves the AWS region based on explicit configuration and endpoint presence.
   * When a custom endpoint is provided without an explicit region, defaults to us-east-1.
   * When neither region nor stsEndpoint is configured, returns null so the STS client
   * falls back to the AWS SDK default region resolution (AWS_REGION / AWS_DEFAULT_REGION
   * environment variables, then the ~/.aws/config profile).
   */
  private static Region resolveRegion(String regionStr, String stsEndpoint) {
    if (regionStr != null && !regionStr.isBlank()) {
      return Region.of(regionStr);
    } else if (stsEndpoint != null && !stsEndpoint.isBlank()) {
      // When a custom endpoint is set but no explicit region, use a default region.
      // The region is required by the SDK but not meaningful for non-AWS endpoints.
      return Region.of(DEFAULT_REGION);
    }
    return null;
  }

  /**
   * Resolves the endpoint override URI from configuration.
   */
  private static URI resolveEndpoint(String stsEndpoint) {
    if (stsEndpoint != null && !stsEndpoint.isBlank()) {
      try {
        return URI.create(stsEndpoint);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Configuration key '" + CONF_STS_ENDPOINT + "' contains a malformed URI: "
                + stsEndpoint, e);
      }
    }
    return null;
  }

  /**
   * Builds the STS client with the resolved region and endpoint.
   */
  private static StsClient buildStsClient(Region resolvedRegion, URI endpointOverride) {
    StsClientBuilder builder = StsClient.builder()
        // AssumeRoleWithWebIdentity does not require AWS credentials;
        // the OIDC token itself serves as the authentication mechanism.
        .credentialsProvider(AnonymousCredentialsProvider.create());

    if (resolvedRegion != null) {
      builder.region(resolvedRegion);
    }

    if (endpointOverride != null) {
      builder.endpointOverride(endpointOverride);
    }

    return builder.build();
  }

  /**
   * Returns the resolved configuration for testing purposes.
   * Package-private visibility allows test assertions on resolved region/endpoint.
   */
  ResolvedConfig resolvedConfig() {
    return config;
  }

  @Override
  public Set<String> supportedSchemes() {
    return Set.of("s3a");
  }

  @Override
  public ServiceCredential resolve(UserContext user, URI target)
      throws CredentialResolutionException {
    if (closed) {
      throw new CredentialResolutionException("provider is closed");
    }
    if (user == null) {
      throw new CredentialResolutionException(
          "UserContext must not be null when resolving AWS credentials");
    }
    if (target == null) {
      throw new CredentialResolutionException(
          "Target URI must not be null when resolving AWS credentials");
    }
    String rawToken = user.getRawToken();
    if (rawToken == null || rawToken.isBlank()) {
      throw new CredentialResolutionException(
          "UserContext raw token must not be null or blank; cannot perform "
              + "AssumeRoleWithWebIdentity without an identity token");
    }

    ResolvedConfig cfg = this.config;
    if (cfg == null) {
      throw new CredentialResolutionException("resolve() called before init()");
    }
    String sessionName = cfg.roleSessionName;
    if (sessionName == null || sessionName.isBlank()) {
      // Derive from principal, sanitizing for STS session name constraints
      // (alphanumeric, =,.@- only, max 64 chars)
      String principal = user.getPrincipal();
      if (principal != null && !principal.isBlank()) {
        sessionName = sanitizeSessionName(principal);
      } else {
        sessionName = DEFAULT_SESSION_NAME;
      }
    }

    try {
      AssumeRoleWithWebIdentityRequest.Builder reqBuilder =
          AssumeRoleWithWebIdentityRequest.builder()
              .roleArn(cfg.roleArn)
              .roleSessionName(sessionName)
              .webIdentityToken(rawToken);

      if (cfg.durationSeconds != null) {
        reqBuilder.durationSeconds(cfg.durationSeconds);
      }

      AssumeRoleWithWebIdentityResponse response =
          cfg.stsClient.assumeRoleWithWebIdentity(reqBuilder.build());

      Credentials creds = response.credentials();
      if (creds == null || creds.accessKeyId() == null
          || creds.secretAccessKey() == null || creds.sessionToken() == null) {
        throw new CredentialResolutionException(
            "STS returned incomplete credentials for role '" + cfg.roleArn + "'");
      }

      Map<String, String> properties = Map.of(
          "fs.s3a.access.key", creds.accessKeyId(),
          "fs.s3a.secret.key", creds.secretAccessKey(),
          "fs.s3a.session.token", creds.sessionToken()
      );

      Instant expiration = creds.expiration();
      return new ServiceCredential(properties, expiration);
    } catch (SdkException e) {
      // SECURITY: Never include the token in exception messages.
      // Defensively strip any occurrence of the raw token from the STS error message
      // in case the service accidentally echoed it.
      String errorMsg = e.getMessage();
      String redactedMsg = errorMsg;
      if (redactedMsg != null && rawToken != null && redactedMsg.contains(rawToken)) {
        redactedMsg = redactedMsg.replace(rawToken, "[REDACTED]");
      }
      // Walk the entire cause chain to check if the token leaked into any layer.
      boolean tokenInAnyMessage = causeChainContainsToken(e, rawToken);
      Throwable cause;
      if (tokenInAnyMessage || (errorMsg != null && errorMsg.contains(rawToken))) {
        // Drop the original cause chain entirely; replace with a sanitized wrapper.
        cause = SdkException.builder()
            .message(redactedMsg)
            .build();
      } else {
        cause = e;
      }
      throw new CredentialResolutionException(
          "Failed to assume role '" + cfg.roleArn + "' via AssumeRoleWithWebIdentity: "
              + redactedMsg, cause);
    } catch (IllegalStateException e) {
      // The SDK throws IllegalStateException when the client has been closed.
      throw new CredentialResolutionException(
          "Failed to assume role '" + cfg.roleArn + "' via AssumeRoleWithWebIdentity: "
              + "the STS client has been closed", e);
    }
  }

  @Override
  public Duration suggestedTtl() {
    ResolvedConfig cfg = this.config;
    if (cfg != null && cfg.durationSeconds != null) {
      return Duration.ofSeconds(cfg.durationSeconds);
    }
    return Duration.ofMinutes(15);
  }

  @Override
  public Map<String, String> additionalSparkProperties() {
    return Map.of(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.spark.security.aws.SparkOidcAwsCredentialsProvider");
  }

  /**
   * Walks the cause chain of the given throwable and checks whether the raw token
   * appears in any layer's message. Used to decide whether the original exception
   * chain is safe to preserve as a cause.
   */
  private static boolean causeChainContainsToken(Throwable root, String rawToken) {
    if (rawToken == null) {
      return false;
    }
    return causeChainStream(root)
        .anyMatch(t -> t.getMessage() != null && t.getMessage().contains(rawToken));
  }

  /**
   * Returns a sequential stream over the cause chain starting from {@code root}.
   * Uses identity-based cycle detection to guard against circular cause chains.
   */
  private static Stream<Throwable> causeChainStream(Throwable root) {
    Stream.Builder<Throwable> builder = Stream.builder();
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = root;
    while (current != null && visited.add(current)) {
      builder.accept(current);
      current = current.getCause();
    }
    return builder.build();
  }

  /**
   * Sanitizes a principal string to be valid as an STS role session name.
   * STS session names must match [a-zA-Z0-9_+=,.@-]{2,64}. Both the validation
   * pattern and the sanitization replacement use an explicit ASCII character class
   * rather than {@code \w} to avoid locale-dependent behavior and to reject
   * non-ASCII characters that AWS STS would not accept.
   */
  static String sanitizeSessionName(String principal) {
    String sanitized = SESSION_NAME_INVALID_CHARS.matcher(principal).replaceAll("-");
    if (sanitized.length() > 64) {
      sanitized = sanitized.substring(0, 64);
    }
    if (sanitized.length() < 2) {
      return DEFAULT_SESSION_NAME;
    }
    return sanitized;
  }
}
