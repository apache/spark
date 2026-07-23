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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.spark.annotation.Private;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * A {@link TokenIngestor} that reads an OIDC identity token from a file.
 * <p>
 * The file path is typically a Kubernetes projected service account token
 * (e.g., {@code /var/run/secrets/tokens/spark-identity}) or a path configured via
 * {@code spark.security.credentials.identityToken.file}.
 * <p>
 * This implementation:
 * <ul>
 *   <li>Detects file rotation via mtime change (only re-parses when the file changes)</li>
 *   <li>Parses JWT claims by Base64-decoding the payload segment directly, without
 *       signature verification, since the token is trusted from the local filesystem
 *       and works with both signed (RS256, ES256) and unsigned tokens</li>
 *   <li>Handles errors gracefully: malformed JWT, missing file, or empty content
 *       returns empty rather than throwing</li>
 * </ul>
 *
 * @since 4.3.0
 */
@Private
public class FileTokenIngestor implements TokenIngestor {

  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(FileTokenIngestor.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Path tokenPath;

  // Cached state for rotation detection.
  // Write order matters for thread-safety: cachedContext must be visible before
  // lastMtime, so a concurrent reader never sees a new mtime with a stale context.
  private volatile UserContext cachedContext = null;
  private volatile long lastMtime = -1L;

  /**
   * Construct a new FileTokenIngestor.
   *
   * @param tokenPath path to the identity token file (must not be null)
   */
  public FileTokenIngestor(Path tokenPath) {
    this.tokenPath = tokenPath;
  }

  @Override
  public Optional<UserContext> load() {
    try {
      if (!Files.exists(tokenPath)) {
        LOG.debug("Token file does not exist: {}", tokenPath);
        return Optional.empty();
      }

      // File did not change since last successful parse
      long currentMtime = Files.getLastModifiedTime(tokenPath).toMillis();
      if (currentMtime == lastMtime && cachedContext != null) {
        return Optional.of(cachedContext);
      }

      String content = new String(Files.readAllBytes(tokenPath), StandardCharsets.UTF_8).trim();
      if (content.isEmpty()) {
        LOG.warn("Token file is empty: {}", MDC.of(LogKeys.PATH, tokenPath));
        return Optional.empty();
      }

      Optional<UserContext> userContext = parseJwt(content);
      if (userContext.isPresent()) {
        // If the new file has invalid content, return empty and do NOT
        // fall back to the previously cached context. The caller will retry on next poll.
        cachedContext = userContext.get();
        lastMtime = currentMtime;
      }
      return userContext;
    } catch (Exception e) {
      LOG.warn("Failed to load token from {}: {}", e,
          MDC.of(LogKeys.PATH, tokenPath),
          MDC.of(LogKeys.CLASS_NAME, e.getClass().getName()));
      return Optional.empty();
    }
  }

  /**
   * Parse a JWT token string into a UserContext by Base64-decoding the payload segment.
   * This works with both signed (RS256, ES256) and unsigned (alg:none) tokens since
   * we never verify the signature - the token is trusted from the local filesystem and
   * will be re-verified downstream at the STS token exchange.
   */
  private Optional<UserContext> parseJwt(String token) {
    try {
      String[] parts = token.split("\\.");
      if (parts.length < 2) {
        LOG.warn("JWT token does not have the expected header.payload format");
        return Optional.empty();
      }

      // Decode the payload (second segment) - works for both signed and unsigned JWTs
      byte[] payloadBytes = Base64.getUrlDecoder().decode(parts[1]);
      JsonNode claims = MAPPER.readTree(payloadBytes);

      String subject = claims.has("sub") ? claims.get("sub").asText() : null;
      String issuer = claims.has("iss") ? claims.get("iss").asText() : null;

      if (subject == null || subject.isEmpty()) {
        LOG.warn("JWT token missing required 'sub' claim");
        return Optional.empty();
      }
      if (issuer == null || issuer.isEmpty()) {
        LOG.warn("JWT token missing required 'iss' claim");
        return Optional.empty();
      }

      Instant issuedAt = claims.has("iat")
          ? Instant.ofEpochSecond(claims.get("iat").asLong()) : null;
      Instant expiresAt = claims.has("exp")
          ? Instant.ofEpochSecond(claims.get("exp").asLong()) : null;

      return Optional.of(new UserContext(subject, issuer, token, issuedAt, expiresAt));
    } catch (Exception e) {
      LOG.warn("Failed to parse JWT token: {}",
          MDC.of(LogKeys.CLASS_NAME, e.getClass().getName()));
      return Optional.empty();
    }
  }
}
