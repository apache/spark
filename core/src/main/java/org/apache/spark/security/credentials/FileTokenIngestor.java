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

package org.apache.spark.security.credentials;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.security.UserContext;

/**
 * :: DeveloperApi ::
 * A {@link TokenIngestor} that reads an OIDC identity token from a file.
 * <p>
 * The file path is typically a Kubernetes projected service account token
 * (e.g., {@code /var/run/secrets/tokens/spark-identity}) or a path configured via
 * {@code spark.security.credentials.identityToken.file}.
 * <p>
 * This implementation:
 * <ul>
 *   <li>Detects file rotation via mtime change (only re-parses when the file changes)</li>
 *   <li>Parses JWT claims without signature verification (unsigned claims parsing)
 *       since the token is trusted from the local filesystem</li>
 *   <li>Handles errors gracefully: malformed JWT, missing file, or empty content
 *       returns empty rather than throwing</li>
 * </ul>
 *
 * @since 4.3.0
 */
@DeveloperApi
public class FileTokenIngestor implements TokenIngestor {

  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(FileTokenIngestor.class);

  private final Path tokenPath;
  private volatile long lastMtime = -1L;
  private volatile UserContext cachedContext = null;

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

      // Read the token contents and check for emptiness
      String content = new String(Files.readAllBytes(tokenPath), "UTF-8").trim();
      if (content.isEmpty()) {
        LOG.warn("Token file is empty: " + tokenPath);
        return Optional.empty();
      }

      Optional<UserContext> userContext = parseJwt(content);
      if (userContext.isPresent()) {
        lastMtime = currentMtime;
        cachedContext = userContext.get();
      }
      return userContext;
    } catch (Exception e) {
      LOG.warn("Failed to load token from " + tokenPath, e);
      return Optional.empty();
    }
  }

  /**
   * Parse a JWT token string into a UserContext using unsigned claims parsing.
   * The token is trusted from the local filesystem. No need to verify the signature.
   */
  private Optional<UserContext> parseJwt(String token) {
    try {
      Claims claims = Jwts.parser()
          .unsecured()
          .build()
          .parseUnsecuredClaims(token)
          .getPayload();

      String subject = claims.getSubject();
      String issuer = claims.getIssuer();

      if (subject == null || subject.isEmpty()) {
        LOG.warn("JWT token missing required 'sub' claim");
        return Optional.empty();
      }
      if (issuer == null || issuer.isEmpty()) {
        LOG.warn("JWT token missing required 'iss' claim");
        return Optional.empty();
      }

      Date issuedAtDate = claims.getIssuedAt();
      Date expirationDate = claims.getExpiration();
      Instant issuedAt = issuedAtDate != null ? issuedAtDate.toInstant() : null;
      Instant expiresAt = expirationDate != null ? expirationDate.toInstant() : null;

      return Optional.of(new UserContext(subject, issuer, token, issuedAt, expiresAt));
    } catch (Exception e) {
      LOG.warn("Failed to parse JWT token", e);
      return Optional.empty();
    }
  }
}
