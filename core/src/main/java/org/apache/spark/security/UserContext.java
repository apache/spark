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

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Represents the authenticated user's identity context on the driver side.
 * <p>
 * This class holds the OIDC token information used to derive short-lived
 * {@link ServiceCredential} instances via credential providers. It is intentionally
 * <b>not</b> {@link java.io.Serializable} and must never be transmitted to executors.
 * The {@code rawToken} field is always redacted in {@link #toString()} and excluded from
 * Jackson serialization.
 *
 * @since 5.0.0
 */
@DeveloperApi
public final class UserContext {

  private final String principal;
  private final String issuer;
  private final String rawToken;
  private final Instant issuedAt;
  private final Instant expiresAt;

  /**
   * Constructs a new {@code UserContext}.
   *
   * @param principal the {@code sub} claim from the JWT (must not be null)
   * @param issuer the {@code iss} claim from the JWT (must not be null)
   * @param rawToken the raw OIDC JWT string (must not be null)
   * @param issuedAt token issue time (may be null)
   * @param expiresAt token expiry time (may be null)
   */
  public UserContext(
      String principal,
      String issuer,
      String rawToken,
      Instant issuedAt,
      Instant expiresAt) {
    this.principal = Objects.requireNonNull(principal, "principal must not be null");
    this.issuer = Objects.requireNonNull(issuer, "issuer must not be null");
    this.rawToken = Objects.requireNonNull(rawToken, "rawToken must not be null");
    this.issuedAt = issuedAt;
    this.expiresAt = expiresAt;
  }

  /** Returns the {@code sub} claim (principal identifier). */
  public String getPrincipal() {
    return principal;
  }

  /** Returns the {@code iss} claim (token issuer). */
  public String getIssuer() {
    return issuer;
  }

  /** Returns the raw OIDC JWT. This value must never be logged or transmitted to executors. */
  @JsonIgnore
  public String getRawToken() {
    return rawToken;
  }

  /** Returns the token issue time, or {@code null} if not set. */
  public Instant getIssuedAt() {
    return issuedAt;
  }

  /** Returns the token expiry time, or {@code null} if not set. */
  public Instant getExpiresAt() {
    return expiresAt;
  }

  /**
   * Returns {@code true} if this context's token has expired relative to the given instant.
   * If {@code expiresAt} is {@code null}, this method returns {@code false}.
   *
   * @param now the current time to compare against (must not be null)
   * @return whether the token is expired
   */
  public boolean isExpired(Instant now) {
    Objects.requireNonNull(now, "now must not be null");
    return expiresAt != null && !now.isBefore(expiresAt);
  }

  /**
   * Returns a string representation with the {@code rawToken} redacted as {@code [REDACTED]}.
   */
  @Override
  public String toString() {
    return "UserContext{" +
        "principal='" + principal + '\'' +
        ", issuer='" + issuer + '\'' +
        ", rawToken='[REDACTED]'" +
        ", issuedAt=" + issuedAt +
        ", expiresAt=" + expiresAt +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UserContext that = (UserContext) o;
    return principal.equals(that.principal)
        && issuer.equals(that.issuer)
        && rawToken.equals(that.rawToken)
        && Objects.equals(issuedAt, that.issuedAt)
        && Objects.equals(expiresAt, that.expiresAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, issuer, rawToken, issuedAt, expiresAt);
  }
}
