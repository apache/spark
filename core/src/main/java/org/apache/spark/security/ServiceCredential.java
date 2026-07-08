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

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * A short-lived, service-specific credential derived from the user's identity token.
 * <p>
 * Instances are produced by credential providers on the driver and transmitted
 * to executors via {@link UserCredentials}. The {@code properties} map holds
 * service-specific key-value pairs (e.g., temporary AWS credentials).
 * <p>
 * This class is immutable and {@link Serializable}.
 *
 * @since 5.0.0
 */
@DeveloperApi
public final class ServiceCredential implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<String, String> properties;
  private final Instant expiresAt;

  /**
   * Constructs a new {@code ServiceCredential}.
   *
   * @param properties service-specific credential properties (must not be null; defensively copied)
   * @param expiresAt credential expiry time (may be null)
   */
  public ServiceCredential(Map<String, String> properties, Instant expiresAt) {
    Objects.requireNonNull(properties, "properties must not be null");
    this.properties = new HashMap<>(properties);
    this.expiresAt = expiresAt;
  }

  /**
   * Returns an unmodifiable view of the credential properties.
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  /** Returns the credential expiry time, or {@code null} if not set. */
  public Instant getExpiresAt() {
    return expiresAt;
  }

  /**
   * Returns {@code true} if this credential has expired relative to the given instant.
   * If {@code expiresAt} is {@code null}, this method returns {@code false}.
   *
   * @param now the current time to compare against (must not be null)
   * @return whether the credential is expired
   */
  public boolean isExpired(Instant now) {
    Objects.requireNonNull(now, "now must not be null");
    return expiresAt != null && !now.isBefore(expiresAt);
  }

  @Override
  public String toString() {
    String redactedProps;
    if (properties == null || properties.isEmpty()) {
      redactedProps = "{}";
    } else {
      StringBuilder sb = new StringBuilder("{");
      boolean first = true;
      for (String key : properties.keySet()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(key).append("=[REDACTED]");
        first = false;
      }
      sb.append("}");
      redactedProps = sb.toString();
    }
    return "ServiceCredential{" +
        "properties=" + redactedProps +
        ", expiresAt=" + expiresAt +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServiceCredential that = (ServiceCredential) o;
    return properties.equals(that.properties)
        && Objects.equals(expiresAt, that.expiresAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, expiresAt);
  }
}
