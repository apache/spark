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
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * A bundle of {@link ServiceCredential} instances keyed by scheme (e.g., "s3a", "abfss").
 * <p>
 * Scheme keys are normalized to lowercase ({@link Locale#ROOT}) at construction time, and
 * lookups via {@link #forScheme(String)} are case-insensitive. If the supplied map contains
 * keys that differ only by case, the last entry (in iteration order) wins.
 * <p>
 * This class is transmitted to executors and does <b>not</b> contain any reference
 * to {@link UserContext} or raw identity tokens. It is immutable and {@link Serializable}.
 *
 * @since 4.3.0
 */
@DeveloperApi
public final class UserCredentials implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<String, ServiceCredential> credentials;

  /**
   * Constructs a new {@code UserCredentials} bundle.
   * <p>
   * Scheme keys are normalized to lowercase using {@link Locale#ROOT}. If multiple keys
   * collide after lowercasing, the last entry in iteration order wins.
   *
   * @param credentials per-scheme map of service credentials (must not be null; defensively copied)
   */
  public UserCredentials(Map<String, ServiceCredential> credentials) {
    Objects.requireNonNull(credentials, "credentials must not be null");
    Map<String, ServiceCredential> normalized = new HashMap<>(credentials.size());
    for (Map.Entry<String, ServiceCredential> entry : credentials.entrySet()) {
      normalized.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
    }
    this.credentials = normalized;
  }

  /**
   * Returns an unmodifiable view of all credentials keyed by scheme.
   */
  public Map<String, ServiceCredential> getCredentials() {
    return Collections.unmodifiableMap(credentials);
  }

  /**
   * Looks up the {@link ServiceCredential} for the given scheme. The lookup is
   * case-insensitive (the argument is lowercased with {@link Locale#ROOT}).
   *
   * @param scheme the target scheme (e.g., "s3a", "S3A"); must not be null
   * @return an {@link Optional} containing the credential, or empty if no credential is registered
   */
  public Optional<ServiceCredential> forScheme(String scheme) {
    Objects.requireNonNull(scheme, "scheme must not be null");
    return Optional.ofNullable(credentials.get(scheme.toLowerCase(Locale.ROOT)));
  }

  @Override
  public String toString() {
    return "UserCredentials{" +
        "credentials=" + credentials +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UserCredentials that = (UserCredentials) o;
    return credentials.equals(that.credentials);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credentials);
  }
}
