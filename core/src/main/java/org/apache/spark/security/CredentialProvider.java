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
import java.util.Map;
import java.util.Set;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Service Provider Interface for credential resolution in the OIDC credential propagation
 * framework.
 * <p>
 * Implementations exchange a user's identity (represented by {@link UserContext}) for a
 * short-lived {@link ServiceCredential} scoped to a target URI. Providers are discovered
 * via {@link java.util.ServiceLoader} and selected based on the URI scheme.
 *
 * @since 4.3.0
 */
@DeveloperApi
public interface CredentialProvider {

  /**
   * Initializes this provider with configuration properties.
   * <p>
   * Called exactly once by {@link CredentialProviderLoader} on first selection of this
   * provider (first-conf-wins semantics). Subsequent resolutions reuse the already-initialized
   * instance without re-calling this method. Implementations should capture any configuration
   * they need (e.g., endpoint URLs, role ARNs) from the provided map.
   *
   * @param conf Spark configuration properties as a string map (must not be null)
   * @since 4.3.0
   */
  void init(Map<String, String> conf);

  /**
   * Returns the set of URI schemes this provider supports (e.g., {@code {"s3a"}}).
   * <p>
   * Scheme values are compared case-insensitively (normalized to lowercase). The returned
   * set must be non-empty and stable across calls.
   *
   * @return a non-empty set of supported scheme names
   * @since 4.3.0
   */
  Set<String> supportedSchemes();

  /**
   * Exchanges the user's identity for a short-lived service credential scoped to the
   * given target URI.
   * <p>
   * For example, an AWS implementation might call STS AssumeRoleWithWebIdentity using
   * the raw token from the {@link UserContext} and return temporary AWS credentials as
   * a {@link ServiceCredential}.
   *
   * @param user the authenticated user context containing the identity token (must not be null)
   * @param target the target URI for which credentials are requested (must not be null)
   * @return a short-lived service credential for the target
   * @throws CredentialResolutionException if the credential exchange fails
   * @since 4.3.0
   */
  ServiceCredential resolve(UserContext user, URI target) throws CredentialResolutionException;

  /**
   * Returns the suggested time-to-live for credentials produced by this provider.
   * <p>
   * The credential management layer uses this as a hint for refresh scheduling.
   * The default is 15 minutes.
   *
   * @return the suggested credential TTL (never null)
   * @since 4.3.0
   */
  default Duration suggestedTtl() {
    return Duration.ofMinutes(15);
  }
}
