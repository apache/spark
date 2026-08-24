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
 * <p>
 * Implementations must be thread-safe: {@code resolve()} may be called concurrently from
 * multiple threads after {@code init()} completes.
 *
 * @since 4.4.0
 */
@DeveloperApi
public interface CredentialProvider extends AutoCloseable {

  /**
   * Initializes this provider with configuration properties.
   * <p>
   * Called exactly once per provider instance by {@link CredentialProviderLoader}
   * (first-conf-wins semantics). Subsequent resolutions reuse the already-initialized
   * instance without re-calling this method. Implementations should capture any configuration
   * they need (e.g., endpoint URLs, role ARNs) from the provided map.
   * <p>
   * The configuration map passed to this method is scoped to keys starting with
   * {@code spark.security.oidc.} only. Keys from other subsystems are not included,
   * preventing accidental leakage of unrelated secrets to third-party providers.
   * <p>
   * If init() throws, it may be retried on the next resolution attempt. Implementations
   * should be safe to call again after a prior failure.
   *
   * @param conf Spark configuration properties scoped to {@code spark.security.oidc.*}
   *     keys (must not be null)
   * @since 4.4.0
   */
  void init(Map<String, String> conf);

  /**
   * Returns the set of URI schemes this provider supports (e.g., {@code {"s3a"}}).
   * <p>
   * Scheme values are compared case-insensitively (normalized to lowercase). The returned
   * set must be non-empty and stable across calls.
   *
   * @return a non-empty set of supported scheme names
   * @since 4.4.0
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
   * @since 4.4.0
   */
  ServiceCredential resolve(UserContext user, URI target) throws CredentialResolutionException;

  /**
   * Returns the suggested time-to-live for credentials produced by this provider.
   * <p>
   * The credential management layer uses this as a hint for refresh scheduling.
   * The default is 15 minutes.
   *
   * @return the suggested credential TTL (never null)
   * @since 4.4.0
   */
  default Duration suggestedTtl() {
    return Duration.ofMinutes(15);
  }

  /**
   * Returns additional Spark configuration properties that should be set when this
   * provider is active.
   * <p>
   * This method is called after {@link #init(Map)} and a successful
   * {@link #resolve(UserContext, URI)} invocation. Implementations may
   * assume that provider state is fully initialized when this is called.
   * <p>
   * The credential management layer applies these entries to {@code SparkConf} after
   * successful startup, only if the user has not already set them explicitly. This
   * allows provider modules to declare executor-side wiring (e.g., the Hadoop
   * credentials provider class for a particular filesystem scheme) without requiring
   * core to have vendor-specific knowledge.
   * <p>
   * Keys must use the {@code spark.} prefix to be effective (SparkConf convention).
   * Keys with the {@code spark.hadoop.} prefix are propagated to executor-side
   * Hadoop {@code Configuration} with the prefix stripped. Other {@code spark.*}
   * keys are applied as Spark-internal configuration.
   * <p>
   * The default implementation returns an empty map (no additional properties).
   *
   * @return an unmodifiable map of property key-value pairs (never null).
   *         Keys and values within the map must not be {@code null}.
   * @since 4.4.0
   */
  default Map<String, String> additionalSparkProperties() {
    return Map.of();
  }

  /**
   * Releases any resources held by this provider (e.g., HTTP clients, connection pools).
   * <p>
   * Called by the credential management layer during shutdown. The default implementation
   * is a no-op; providers that allocate long-lived resources in {@link #init(Map)} should
   * override this method to clean them up.
   * <p>
   * {@code close()} may be invoked while another thread is still executing
   * {@link #resolve(UserContext, URI)}: shutdown interrupts the renewal thread but does
   * not wait for in-flight calls to complete. Implementations must tolerate a concurrent
   * or subsequent {@code resolve()} failing after resources have been released, and
   * {@code close()} itself must not block indefinitely.
   * <p>
   * Implementations that do not throw checked exceptions may narrow the {@code throws}
   * clause in their override (e.g., declare {@code close()} with no {@code throws} or
   * with a more specific exception type).
   *
   * @since 4.4.0
   */
  @Override
  default void close() throws Exception {}
}
