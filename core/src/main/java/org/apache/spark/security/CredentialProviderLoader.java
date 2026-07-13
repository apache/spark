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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * Discovers {@link CredentialProvider} implementations via {@link ServiceLoader} and selects
 * the appropriate provider for a given URI scheme using Binding Policy A (explicit selection).
 * <p>
 * Provider discovery happens once (lazily on first call) and the list is cached. Each provider
 * is initialized exactly once per provider instance via {@link CredentialProvider#init(Map)}
 * with the configuration from the first call that selects it (first-conf-wins semantics);
 * subsequent resolutions reuse the already-initialized instance without re-calling {@code init}.
 * <p>
 * When the configuration key {@code spark.security.credentials.provider.<scheme>} is set
 * (non-empty), the loader validates the configured fully-qualified class name against the
 * discovered candidates for that scheme regardless of candidate count. If the configured class
 * does not match any candidate, an {@link IllegalArgumentException} is thrown listing the
 * scheme, the configured class, and the available candidates. Only when the configuration key
 * is unset (or empty) do the count-based rules apply: a single candidate is auto-selected;
 * multiple candidates produce an ambiguity error; no candidates produce {@code Optional.empty()}.
 * <p>
 * <b>Thread-safety:</b> This class uses synchronized access to the cached provider list and
 * initialization tracking. Callers may invoke {@link #providerFor(String, Map)} from multiple
 * threads. However, the returned {@link CredentialProvider} instances are not guaranteed to be
 * thread-safe; callers should synchronize on the provider or confine it to a single thread.
 *
 * @since 4.3.0
 */
@Private
public final class CredentialProviderLoader {

  /**
   * Configuration key prefix for explicit provider selection per scheme.
   * When set (non-empty), the configured fully-qualified class name must match a discovered
   * provider that supports the scheme; a mismatch results in an {@link IllegalArgumentException}.
   */
  private static final String CONF_PREFIX = "spark.security.credentials.provider.";

  /**
   * Configuration key prefix used to scope the configuration passed to
   * {@link CredentialProvider#init(Map)}. Only keys starting with this prefix are forwarded,
   * preventing unrelated secrets from leaking to third-party provider implementations.
   */
  private static final String CREDENTIALS_CONF_PREFIX = "spark.security.credentials.";

  private static volatile List<CredentialProvider> cachedProviders;

  /**
   * Tracks which provider instances have already been initialized. Guarded by the class lock.
   * Uses identity semantics (reference equality) to handle multiple provider instances correctly.
   */
  private static final Set<CredentialProvider> initializedProviders =
      Collections.newSetFromMap(new IdentityHashMap<>());

  private CredentialProviderLoader() {
    // utility class
  }

  /**
   * Returns the {@link CredentialProvider} for the given URI scheme, applying Binding Policy A:
   * <ol>
   *   <li>If no candidate supports the scheme, {@link Optional#empty()} is returned.</li>
   *   <li>If {@code spark.security.credentials.provider.<scheme>} is set (non-empty) in
   *       {@code conf}, the provider whose fully-qualified class name matches is selected
   *       regardless of candidate count. If the configured class does not match any candidate,
   *       an {@link IllegalArgumentException} is thrown naming the scheme, the configured class,
   *       and the available candidates.</li>
   *   <li>If unset and exactly one candidate supports the scheme, that candidate is selected.</li>
   *   <li>If unset and multiple candidates support the scheme, an
   *       {@link IllegalArgumentException} is thrown listing the candidates.</li>
   * </ol>
   * The selected provider is initialized exactly once per provider instance via
   * {@link CredentialProvider#init(Map)} (first-conf-wins semantics); later resolutions reuse
   * the initialized instance without re-calling {@code init}. The configuration passed to
   * {@code init()} is scoped to {@code spark.security.credentials.*} keys only; keys from
   * other subsystems are filtered out to prevent secret leakage to third-party providers.
   * <p>
   * Spark-internal callers pass their configuration as a {@code Map<String, String>} for
   * testability and to match the signature of {@link CredentialProvider#init(Map)}.
   *
   * @param scheme the URI scheme (e.g., "s3a"); normalized to lowercase
   * @param conf Spark configuration properties as a string map
   * @return the selected provider, or empty if no provider supports the scheme
   * @throws IllegalArgumentException if explicit selection names an unknown or non-supporting
   *     class, or if multiple candidates exist without explicit selection
   * @throws IllegalStateException if a provider returns null from {@code supportedSchemes()}
   */
  public static Optional<CredentialProvider> providerFor(String scheme, Map<String, String> conf) {
    Objects.requireNonNull(scheme, "scheme must not be null");
    Objects.requireNonNull(conf, "conf must not be null");
    if (scheme.isEmpty()) {
      throw new IllegalArgumentException("scheme must not be empty");
    }
    String normalizedScheme = scheme.toLowerCase(Locale.ROOT);
    List<CredentialProvider> providers = getProviders();

    List<CredentialProvider> candidates = providers.stream()
        .filter(p -> {
          Set<String> schemes = p.supportedSchemes();
          if (schemes == null) {
            throw new IllegalStateException(
                "Provider " + p.getClass().getName()
                    + " returned null from supportedSchemes()");
          }
          return schemes.stream()
              .anyMatch(s -> s.toLowerCase(Locale.ROOT).equals(normalizedScheme));
        })
        .collect(Collectors.toList());

    if (candidates.isEmpty()) {
      return Optional.empty();
    }

    String confKey = CONF_PREFIX + normalizedScheme;
    String explicitClass = conf.get(confKey);

    CredentialProvider selected;
    if (explicitClass != null && !explicitClass.isEmpty()) {
      selected = candidates.stream()
          .filter(p -> p.getClass().getName().equals(explicitClass))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              "Configured credential provider class '" + explicitClass + "' for scheme '"
                  + normalizedScheme + "' (key: " + confKey
                  + ") was not found among candidates or does not support the scheme. "
                  + "Available candidates: "
                  + candidates.stream()
                      .map(p -> p.getClass().getName())
                      .collect(Collectors.joining(", "))));
    } else if (candidates.size() == 1) {
      selected = candidates.get(0);
    } else {
      String candidateNames = candidates.stream()
          .map(p -> p.getClass().getName())
          .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "Multiple credential providers found for scheme '" + normalizedScheme
              + "'. Set " + confKey + " to one of: " + candidateNames);
    }

    // Initialize exactly once under the lock (first-conf-wins).
    // Only pass spark.security.credentials.* keys to init() to avoid leaking secrets
    // from other subsystems to third-party ServiceLoader providers. This follows the
    // precedent of DataSourceV2Utils.extractSessionConfigs() which scopes configuration
    // to a specific prefix. We keep the full key (unlike extractSessionConfigs which
    // strips the prefix) so providers can distinguish sub-keys unambiguously.
    synchronized (CredentialProviderLoader.class) {
      if (!initializedProviders.contains(selected)) {
        Map<String, String> filteredConf = new HashMap<>();
        for (Map.Entry<String, String> entry : conf.entrySet()) {
          if (entry.getKey().startsWith(CREDENTIALS_CONF_PREFIX)) {
            filteredConf.put(entry.getKey(), entry.getValue());
          }
        }
        selected.init(Collections.unmodifiableMap(filteredConf));
        initializedProviders.add(selected);
      }
    }
    return Optional.of(selected);
  }

  /**
   * Returns the cached list of discovered providers, loading them on first access.
   */
  private static List<CredentialProvider> getProviders() {
    List<CredentialProvider> providers = cachedProviders;
    if (providers == null) {
      synchronized (CredentialProviderLoader.class) {
        providers = cachedProviders;
        if (providers == null) {
          providers = loadProviders();
          cachedProviders = providers;
        }
      }
    }
    return providers;
  }

  private static List<CredentialProvider> loadProviders() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = CredentialProvider.class.getClassLoader();
    }
    ServiceLoader<CredentialProvider> loader = ServiceLoader.load(CredentialProvider.class, cl);
    List<CredentialProvider> result = new ArrayList<>();
    for (CredentialProvider provider : loader) {
      result.add(provider);
    }
    return result;
  }

  /**
   * Resets the cached provider list and initialization tracking. Intended for testing only.
   */
  static void resetForTesting() {
    synchronized (CredentialProviderLoader.class) {
      cachedProviders = null;
      initializedProviders.clear();
    }
  }

  /**
   * Overrides the cached provider list for testing. Intended for testing only.
   */
  static void setProvidersForTesting(List<CredentialProvider> providers) {
    synchronized (CredentialProviderLoader.class) {
      cachedProviders = providers;
      initializedProviders.clear();
    }
  }
}
