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

package org.apache.spark.config;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.protobuf.TextFormat;

import org.apache.spark.config.protobuf.ConfigEntries;
import org.apache.spark.config.protobuf.ConfigEntry;

/**
 * A registry for Spark configuration entries.
 *
 * <p>This class loads configuration entries from .textproto files on the classpath
 * and provides methods to query them by key.
 *
 * <p>For production use, use the static methods which access the default singleton instance.
 * For testing, create a new instance with custom config file paths using the constructor.
 *
 * <p>Config files are organized into two directories:
 * <ul>
 *   <li>cluster_configs/: Contains configs with CLUSTER scope</li>
 *   <li>session_configs/: Contains configs with SESSION scope</li>
 * </ul>
 */
public class ConfigRegistry {

  // Explicit list of config files to load for production (package-private for testing)
  static final String[] DEFAULT_CONFIG_FILES = {
    "org/apache/spark/config/cluster_configs/sql.textproto",
    "org/apache/spark/config/session_configs/sql.textproto"
  };

  // Lazy initialization holder class idiom for thread-safe lazy loading of default instance
  private static class DefaultInstanceHolder {
    static final ConfigRegistry INSTANCE = new ConfigRegistry(DEFAULT_CONFIG_FILES);
  }

  // Instance field: map of config key -> ConfigEntry
  private final Map<String, ConfigEntry> configMap;

  /**
   * Create a ConfigRegistry with custom config file paths.
   *
   * @param configFiles the resource paths of .textproto files to load
   */
  public ConfigRegistry(String... configFiles) {
    this.configMap = loadConfigs(configFiles);
  }

  /**
   * Get a config entry by its key.
   *
   * @param key the config key (e.g., "spark.sql.optimizer.maxIterations")
   * @return the ConfigEntry if found, null otherwise
   */
  public ConfigEntry get(String key) {
    return configMap.get(key);
  }

  /**
   * Check if a config key exists in the registry.
   *
   * @param key the config key
   * @return true if the key exists, false otherwise
   */
  public boolean contains(String key) {
    return configMap.containsKey(key);
  }

  /**
   * Get all registered config keys.
   *
   * @return an unmodifiable set of all config keys
   */
  public Set<String> keys() {
    return configMap.keySet();
  }

  /**
   * Get all registered config entries.
   *
   * @return an unmodifiable collection of all config entries
   */
  public Collection<ConfigEntry> all() {
    return configMap.values();
  }

  // ==========================================================================
  // Static methods for production use (delegate to default singleton instance)
  // ==========================================================================

  /**
   * Get the default singleton instance.
   *
   * @return the default ConfigRegistry instance
   */
  public static ConfigRegistry getInstance() {
    return DefaultInstanceHolder.INSTANCE;
  }

  /**
   * Get a config entry by its key from the default registry.
   *
   * @param key the config key (e.g., "spark.sql.optimizer.maxIterations")
   * @return the ConfigEntry if found, null otherwise
   */
  public static ConfigEntry getConfig(String key) {
    return getInstance().get(key);
  }

  /**
   * Check if a config key exists in the default registry.
   *
   * @param key the config key
   * @return true if the key exists, false otherwise
   */
  public static boolean containsConfig(String key) {
    return getInstance().contains(key);
  }

  /**
   * Get all registered config keys from the default registry.
   *
   * @return an unmodifiable set of all config keys
   */
  public static Set<String> allKeys() {
    return getInstance().keys();
  }

  /**
   * Get all registered config entries from the default registry.
   *
   * @return an unmodifiable collection of all config entries
   */
  public static Collection<ConfigEntry> allConfigs() {
    return getInstance().all();
  }

  // ==========================================================================
  // Private helper methods
  // ==========================================================================

  /**
   * Load configs from .textproto files.
   */
  private static Map<String, ConfigEntry> loadConfigs(String[] configFiles) {
    Map<String, ConfigEntry> result = new HashMap<>();
    ClassLoader classLoader = ConfigRegistry.class.getClassLoader();

    for (String file : configFiles) {
      List<ConfigEntry> configs = loadConfigFile(classLoader, file);
      for (ConfigEntry config : configs) {
        result.put(config.getKey(), config);
      }
    }

    return Collections.unmodifiableMap(result);
  }

  /**
   * Load configs from a single .textproto file.
   * Package-private for testing.
   */
  static List<ConfigEntry> loadConfigFile(
      ClassLoader classLoader,
      String resourcePath) {
    InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
    if (inputStream == null) {
      throw internalError("Config file not found on classpath: " + resourcePath);
    }

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      ConfigEntries.Builder builder = ConfigEntries.newBuilder();
      TextFormat.getParser().merge(reader, builder);
      return builder.build().getConfigsList();
    } catch (Exception e) {
      throw internalError("Failed to load config file: " + resourcePath, e);
    }
  }

  private static AssertionError internalError(String message) {
    return new AssertionError(message);
  }

  private static AssertionError internalError(String message, Throwable cause) {
    return new AssertionError(message, cause);
  }
}
