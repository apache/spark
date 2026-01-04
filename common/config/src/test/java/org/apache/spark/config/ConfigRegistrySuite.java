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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.spark.config.protobuf.ConfigEntry;
import org.apache.spark.config.protobuf.Scope;
import org.apache.spark.config.protobuf.ValueType;
import org.apache.spark.config.protobuf.Visibility;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ConfigRegistry}.
 */
public class ConfigRegistrySuite {

  private static final String TEST_CONFIG_FILE =
      "org/apache/spark/config/test_configs.textproto";

  private static final ConfigRegistry registry = new ConfigRegistry(TEST_CONFIG_FILE);

  private static final ClassLoader CLASS_LOADER = ConfigRegistrySuite.class.getClassLoader();

  // ==========================================================================
  // Validation helpers
  // ==========================================================================

  /**
   * Validate configs in one file: required fields and alphabetical ordering.
   * Convenience method that loads the file first.
   *
   * @param file the resource path of the .textproto file
   */
  private static void validateConfigsInOneFile(String file) {
    List<ConfigEntry> configs = ConfigRegistry.loadConfigFile(CLASS_LOADER, file);
    validateConfigsInOneFile(configs, file);
  }

  /**
   * Validate configs: required fields and alphabetical ordering.
   *
   * @param configs the list of config entries
   * @param file the file path (for error messages)
   */
  private static void validateConfigsInOneFile(List<ConfigEntry> configs, String file) {
    String previousKey = null;
    for (ConfigEntry config : configs) {
      // Validate required fields
      List<String> missingFields = new ArrayList<>();
      if (config.getKey().isEmpty()) {
        missingFields.add("key");
      }
      if (config.getValueType() == ValueType.VALUE_TYPE_UNSPECIFIED) {
        missingFields.add("value_type");
      }
      if (config.getScope() == Scope.SCOPE_UNSPECIFIED) {
        missingFields.add("scope");
      }
      if (config.getVisibility() == Visibility.VISIBILITY_UNSPECIFIED) {
        missingFields.add("visibility");
      }
      if (config.getDoc().isEmpty()) {
        missingFields.add("doc");
      }
      if (!missingFields.isEmpty()) {
        String keyInfo = config.getKey().isEmpty() ? "" : " for key '" + config.getKey() + "'";
        fail("Missing required fields " + missingFields + keyInfo + " in " + file);
      }

      // Validate alphabetical ordering
      String key = config.getKey();
      if (previousKey != null && key.compareTo(previousKey) < 0) {
        fail("Config keys must be ordered alphabetically in " + file +
            ": '" + key + "' should come before '" + previousKey + "'");
      }
      previousKey = key;
    }
  }

  /**
   * Validate all config files: required fields, ordering, and no duplicate keys.
   * Each file is loaded only once.
   *
   * @param configFiles the list of config file paths to check
   */
  private static void validateAllConfigFiles(String[] configFiles) {
    Map<String, String> keyToFile = new HashMap<>();
    for (String file : configFiles) {
      List<ConfigEntry> configs = ConfigRegistry.loadConfigFile(CLASS_LOADER, file);
      // Validate required fields and ordering
      validateConfigsInOneFile(configs, file);
      // Validate no duplicate keys across files
      for (ConfigEntry config : configs) {
        String key = config.getKey();
        if (keyToFile.containsKey(key)) {
          fail("Duplicate config key '" + key + "': " +
              "first defined in " + keyToFile.get(key) + ", duplicated in " + file);
        }
        keyToFile.put(key, file);
      }
    }
  }

  // ==========================================================================
  // Basic functionality tests
  // ==========================================================================

  @Test
  public void testLoadConfigs() {
    assertEquals(4, registry.keys().size());
    assertTrue(registry.contains("spark.test.bool.config"));
    assertTrue(registry.contains("spark.test.int.config"));
    assertTrue(registry.contains("spark.test.string.config"));
    assertTrue(registry.contains("spark.test.long.doc.config"));
  }

  @Test
  public void testBoolConfig() {
    ConfigEntry config = registry.get("spark.test.bool.config");
    assertNotNull(config);
    assertEquals("spark.test.bool.config", config.getKey());
    assertEquals(ValueType.BOOL, config.getValueType());
    assertEquals("true", config.getDefaultValue());
    assertEquals(Scope.SESSION, config.getScope());
    assertEquals(Visibility.PUBLIC, config.getVisibility());
    assertEquals("A test boolean config", config.getDoc());
    assertEquals("4.0.0", config.getVersion());
  }

  @Test
  public void testIntConfig() {
    ConfigEntry config = registry.get("spark.test.int.config");
    assertNotNull(config);
    assertEquals("spark.test.int.config", config.getKey());
    assertEquals(ValueType.INT, config.getValueType());
    assertEquals("42", config.getDefaultValue());
    assertEquals(Scope.CLUSTER, config.getScope());
    assertEquals(Visibility.INTERNAL, config.getVisibility());
    assertEquals("A test integer config", config.getDoc());
    assertEquals("4.0.0", config.getVersion());
  }

  @Test
  public void testStringConfig() {
    ConfigEntry config = registry.get("spark.test.string.config");
    assertNotNull(config);
    assertEquals("spark.test.string.config", config.getKey());
    assertEquals(ValueType.STRING, config.getValueType());
    assertEquals("default_value", config.getDefaultValue());
    assertEquals(Scope.SESSION, config.getScope());
    assertEquals(Visibility.PUBLIC, config.getVisibility());
    assertEquals("A test string config", config.getDoc());
    assertEquals("4.0.0", config.getVersion());
  }

  @Test
  public void testGetNonExistent() {
    assertNull(registry.get("spark.nonexistent.config"));
    assertFalse(registry.contains("spark.nonexistent.config"));
  }

  @Test
  public void testAll() {
    assertEquals(4, registry.all().size());
  }

  @Test
  public void testMultiLineDoc() {
    ConfigEntry config = registry.get("spark.test.long.doc.config");
    assertNotNull(config);
    assertEquals("spark.test.long.doc.config", config.getKey());
    assertEquals(ValueType.STRING, config.getValueType());
    assertEquals("test", config.getDefaultValue());
    assertEquals(Scope.SESSION, config.getScope());
    assertEquals(Visibility.PUBLIC, config.getVisibility());
    // Verify multi-line string concatenation works
    String expectedDoc = "This is a very long documentation string that spans multiple lines. " +
        "It demonstrates the prototext multi-line string concatenation feature. " +
        "Each quoted segment will be concatenated together into a single string, " +
        "which is useful for configs with lengthy descriptions.";
    assertEquals(expectedDoc, config.getDoc());
    assertEquals("4.0.0", config.getVersion());
  }

  // ==========================================================================
  // Validation of production config files
  // ==========================================================================

  @Test
  public void testProductionConfigsAreValid() {
    validateAllConfigFiles(ConfigRegistry.DEFAULT_CONFIG_FILES);
  }

  // ==========================================================================
  // Tests for validation logic using invalid config files
  // ==========================================================================

  @Test
  public void testValidationMissingKey() {
    String file = "org/apache/spark/config/invalid_missing_key.textproto";
    Throwable error = assertThrows(Throwable.class,
        () -> validateConfigsInOneFile(file));
    assertTrue(error.getMessage().contains("Missing required fields"));
    assertTrue(error.getMessage().contains("key"));
    assertTrue(error.getMessage().contains(file));
  }

  @Test
  public void testValidationMissingValueType() {
    String file = "org/apache/spark/config/invalid_missing_value_type.textproto";
    Throwable error = assertThrows(Throwable.class,
        () -> validateConfigsInOneFile(file));
    assertTrue(error.getMessage().contains("Missing required fields"));
    assertTrue(error.getMessage().contains("value_type"));
    assertTrue(error.getMessage().contains("spark.test.missing.value.type"));
    assertTrue(error.getMessage().contains(file));
  }

  @Test
  public void testValidationMissingScope() {
    String file = "org/apache/spark/config/invalid_missing_scope.textproto";
    Throwable error = assertThrows(Throwable.class,
        () -> validateConfigsInOneFile(file));
    assertTrue(error.getMessage().contains("Missing required fields"));
    assertTrue(error.getMessage().contains("scope"));
    assertTrue(error.getMessage().contains("spark.test.missing.scope"));
    assertTrue(error.getMessage().contains(file));
  }

  @Test
  public void testValidationMissingVisibility() {
    String file = "org/apache/spark/config/invalid_missing_visibility.textproto";
    Throwable error = assertThrows(Throwable.class,
        () -> validateConfigsInOneFile(file));
    assertTrue(error.getMessage().contains("Missing required fields"));
    assertTrue(error.getMessage().contains("visibility"));
    assertTrue(error.getMessage().contains("spark.test.missing.visibility"));
    assertTrue(error.getMessage().contains(file));
  }

  @Test
  public void testValidationMissingDoc() {
    String file = "org/apache/spark/config/invalid_missing_doc.textproto";
    Throwable error = assertThrows(Throwable.class,
        () -> validateConfigsInOneFile(file));
    assertTrue(error.getMessage().contains("Missing required fields"));
    assertTrue(error.getMessage().contains("doc"));
    assertTrue(error.getMessage().contains("spark.test.missing.doc"));
    assertTrue(error.getMessage().contains(file));
  }

  @Test
  public void testValidationUnorderedConfigs() {
    String file = "org/apache/spark/config/invalid_unordered.textproto";
    Throwable error = assertThrows(Throwable.class,
        () -> validateConfigsInOneFile(file));
    assertTrue(error.getMessage().contains("must be ordered alphabetically"));
    assertTrue(error.getMessage().contains(file));
  }

  @Test
  public void testValidationDuplicateKeys() {
    String file1 = TEST_CONFIG_FILE;
    String file2 = "org/apache/spark/config/duplicate_key_file2.textproto";
    Throwable error = assertThrows(Throwable.class,
        () -> validateAllConfigFiles(new String[]{file1, file2}));
    assertTrue(error.getMessage().contains("Duplicate config key"));
    assertTrue(error.getMessage().contains("spark.test.bool.config"));
    assertTrue(error.getMessage().contains(file1));
    assertTrue(error.getMessage().contains(file2));
  }
}
