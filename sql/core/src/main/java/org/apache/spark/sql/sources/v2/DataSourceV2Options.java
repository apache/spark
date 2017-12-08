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

package org.apache.spark.sql.sources.v2;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.annotation.InterfaceStability;

/**
 * An immutable string-to-string map in which keys are case-insensitive. This is used to represent
 * data source options.
 */
@InterfaceStability.Evolving
public class DataSourceV2Options {
  private final Map<String, String> keyLowerCasedMap;

  private String toLowerCase(String key) {
    return key.toLowerCase(Locale.ROOT);
  }

  public DataSourceV2Options(Map<String, String> originalMap) {
    keyLowerCasedMap = new HashMap<>(originalMap.size());
    for (Map.Entry<String, String> entry : originalMap.entrySet()) {
      keyLowerCasedMap.put(toLowerCase(entry.getKey()), entry.getValue());
    }
  }

  /**
   * Returns the option value to which the specified key is mapped, case-insensitively.
   */
  public Optional<String> get(String key) {
    return Optional.ofNullable(keyLowerCasedMap.get(toLowerCase(key)));
  }

  /**
   * Returns the boolean value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Boolean.parseBoolean(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }

  /**
   * Returns the integer value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public int getInt(String key, int defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Integer.parseInt(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }

  /**
   * Returns the long value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public long getLong(String key, long defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Long.parseLong(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }

  /**
   * Returns the double value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public double getDouble(String key, double defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Double.parseDouble(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }
}
