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

package org.apache.spark.network.util;

import java.util.Map;
import java.util.Properties;

/**
 * Utility methods related to the commons-crypto library.
 */
public class CryptoUtils {

  // The prefix for the configurations passing to Apache Commons Crypto library.
  public static final String COMMONS_CRYPTO_CONFIG_PREFIX = "commons.crypto.";

  /**
   * Extract the commons-crypto configuration embedded in a list of config values.
   *
   * @param prefix Prefix in the given configuration that identifies the commons-crypto configs.
   * @param conf List of configuration values.
   */
  public static Properties toCryptoConf(String prefix, Iterable<Map.Entry<String, String>> conf) {
    Properties props = new Properties();
    for (Map.Entry<String, String> e : conf) {
      String key = e.getKey();
      if (key.startsWith(prefix)) {
        props.setProperty(COMMONS_CRYPTO_CONFIG_PREFIX + key.substring(prefix.length()),
          e.getValue());
      }
    }
    return props;
  }

}
