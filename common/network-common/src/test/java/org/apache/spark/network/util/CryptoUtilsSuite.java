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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class CryptoUtilsSuite {

  @Test
  public void testConfConversion() {
    String prefix = "my.prefix.commons.config.";

    String confKey1 = prefix + "a.b.c";
    String confVal1 = "val1";
    String cryptoKey1 = CryptoUtils.COMMONS_CRYPTO_CONFIG_PREFIX + "a.b.c";

    String confKey2 = prefix.substring(0, prefix.length() - 1) + "A.b.c";
    String confVal2 = "val2";
    String cryptoKey2 = CryptoUtils.COMMONS_CRYPTO_CONFIG_PREFIX + "A.b.c";

    Map<String, String> conf = ImmutableMap.of(
      confKey1, confVal1,
      confKey2, confVal2);

    Properties cryptoConf = CryptoUtils.toCryptoConf(prefix, conf.entrySet());

    assertEquals(confVal1, cryptoConf.getProperty(cryptoKey1));
    assertFalse(cryptoConf.containsKey(cryptoKey2));
  }

}
