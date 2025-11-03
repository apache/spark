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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeAll;

import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.ssl.SslSampleConfigs;

public class SslExternalShuffleIntegrationSuite extends ExternalShuffleIntegrationSuite {

  private static TransportConf createTransportConf(int maxRetries, boolean rddEnabled) {
    HashMap<String, String> config = new HashMap<>();
    config.put("spark.shuffle.io.maxRetries", String.valueOf(maxRetries));
    config.put(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, String.valueOf(rddEnabled));
    return new TransportConf(
      "shuffle",
      SslSampleConfigs.createDefaultConfigProviderForRpcNamespaceWithAdditionalEntries(config)
    );
  }

  @BeforeAll
  public static void beforeAll() throws IOException {
    doBeforeAllWithConfig(createTransportConf(0, true));
  }
}
