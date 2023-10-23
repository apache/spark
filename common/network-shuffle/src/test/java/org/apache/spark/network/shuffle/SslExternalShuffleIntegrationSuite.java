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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.junit.jupiter.api.BeforeAll;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.ssl.SslSampleConfigs;

public class SslExternalShuffleIntegrationSuite extends ExternalShuffleIntegrationSuite {

  private static TransportConf createTransportConf(String maxRetries, String rddEnabled) {
    HashMap<String, String> config = new HashMap<>();
    config.put("spark.shuffle.io.maxRetries", maxRetries);
    config.put(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, rddEnabled);
    return new TransportConf(
      "shuffle",
      SslSampleConfigs.createDefaultConfigProviderForRpcNamespaceWithAdditionalEntries(config)
    );
  }

  @Override
  protected TransportConf createTransportConfForFetchNoServerTest() {
    return createTransportConf("0", "false");
  }

  @BeforeAll
  public static void beforeAll() throws IOException {
    Random rand = new Random();

    for (byte[] block : exec0Blocks) {
      rand.nextBytes(block);
    }
    for (byte[] block: exec1Blocks) {
      rand.nextBytes(block);
    }
    rand.nextBytes(exec0RddBlockValid);
    rand.nextBytes(exec0RddBlockToRemove);

    dataContext0 = new TestShuffleDataContext(2, 5);
    dataContext0.create();
    dataContext0.insertSortShuffleData(0, 0, exec0Blocks);
    dataContext0.insertCachedRddData(RDD_ID, SPLIT_INDEX_VALID_BLOCK, exec0RddBlockValid);
    dataContext0.insertCachedRddData(RDD_ID, SPLIT_INDEX_VALID_BLOCK_TO_RM, exec0RddBlockToRemove);

    conf = createTransportConf("0", "true");
    handler = new ExternalBlockHandler(
      new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, null) {
        @Override
        public ManagedBuffer getRddBlockData(String appId, String execId, int rddId, int splitIdx) {
          ManagedBuffer res;
          if (rddId == RDD_ID) {
            switch (splitIdx) {
              case SPLIT_INDEX_CORRUPT_LENGTH:
                res = new FileSegmentManagedBuffer(
                  conf, new File("missing.file"), 0, 12);
                break;
              default:
                res = super.getRddBlockData(appId, execId, rddId, splitIdx);
            }
          } else {
            res = super.getRddBlockData(appId, execId, rddId, splitIdx);
          }
          return res;
        }
    });
    transportContext = new TransportContext(conf, handler);
    server = transportContext.createServer();
  }
}
