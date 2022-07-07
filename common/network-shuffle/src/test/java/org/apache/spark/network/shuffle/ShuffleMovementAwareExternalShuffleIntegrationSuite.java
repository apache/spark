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
import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class ShuffleMovementAwareExternalShuffleIntegrationSuite {
    private static final String APP_ID = "app-id";
    private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";
    private static final int RDD_ID = 1;
    private static final int SPLIT_INDEX_VALID_BLOCK = 0;
    private static final int SPLIT_INDEX_CORRUPT_LENGTH = 2;
    private static final int SPLIT_INDEX_VALID_BLOCK_TO_RM = 3;
    // Executor 0 is sort-based
    static TestShuffleDataContext dataContext0;
    static ExternalBlockHandler handler;
    static TransportServer server;
    static TransportConf conf;
    static TransportContext transportContext;
    static byte[] exec0RddBlockValid = new byte[123];
    static byte[] exec0RddBlockToRemove = new byte[124];
    static byte[][] exec0Blocks = new byte[][] {
            new byte[123],
            new byte[12345],
            new byte[1234567],
    };
    static byte[][] exec1Blocks = new byte[][] {
            new byte[321],
            new byte[54321],
    };
    @BeforeClass
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
        HashMap<String, String> config = new HashMap<>();
        config.put("spark.shuffle.io.maxRetries", "0");
        config.put(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, "true");
        conf = new TransportConf("shuffle", new MapConfigProvider(config));
        handler = new ExternalBlockHandler(
                new OneForOneStreamManager(),
                new ShuffleMovementAwareExternalShuffleBlockResolver(conf, null) {
                    @Override
                    public ManagedBuffer getRddBlockData(String appId, String execId, int rddId, int splitIdx) {
                        ManagedBuffer res;
                        if (rddId == RDD_ID) {
                            switch (splitIdx) {
                                case SPLIT_INDEX_CORRUPT_LENGTH:
                                    res = new FileSegmentManagedBuffer(conf, new File("missing.file"), 0, 12);
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
    @AfterClass
    public static void afterAll() {
        dataContext0.cleanup();
        server.close();
        transportContext.close();
    }
    @After
    public void afterEach() {
        handler.applicationRemoved(APP_ID, false /* cleanupLocalDirs */);
    }
    @Test
    public void testShuffleOffloadingCompletion() throws Exception {
        ExternalBlockStoreClient client = new ShuffleMovementAwareExternalBlockStoreClient(conf, null, false, 5000);
        client.init(APP_ID);
        client.registerWithShuffleServer(TestUtils.getLocalHost(), server.getPort(),
                "exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
        assert(!((ShuffleMovementAwareExternalShuffleBlockResolver)handler.blockManager).getCompletionState());
        client.executorDecommissioned(TestUtils.getLocalHost(), server.getPort(),
                "exec-0", 1000);
        assert(((ShuffleMovementAwareExternalShuffleBlockResolver)handler.blockManager).getCompletionState());
    }
}

