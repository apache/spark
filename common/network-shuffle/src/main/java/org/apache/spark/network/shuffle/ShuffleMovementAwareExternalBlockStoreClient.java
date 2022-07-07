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
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;
import java.io.IOException;
/**
 * Shuffle client that allows marking of executor shuffle data as offloaded.
 */
public class ShuffleMovementAwareExternalBlockStoreClient extends ExternalBlockStoreClient {
    public ShuffleMovementAwareExternalBlockStoreClient(
            TransportConf conf,
            SecretKeyHolder secretKeyHolder,
            boolean authEnabled,
            long registrationTimeoutMs) {
        super(conf, secretKeyHolder, authEnabled, registrationTimeoutMs);
    }
    /**
     * Notify shuffle server that the executor has offloaded it's shuffle data.
     *
     * @param host Host of shuffle server.
     * @param port Port of shuffle server.
     * @param execId Id for executor which offloaded shuffle
     */
    @Override
    public void executorDecommissioned(String host, int port, String execId, long timeout)
            throws InterruptedException, IOException {
        checkInit();
        logger.info("Marking shuffle as decommissioned for executor {} on {}:{},",
                execId, host, port);
        TransportClient client = clientFactory.createClient(host, port);
        try {
            client.sendRpcSync(new ExecutorDecommissioned(appId, execId).toByteBuffer(),
                    timeout);
        } catch (Exception e) {
            logger.error("Error in marking shuffle as decommissioned for executor {} on {}:{},",
                    execId, host, port);
        }
    }
}