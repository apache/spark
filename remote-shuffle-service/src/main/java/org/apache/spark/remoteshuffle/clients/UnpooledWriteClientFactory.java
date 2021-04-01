/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.clients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates unpooled write client.
 */
public class UnpooledWriteClientFactory implements WriteClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(UnpooledWriteClientFactory.class);

  private final static UnpooledWriteClientFactory instance = new UnpooledWriteClientFactory();

  public static UnpooledWriteClientFactory getInstance() {
    return instance;
  }

  @Override
  public ShuffleDataSyncWriteClient getOrCreateClient(String host, int port, int timeoutMillis,
                                                      boolean finishUploadAck, String user,
                                                      String appId, String appAttempt,
                                                      ShuffleWriteConfig shuffleWriteConfig) {
    final ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient(
        host,
        port,
        timeoutMillis,
        finishUploadAck,
        user,
        appId,
        appAttempt,
        shuffleWriteConfig);
    return writeClient;
  }
}
