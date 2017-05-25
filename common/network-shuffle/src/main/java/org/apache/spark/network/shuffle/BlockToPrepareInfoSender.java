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

import java.lang.Override;
import java.lang.String;
import java.lang.Throwable;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.network.client.PrepareRequestReceivedCallBack;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.PrepareBlocks;


public class BlockToPrepareInfoSender {
  private final Logger logger = LoggerFactory.getLogger(BlockToPrepareInfoSender.class);

  private final TransportClient client;
  private final PrepareBlocks prepareMessage;
  private final String[] blockIds;
  private final String[] blocksToRelease;
  private final BlockPreparingListener listener;
  private final PrepareRequestReceivedCallBack requestReceivedCallBack;

  public BlockToPrepareInfoSender(
      TransportClient client,
      String appId,
      String execId,
      String[] blockIds,
      String[] blocksToRelease,
      BlockPreparingListener listener) {
    this.client = client;
    this.prepareMessage = new PrepareBlocks(appId, execId, blockIds, blocksToRelease);
    this.blockIds = blockIds;
    this.blocksToRelease = blocksToRelease;
    this.listener = listener;
    this.requestReceivedCallBack = new PrepareCallBack();
  }

  private class PrepareCallBack implements  PrepareRequestReceivedCallBack {
    @Override
    public void onSuccess() {
      listener.onBlockPrepareSuccess();
    }

    @Override
    public void onFailure(Throwable e) {
      listener.onBlockPrepareFailure(e);
    }
  }

  public void start() {
    if (blockIds.length == 0) {
//    throw new IllegalArgumentException("Zero-size blockIds array");
      logger.warn("Zero-size blockIds array");
    }

    client.sendRpc(prepareMessage.toByteBuffer(), new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        logger.debug("Successfully send prepare block's info, ready for the next step");
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while send the prepare message");
       }
    });
  }
}
