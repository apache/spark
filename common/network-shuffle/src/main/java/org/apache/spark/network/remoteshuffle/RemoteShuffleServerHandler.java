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

package org.apache.spark.network.remoteshuffle;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.remoteshuffle.protocol.ConnectWriteRequest;
import org.apache.spark.network.remoteshuffle.protocol.ConnectWriteResponse;
import org.apache.spark.network.remoteshuffle.protocol.RemoteShuffleMessage;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/***
 * Remote shuffle server handler.
 */
public class RemoteShuffleServerHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(RemoteShuffleServerHandler.class);

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    RemoteShuffleMessage msgObj = RemoteShuffleMessage.Decoder.fromByteBuffer(message);
    handleMessage(msgObj, client, callback);
  }

  @Override
  public StreamManager getStreamManager() {
    return null;
  }

  protected void handleMessage(
      RemoteShuffleMessage msgObj,
      TransportClient client,
      RpcResponseCallback callback) {
    if (msgObj instanceof ConnectWriteRequest) {
      ConnectWriteRequest msg = (ConnectWriteRequest) msgObj;
      if (logger.isTraceEnabled()) {
        logger.trace("Connect write request for shuffle {} for client {} from host {}",
            msg.shuffleId,
            client.getClientId(),
            getRemoteAddress(client.getChannel()));
      }
      callback.onSuccess(new ConnectWriteResponse("TODO").toByteBuffer());
    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
  }
}
