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
import org.apache.spark.network.remoteshuffle.protocol.FinishTaskRequest;
import org.apache.spark.network.remoteshuffle.protocol.FinishTaskResponse;
import org.apache.spark.network.remoteshuffle.protocol.RemoteShuffleMessage;
import org.apache.spark.network.remoteshuffle.protocol.StreamRecord;
import org.apache.spark.network.remoteshuffle.protocol.TaskAttemptRecord;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/***
 * Remote shuffle server handler.
 */
public class ShuffleServerHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleServerHandler.class);

  private final ShuffleEngine shuffleEngine;

  public ShuffleServerHandler(String rootDir) {
    this.shuffleEngine = new ShuffleEngine(rootDir);
  }

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
      long sessionId = shuffleEngine.createWriteSession(new ShuffleStageFqid(
          msg.appId,
          msg.execId,
          msg.shuffleId,
          msg.stageAttempt
      ));
      callback.onSuccess(new ConnectWriteResponse(sessionId).toByteBuffer());
    } else if (msgObj instanceof StreamRecord) {
      StreamRecord record = (StreamRecord)msgObj;
      long taskAttempt = TaskAttemptRecord.getTaskAttemptId(record.taskData);
      shuffleEngine.writeTaskData(record.sessionId, record.partition, taskAttempt, record.taskData);
    } else if (msgObj instanceof FinishTaskRequest) {
      FinishTaskRequest msg = (FinishTaskRequest)msgObj;
      if (logger.isTraceEnabled()) {
        logger.trace("Finish task for session {} for client {} from host {}",
            msg.sessionId,
            client.getClientId(),
            getRemoteAddress(client.getChannel()));
      }
      shuffleEngine.finishTask(msg.sessionId, msg.taskAttempt);
      callback.onSuccess(new FinishTaskResponse((byte)0).toByteBuffer());
    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
  }
}
