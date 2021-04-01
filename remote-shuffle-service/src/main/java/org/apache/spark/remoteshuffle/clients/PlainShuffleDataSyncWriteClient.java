/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/***
 * Shuffle write client to upload data to shuffle server.
 */
public class PlainShuffleDataSyncWriteClient extends ShuffleDataSyncWriteClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(PlainShuffleDataSyncWriteClient.class);

  public PlainShuffleDataSyncWriteClient(String host, int port, int timeoutMillis,
                                         boolean finishUploadAck, String user, String appId,
                                         String appAttempt,
                                         ShuffleWriteConfig shuffleWriteConfig) {
    super(host, port, timeoutMillis, finishUploadAck, user, appId, appAttempt, shuffleWriteConfig);
  }

  @Override
  public void writeDataBlock(int partition, ByteBuffer value) {
    int totalRecordBytes = getRecordSerializedSize(value);
    ByteBuf recordSerializedByteBuf = Unpooled.buffer(totalRecordBytes);
    writeRecordToBuffer(recordSerializedByteBuf, value);
    dataBlockSyncWriteClient
        .writeData(partition, shuffleMapTaskAttemptId.getTaskAttemptId(), recordSerializedByteBuf);
  }

  @Override
  public String toString() {
    return "PlainRecordSyncWriteClient{" +
        super.toString() +
        '}';
  }
}
