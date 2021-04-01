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

import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.ShuffleMapTaskAttemptId;
import org.apache.spark.remoteshuffle.messages.ConnectUploadResponse;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/***
 * Shuffle write client to upload data (records) to shuffle server.
 */
public abstract class ShuffleDataSyncWriteClientBase implements ShuffleDataSyncWriteClient {
  private static final Logger logger =
      LoggerFactory.getLogger(ShuffleDataSyncWriteClientBase.class);

  private final String host;
  private final int port;
  private final String user;
  private final String appId;
  private final String appAttempt;

  protected final DataBlockSyncWriteClient dataBlockSyncWriteClient;
  protected final ShuffleWriteConfig shuffleWriteConfig;

  protected ShuffleMapTaskAttemptId shuffleMapTaskAttemptId;

  protected ShuffleDataSyncWriteClientBase(String host, int port, int timeoutMillis,
                                           boolean finishUploadAck, String user, String appId,
                                           String appAttempt,
                                           ShuffleWriteConfig shuffleWriteConfig) {
    this.dataBlockSyncWriteClient =
        new DataBlockSyncWriteClient(host, port, timeoutMillis, finishUploadAck, user, appId,
            appAttempt);
    this.shuffleWriteConfig = shuffleWriteConfig;

    this.host = host;
    this.port = port;
    this.user = user;
    this.appId = appId;
    this.appAttempt = appAttempt;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getAppId() {
    return appId;
  }

  public String getAppAttempt() {
    return appAttempt;
  }

  public ConnectUploadResponse connect() {
    return dataBlockSyncWriteClient.connect();
  }

  public void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {
    shuffleMapTaskAttemptId = appTaskAttemptId.getShuffleMapTaskAttemptId();
    dataBlockSyncWriteClient
        .startUpload(shuffleMapTaskAttemptId, numMaps, numPartitions, shuffleWriteConfig);
  }

  @Override
  abstract public void writeDataBlock(int partition, ByteBuffer value);

  @Override
  public void finishUpload() {
    dataBlockSyncWriteClient.finishUpload(shuffleMapTaskAttemptId.getTaskAttemptId());
  }

  @Override
  public long getShuffleWriteBytes() {
    return dataBlockSyncWriteClient.getShuffleWriteBytes();
  }

  @Override
  public void close() {
    dataBlockSyncWriteClient.close();
  }

  @Override
  public String toString() {
    return "RecordSyncWriteClientBase{" +
        "host='" + host + '\'' +
        ", port=" + port +
        ", user='" + user + '\'' +
        ", appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", dataBlockSyncWriteClient=" + dataBlockSyncWriteClient +
        ", shuffleWriteConfig=" + shuffleWriteConfig +
        '}';
  }

  protected int getRecordSerializedSize(ByteBuffer value) {
    int numValueBytes = value == null ? 0 : value.remaining();
    return numValueBytes;
  }

  protected void writeRecordToBuffer(ByteBuf buffer, ByteBuffer value) {
    if (value != null) {
      buffer.writeBytes(value);
    }
  }

}
