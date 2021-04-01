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
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidServerIdException;
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException;
import org.apache.spark.remoteshuffle.messages.ConnectUploadResponse;
import org.apache.spark.remoteshuffle.util.ServerHostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/***
 * This write client will retry if the given server is not valid.
 */
public class ServerIdAwareSyncWriteClient implements SingleServerWriteClient {
  private static final Logger logger =
      LoggerFactory.getLogger(ServerIdAwareSyncWriteClient.class);

  private final ServerDetail serverDetail;
  private final int timeoutMillis;
  private final boolean finishUploadAck;
  private final boolean usePooledConnection;
  private final String user;
  private final String appId;
  private final String appAttempt;
  private final ShuffleWriteConfig shuffleWriteConfig;

  private SingleServerWriteClient writeClient;

  public ServerIdAwareSyncWriteClient(ServerDetail serverDetail, int timeoutMillis,
                                      boolean finishUploadAck, boolean usePooledConnection,
                                      String user, String appId, String appAttempt,
                                      ShuffleWriteConfig shuffleWriteConfig) {
    this.serverDetail = serverDetail;
    this.timeoutMillis = timeoutMillis;
    this.finishUploadAck = finishUploadAck;
    this.user = user;
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.shuffleWriteConfig = shuffleWriteConfig;
    this.usePooledConnection = usePooledConnection;
  }

  @Override
  public ConnectUploadResponse connect() {
    return connectImpl(serverDetail, finishUploadAck);
  }

  @Override
  public void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {
    writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
  }

  // key/value could be null
  @Override
  public void writeDataBlock(int partition, ByteBuffer value) {
    writeClient.writeDataBlock(partition, value);
  }

  @Override
  public void finishUpload() {
    writeClient.finishUpload();
  }

  @Override
  public long getShuffleWriteBytes() {
    return writeClient.getShuffleWriteBytes();
  }

  @Override
  public void close() {
    closeUnderlyingClient();
  }

  @Override
  public String toString() {
    return "ServerIdAwareSyncWriteClient{" +
        "serverDetail=" + serverDetail +
        '}';
  }

  private ConnectUploadResponse connectImpl(ServerDetail serverDetail, boolean finishUploadAck) {
    ServerHostAndPort hostAndPort =
        ServerHostAndPort.fromString(serverDetail.getConnectionString());

    ConnectUploadResponse uploadServerVerboseInfo;

    try {
      if (!usePooledConnection) {
        writeClient = UnpooledWriteClientFactory.getInstance().getOrCreateClient(
            hostAndPort.getHost(),
            hostAndPort.getPort(),
            timeoutMillis,
            finishUploadAck,
            user,
            appId,
            appAttempt,
            shuffleWriteConfig);
      } else {
        writeClient = PooledWriteClientFactory.getInstance().getOrCreateClient(
            hostAndPort.getHost(),
            hostAndPort.getPort(),
            timeoutMillis,
            finishUploadAck,
            user,
            appId,
            appAttempt,
            shuffleWriteConfig);
      }

      uploadServerVerboseInfo = writeClient.connect();
    } catch (RssNetworkException ex) {
      closeUnderlyingClient();
      throw ex;
    } catch (Throwable ex) {
      close();
      throw ex;
    }

    if (!uploadServerVerboseInfo.getServerId().equals(serverDetail.getServerId())) {
      close();
      String msg = String
          .format("Server id (%s) is not expected (%s)", uploadServerVerboseInfo.getServerId(),
              serverDetail);
      throw new RssInvalidServerIdException(msg);
    }

    return uploadServerVerboseInfo;
  }

  private void closeUnderlyingClient() {
    if (writeClient != null) {
      try {
        writeClient.close();
      } catch (Throwable ex) {
        logger.warn(String.format("Failed to close underlying client %s", writeClient), ex);
      }
      writeClient = null;
    }
  }
}
