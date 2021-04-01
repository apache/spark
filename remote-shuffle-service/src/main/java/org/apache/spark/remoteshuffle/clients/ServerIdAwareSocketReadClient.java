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

import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.DownloadServerVerboseInfo;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidServerIdException;
import org.apache.spark.remoteshuffle.util.ServerHostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/***
 * This client will check server id/version to make sure connecting to the correct server.
 */
public class ServerIdAwareSocketReadClient implements SingleServerReadClient {
  private static final Logger logger =
      LoggerFactory.getLogger(ServerIdAwareSocketReadClient.class);

  private final ServerDetail serverDetail;
  private SingleServerReadClient readClient;

  public ServerIdAwareSocketReadClient(ServerDetail serverDetail, int timeoutMillis, String user,
                                       AppShufflePartitionId appShufflePartitionId,
                                       Collection<Long> fetchTaskAttemptIds,
                                       long dataAvailablePollInterval,
                                       long dataAvailableWaitTime) {
    this.serverDetail = serverDetail;

    ServerHostAndPort hostAndPort =
        ServerHostAndPort.fromString(serverDetail.getConnectionString());
    String host = hostAndPort.getHost();
    int port = hostAndPort.getPort();

    SingleServerReadClient client;
    client = new PlainShuffleDataSocketReadClient(host, port, timeoutMillis, user,
        appShufflePartitionId, fetchTaskAttemptIds, dataAvailablePollInterval,
        dataAvailableWaitTime);
    this.readClient = client;
  }

  @Override
  public DownloadServerVerboseInfo connect() {
    DownloadServerVerboseInfo serverVerboseInfo;

    try {
      serverVerboseInfo = readClient.connect();
    } catch (Throwable ex) {
      close();
      throw ex;
    }

    if (!serverVerboseInfo.getId().equals(serverDetail.getServerId())) {
      close();
      String msg = String
          .format("Server id (%s) is not expected (%s)", serverVerboseInfo.getId(), serverDetail);
      throw new RssInvalidServerIdException(msg);
    }

    return serverVerboseInfo;
  }

  @Override
  public void close() {
    closeUnderlyingClient();
  }

  @Override
  public TaskDataBlock readDataBlock() {
    return readClient.readDataBlock();
  }

  @Override
  public long getShuffleReadBytes() {
    return readClient.getShuffleReadBytes();
  }

  @Override
  public String toString() {
    return "ServerIdAwareSocketReadClient{" +
        "serverDetail=" + serverDetail +
        ", readClient=" + readClient +
        '}';
  }

  private void closeUnderlyingClient() {
    if (readClient != null) {
      try {
        readClient.close();
      } catch (Throwable ex) {
        logger.warn(String.format("Failed to close underlying client %s", readClient), ex);
      }
      readClient = null;
    }
  }
}
