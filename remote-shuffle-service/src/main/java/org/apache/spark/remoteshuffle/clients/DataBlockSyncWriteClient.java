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

import com.uber.m3.tally.Stopwatch;
import org.apache.spark.remoteshuffle.common.ShuffleMapTaskAttemptId;
import org.apache.spark.remoteshuffle.exceptions.RssFinishUploadException;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.metrics.WriteClientMetrics;
import org.apache.spark.remoteshuffle.metrics.WriteClientMetricsKey;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import org.apache.spark.remoteshuffle.util.ExceptionUtils;
import io.netty.buffer.ByteBuf;
import org.apache.spark.remoteshuffle.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/***
 * Shuffle write client to upload data (data blocks) to shuffle server.
 */
public class DataBlockSyncWriteClient extends ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DataBlockSyncWriteClient.class);

  private final boolean finishUploadAck;

  private final String user;
  private final String appId;
  private final String appAttempt;

  private long totalWriteBytes = 0;
  private long startUploadShuffleByteSnapshot = 0;

  private WriteClientMetrics metrics = null;

  public DataBlockSyncWriteClient(String host, int port, int timeoutMillis, String user,
                                  String appId, String appAttempt) {
    this(host, port, timeoutMillis, true, user, appId, appAttempt);
  }

  public DataBlockSyncWriteClient(String host, int port, int timeoutMillis,
                                  boolean finishUploadAck, String user, String appId,
                                  String appAttempt) {
    super(host, port, timeoutMillis);
    this.finishUploadAck = finishUploadAck;
    this.user = user;
    this.appId = appId;
    this.appAttempt = appAttempt;

    this.metrics = new WriteClientMetrics(new WriteClientMetricsKey(
        this.getClass().getSimpleName(), user));
    metrics.getNumClients().inc(1);
  }

  public ConnectUploadResponse connect() {
    Stopwatch stopwatch = metrics.getWriteConnectLatency().start();
    try {
      return connectImpl();
    } finally {
      stopwatch.stop();
    }
  }

  private ConnectUploadResponse connectImpl() {
    if (socket != null) {
      throw new RssInvalidStateException(
          String.format("Already connected to server, cannot connect again: %s", connectionInfo));
    }

    ConnectUploadRequest connectUploadRequest = new ConnectUploadRequest(user, appId, appAttempt);

    logger.debug(String.format("Connecting to server: %s", connectionInfo));

    connectSocket();

    write(MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE);
    write(MessageConstants.UPLOAD_UPLINK_VERSION_3);

    writeControlMessageAndWaitResponseStatus(connectUploadRequest);

    ConnectUploadResponse connectUploadResponse =
        readResponseMessage(MessageConstants.MESSAGE_ConnectUploadResponse,
            ConnectUploadResponse::deserialize);

    logger.info(String
        .format("Connected to server: %s, response: %s", connectionInfo, connectUploadResponse));
    return connectUploadResponse;
  }

  // TODO do not need mapId/taskAttamptId for StartUploadMessage
  public void startUpload(ShuffleMapTaskAttemptId shuffleMapTaskAttemptId, int numMaps,
                          int numPartitions, ShuffleWriteConfig shuffleWriteConfig) {
    logger.debug(String.format("Starting upload %s, %s", shuffleMapTaskAttemptId, connectionInfo));

    startUploadShuffleByteSnapshot = totalWriteBytes;

    StartUploadMessage startUploadMessage = new StartUploadMessage(
        shuffleMapTaskAttemptId.getShuffleId(),
        shuffleMapTaskAttemptId.getMapId(),
        shuffleMapTaskAttemptId.getTaskAttemptId(),
        numMaps,
        numPartitions,
        "",
        shuffleWriteConfig.getNumSplits());

    writeControlMessageNotWaitResponseStatus(startUploadMessage);
  }

  public void writeData(int partitionId, long taskAttemptId, ByteBuf data) {
    final int headerByteCount = Integer.BYTES + Long.BYTES + Integer.BYTES;
    final int dataByteCount = data.readableBytes();

    byte[] headerBytes = new byte[headerByteCount];
    ByteBufUtils.writeInt(headerBytes, 0, partitionId);
    ByteBufUtils.writeLong(headerBytes, Integer.BYTES, taskAttemptId);
    ByteBufUtils.writeInt(headerBytes, Integer.BYTES + Long.BYTES, dataByteCount);

    try {
      outputStream.write(headerBytes);
    } catch (IOException e) {
      throw new RssNetworkException(String.format(
          "writeRowGroup: hit exception writing heading bytes %s, %s, %s",
          partitionId, connectionInfo, ExceptionUtils.getSimpleMessage(e)), e);
    }

    try {
      ByteBufUtils.readBytesToStream(data, outputStream);
    } catch (IOException e) {
      throw new RssNetworkException(String.format(
          "writeRowGroup: hit exception writing data %s, %s, %s",
          partitionId, connectionInfo, ExceptionUtils.getSimpleMessage(e)), e);
    }

    long bytesDelta = headerByteCount + dataByteCount;
    totalWriteBytes += bytesDelta;
    metrics.getNumWriteBytes().inc(bytesDelta);
  }

  public void finishUpload(long taskAttemptId) {
    Stopwatch stopwatch = metrics.getFinishUploadLatency().start();
    try {
      byte ackFlag = finishUploadAck ? FinishUploadMessage.ACK_FLAG_HAS_ACK :
          FinishUploadMessage.ACK_FLAG_NO_ACK;
      FinishUploadMessage message =
          new FinishUploadMessage(taskAttemptId, System.currentTimeMillis(), ackFlag);
      if (ackFlag == FinishUploadMessage.ACK_FLAG_NO_ACK) {
        writeControlMessageNotWaitResponseStatus(message);
      } else {
        writeControlMessageAndWaitResponseStatus(message);
      }
    } catch (Throwable e) {
      String msg = String.format(
          "Failed to finish upload to server %s, %s, %s. If the network is good, this error " +
              "may indicate your shuffle data exceeds the server side limit. This shuffle " +
              "client has written %s bytes.",
          taskAttemptId, connectionInfo, ExceptionUtils.getSimpleMessage(e),
          getShuffleWriteBytes());
      throw new RssFinishUploadException(msg, e);
    } finally {
      stopwatch.stop();
    }
  }

  @Override
  public void close() {
    super.close();
    closeMetrics();
  }

  public long getShuffleWriteBytes() {
    return totalWriteBytes - startUploadShuffleByteSnapshot;
  }

  private void closeMetrics() {
    try {
      if (metrics != null) {
        metrics.close();
        metrics = null;
      }
    } catch (Throwable e) {
      M3Stats.addException(e, this.getClass().getSimpleName());
      logger.warn(String.format("Failed to close metrics: %s", connectionInfo), e);
    }
  }
}
