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
import org.apache.spark.remoteshuffle.common.*;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.metrics.ReadClientMetrics;
import org.apache.spark.remoteshuffle.metrics.ReadClientMetricsKey;
import org.apache.spark.remoteshuffle.exceptions.*;
import org.apache.spark.remoteshuffle.messages.*;
import org.apache.spark.remoteshuffle.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/***
 * Shuffle read client to download data (data blocks) from shuffle server.
 */
public class DataBlockSocketReadClient extends ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DataBlockSocketReadClient.class);

  private static final int POLL_INTERVAL_MAX_MULTIPLIER = 100;

  private final String user;
  private final AppShufflePartitionId appShufflePartitionId;
  private final Set<Long> fetchTaskAttemptIds;
  private final long dataAvailablePollInterval;
  private final long dataAvailableWaitTime;

  private ReadClientMetrics metrics;

  private String fileCompressionCodec;
  private MapTaskCommitStatus commitMapTaskCommitStatus;
  private Set<Long> commitTaskAttemptIds;

  private boolean downloadStarted = false;
  private long dataLength = -1;

  private int totalReadDataBlocks = 0;
  private FixedLengthInputStream fixedLengthInputStream;

  public DataBlockSocketReadClient(String host, int port, int timeoutMillis, String user,
                                   AppShufflePartitionId appShufflePartitionId,
                                   Collection<Long> fetchTaskAttemptIds,
                                   long dataAvailablePollInterval, long dataAvailableWaitTime) {
    super(host, port, timeoutMillis);
    this.user = user;
    this.appShufflePartitionId = appShufflePartitionId;
    this.fetchTaskAttemptIds = new HashSet<>(fetchTaskAttemptIds);
    this.dataAvailablePollInterval = dataAvailablePollInterval;
    this.dataAvailableWaitTime = dataAvailableWaitTime;

    this.metrics = new ReadClientMetrics(
        new ReadClientMetricsKey(this.getClass().getSimpleName(), user));
  }

  public ConnectDownloadResponse connect() {
    Stopwatch stopwatch = metrics.getReadConnectLatency().start();
    try {
      return connectImpl();
    } finally {
      stopwatch.stop();
    }
  }

  private ConnectDownloadResponse connectImpl() {
    if (socket != null) {
      throw new RssInvalidStateException(String.format(
          "Already connected to server, cannot connect again: %s", connectionInfo));
    }

    logger.debug("Connecting to server: {}", connectionInfo);

    connectSocket();

    write(MessageConstants.DOWNLOAD_UPLINK_MAGIC_BYTE);
    write(MessageConstants.DOWNLOAD_UPLINK_VERSION_3);

    ConnectDownloadRequest connectRequest = new ConnectDownloadRequest(user,
        appShufflePartitionId, fetchTaskAttemptIds);


    ExceptionWrapper<RssException> exceptionWrapper = new ExceptionWrapper<>();

    Boolean succeeded = RetryUtils.retryUntilNotNull(dataAvailablePollInterval,
        dataAvailablePollInterval * POLL_INTERVAL_MAX_MULTIPLIER,
        dataAvailableWaitTime, () -> {
          try {
            writeControlMessageAndWaitResponseStatus(connectRequest);
            return Boolean.TRUE;
          } catch (RssShuffleCorruptedException ex) {
            throw new RssShuffleCorruptedException(
                "Shuffle data corrupted for: " + appShufflePartitionId, ex);
          } catch (RssMissingShuffleWriteConfigException | RssShuffleStageNotStartedException ex) {
            exceptionWrapper.setException(ex);
            logger.warn(String.format(
                "Did not find data in server side, server may not run fast enough to get data from" +
                    " client or server hits some issue, %s",
                appShufflePartitionId), ex);
            return null;
          }
        });

    if (succeeded == null || !succeeded.booleanValue()) {
      if (exceptionWrapper.getException() != null) {
        throw exceptionWrapper.getException();
      } else {
        throw new RssInvalidStateException(String.format(
            "Failed to connect to server %s, %s", connectionInfo, appShufflePartitionId));
      }
    }

    ConnectDownloadResponse connectDownloadResponse = readResponseMessage(
        MessageConstants.MESSAGE_ConnectDownloadResponse, ConnectDownloadResponse::deserialize);

    logger.info("Connected to server: {}, response: {}", connectionInfo, connectDownloadResponse);

    fileCompressionCodec = connectDownloadResponse.getCompressionCodec();

    if (connectDownloadResponse.isDataAvailable()) {
      this.commitMapTaskCommitStatus = connectDownloadResponse.getMapTaskCommitStatus();
      if (this.commitMapTaskCommitStatus == null) {
        throw new RssInvalidDataException("MapTaskCommitStatus should not be null");
      }
      this.commitTaskAttemptIds = this.commitMapTaskCommitStatus.getTaskAttemptIds();
      if (!this.commitTaskAttemptIds.containsAll(fetchTaskAttemptIds)) {
        throw new RssInvalidDataException(String.format(
            "Task attempt ids not matched, committed: %s, fetching: %s",
            this.commitTaskAttemptIds,
            fetchTaskAttemptIds));
      }
    }

    return connectDownloadResponse;
  }

  public GetDataAvailabilityResponse waitDataAvailable() {
    if (this.commitMapTaskCommitStatus != null) {
      throw new RssInvalidStateException("Data already available, should not wait again");
    }

    long startTime = System.currentTimeMillis();
    logger.info("Waiting for all mappers finished: {}, {}", appShufflePartitionId, connectionInfo);


    Stopwatch reducerWaitTimeStopwatch = metrics.getReducerWaitTime().start();
    final ObjectWrapper<GetDataAvailabilityResponse> getDataAvailabilityRetryLastResult
        = new ObjectWrapper<>();
    try {
      RetryUtils.retryUntilNotNull(dataAvailablePollInterval,
          dataAvailablePollInterval * POLL_INTERVAL_MAX_MULTIPLIER, dataAvailableWaitTime, () -> {
            GetDataAvailabilityResponse getDataAvailabilityResponse = getDataAvailability();
            getDataAvailabilityRetryLastResult.setObject(getDataAvailabilityResponse);
            if (getDataAvailabilityResponse.isDataAvailable()) {
              return getDataAvailabilityResponse;
            } else {
              return null;
            }
          });
    } finally {
      reducerWaitTimeStopwatch.stop();
    }

    logger.info("Finished waiting for all mappers to finish, partition: {}, duration: {} seconds",
        appShufflePartitionId, (System.currentTimeMillis() - startTime) / 1000);

    GetDataAvailabilityResponse getDataAvailabilityRetryResult =
        getDataAvailabilityRetryLastResult.getObject();

    // Throw exception if not get the status which indicating all mappers are finished
    if (getDataAvailabilityRetryResult == null
        || !getDataAvailabilityRetryResult.isDataAvailable()) {
      // get task attempt ids from GetDataAvailabilityResponse and put them into the exception
      // to help debugging
      String taskAttemptIdInfo = "";
      if (getDataAvailabilityRetryResult != null
          && getDataAvailabilityRetryResult.getMapTaskCommitStatus() != null) {
        MapTaskCommitStatus mapTaskCommitStatus =
            getDataAvailabilityRetryResult.getMapTaskCommitStatus();
        if (mapTaskCommitStatus.getTaskAttemptIds().isEmpty()) {
          taskAttemptIdInfo = String.format("no task attempt committed");
        } else {
          List<Long> taskAttemptIds = mapTaskCommitStatus.getTaskAttemptIds()
              .stream().collect(Collectors.toList());
          Collections.sort(taskAttemptIds);
          taskAttemptIdInfo = String.format("committed task ids: %s, fetching tasks: %s",
              StringUtils.toString4SortedNumberList(taskAttemptIds),
              StringUtils.toString4SortedNumberList(fetchTaskAttemptIds
                  .stream().sorted().collect(Collectors.toList())));
        }
      }
      throw new RssShuffleDataNotAvailableException(String.format(
          "Not all mappers finished after trying %s:%s for %s millis, partition: %s, %s",
          host, port, dataAvailableWaitTime, appShufflePartitionId, taskAttemptIdInfo));
    }

    this.commitMapTaskCommitStatus = getDataAvailabilityRetryResult.getMapTaskCommitStatus();
    if (this.commitMapTaskCommitStatus == null) {
      throw new RssInvalidDataException("MapTaskCommitStatus should not be null");
    }
    this.commitTaskAttemptIds = this.commitMapTaskCommitStatus.getTaskAttemptIds();
    if (!this.commitTaskAttemptIds.containsAll(fetchTaskAttemptIds)) {
      throw new RssInvalidDataException(String.format(
          "Task attempt ids not matched, committed: %s, fetching: %s",
          this.commitTaskAttemptIds,
          fetchTaskAttemptIds));
    }

    return getDataAvailabilityRetryResult;
  }

  public DataBlock readDataBlock() {
    try {
      DataBlock dataBlock = readDataBlockNoCheckTaskAttemptId();
      while (dataBlock != null) {
        totalReadDataBlocks++;

        if (!fetchTaskAttemptIds.contains(dataBlock.getHeader().getTaskAttemptId())) {
          // ignore the previous record and read next record
          dataBlock = readDataBlockNoCheckTaskAttemptId();
          metrics.getNumIgnoredBlocks().inc(1);
        } else {
          break;
        }
      }
      return dataBlock;
    } catch (Throwable ex) {
      if (fixedLengthInputStream != null) {
        throw new RssStreamReadException(
            String.format(
                "Bad data stream, total expected bytes: %s, remaining unread bytes: %s, %s",
                fixedLengthInputStream.getLength(), fixedLengthInputStream.getRemaining(),
                connectionInfo)
            , ex);
      } else {
        throw ex;
      }
    }
  }

  @Override
  public void close() {
    try {
      super.close();
      closeMetrics();
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to close read client %s", this), ex);
    }
  }

  public AppShufflePartitionId getAppShufflePartitionId() {
    return appShufflePartitionId;
  }

  @Override
  public String toString() {
    return "DataBlockSocketReadClient{" +
        "user='" + user + '\'' +
        ", appShufflePartitionId=" + appShufflePartitionId +
        ", downloadStarted=" + downloadStarted +
        ", totalReadDataBlocks=" + totalReadDataBlocks +
        ", connectionInfo=" + connectionInfo +
        '}';
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

  private GetDataAvailabilityResponse getDataAvailability() {
    GetDataAvailabilityRequest request = new GetDataAvailabilityRequest();
    writeControlMessageAndWaitResponseStatus(request);
    GetDataAvailabilityResponse getDataAvailabilityResponse =
        readResponseMessage(MessageConstants.MESSAGE_GetDataAvailabilityResponse,
            GetDataAvailabilityResponse::deserialize);
    return getDataAvailabilityResponse;
  }

  private DataBlock readDataBlockNoCheckTaskAttemptId() {
    if (!downloadStarted) {
      if (this.commitTaskAttemptIds == null) {
        waitDataAvailable();
      }

      startDownload();
      downloadStarted = true;
    }

    if (commitTaskAttemptIds == null) {
      throw new RssInvalidStateException(
          String.format("commitTaskAttemptIds is null, %s", connectionInfo));
    }

    if (commitTaskAttemptIds.isEmpty()) {
      throw new RssInvalidStateException(
          String.format("commitTaskAttemptIds is empty, %s", connectionInfo));
    }

    DataBlockHeader header = readDataBlockHeader(inputStream);
    if (header == null) {
      return null;
    }

    byte[] bytes = StreamUtils.readBytes(inputStream, header.getLength());
    if (bytes == null) {
      throw new RssEndOfStreamException("Failed to read data block: " + this.toString());
    }

    return new DataBlock(header, bytes);
  }

  private void startDownload() {
    byte[] bytes = StreamUtils.readBytes(inputStream, Long.BYTES);
    if (bytes == null) {
      throw new RssEndOfStreamException(
          String.format("Hit unexpected end of stream: %s", connectionInfo));
    }
    dataLength = ByteBufUtils.readLong(bytes, 0);
    if (dataLength < 0) {
      throw new RssInvalidDataException(
          String.format("Invalid data length: %s, %s", dataLength, connectionInfo));
    }
    logger.info("Data length to read: {}", dataLength);
    fixedLengthInputStream = new FixedLengthInputStream(inputStream, dataLength);
    inputStream = fixedLengthInputStream;

    InputStream decompressedStream =
        Compression.decompressStream(inputStream, fileCompressionCodec);
    if (decompressedStream != inputStream) {
      inputStream = decompressedStream;
      logger.info("Switched to compressing stream {}, {}", appShufflePartitionId, connectionInfo);
    }
  }

  private DataBlockHeader readDataBlockHeader(InputStream dataStream) {
    // Header consists of: long taskAttemptID + int length
    byte[] bytes = StreamUtils.readBytes(dataStream, DataBlockHeader.NUM_BYTES);
    if (bytes == null) {
      if (fixedLengthInputStream != null && fixedLengthInputStream.getRemaining() != 0) {
        throw new RssInvalidDataException(String.format(
            "Bad data stream, total expected bytes: %s, remaining unread bytes: %s",
            fixedLengthInputStream.getLength(), fixedLengthInputStream.getRemaining()));
      }
      return null;
    }

    metrics.getNumReadBytes().inc(bytes.length);
    return DataBlockHeader.deserializeFromBytes(bytes);
  }

}
