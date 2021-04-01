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

package org.apache.spark.remoteshuffle.handlers;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.FilePathAndLength;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.apache.spark.remoteshuffle.exceptions.RssShuffleCorruptedException;
import org.apache.spark.remoteshuffle.execution.ShuffleExecutor;
import org.apache.spark.remoteshuffle.messages.ConnectDownloadRequest;
import org.apache.spark.remoteshuffle.messages.ShuffleStageStatus;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage;
import org.apache.spark.remoteshuffle.storage.ShuffleStorage;
import org.apache.spark.remoteshuffle.util.LogUtils;
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.channel.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/***
 * This class processes client request to download shuffle data.
 */
public class DownloadServerHandler {
  private static final Logger logger = LoggerFactory.getLogger(DownloadServerHandler.class);

  private static final AtomicInteger numConcurrentReadFilesAtomicInteger = new AtomicInteger();
  private static final Gauge numConcurrentReadFiles =
      M3Stats.getDefaultScope().gauge("numConcurrentReadFiles");
  private static final Counter numReadFileBytes =
      M3Stats.getDefaultScope().counter("numReadFileBytes");

  private final ShuffleExecutor executor;
  private final ShuffleStorage storage = new ShuffleFileStorage();

  private AppShuffleId appShuffleId;
  private int partitionId;

  public DownloadServerHandler(ShuffleExecutor executor) {
    this.executor = executor;
  }

  public void initialize(ConnectDownloadRequest connectDownloadRequest) {
    this.appShuffleId = new AppShuffleId(
        connectDownloadRequest.getAppId(), connectDownloadRequest.getAppAttempt(),
        connectDownloadRequest.getShuffleId());
    this.partitionId = connectDownloadRequest.getPartitionId();
  }

  public ShuffleWriteConfig getShuffleWriteConfig(AppShuffleId appShuffleId) {
    return executor.getShuffleWriteConfig(appShuffleId);
  }

  public ShuffleStageStatus getShuffleStageStatus(AppShuffleId appShuffleId) {
    return executor.getShuffleStageStatus(appShuffleId);
  }

  public List<FilePathAndLength> getNonEmptyPartitionFiles(String connectionInfoForLogging) {
    if (!storage.isLocalStorage()) {
      throw new RssInvalidStateException(
          "Only local file storage is supported to download shuffle data, closing the connection");
    }

    List<FilePathAndLength> persistedBytes = executor.getPersistedBytes(
        appShuffleId, partitionId)
        .stream()
        .filter(t -> t.getLength() > 0)
        .collect(Collectors.toList());

    if (persistedBytes.isEmpty()) {
      return Collections.emptyList();
    }

    for (FilePathAndLength filePathAndLength : persistedBytes) {
      if (!storage.exists(filePathAndLength.getPath())) {
        throw new RssShuffleCorruptedException(String.format(
            "Shuffle file %s not found for partition %s, %s, %s, but there are persisted bytes: %s",
            filePathAndLength.getPath(), partitionId, appShuffleId, connectionInfoForLogging,
            filePathAndLength.getLength()));
      }
      long fileSize = storage.size(filePathAndLength.getPath());
      if (fileSize <= 0) {
        throw new RssShuffleCorruptedException(String.format(
            "Shuffle file %s is empty for partition %s, %s, %s, but there are persisted bytes: %s",
            filePathAndLength.getPath(), partitionId, appShuffleId, connectionInfoForLogging,
            filePathAndLength.getLength()));
      }
      if (fileSize < filePathAndLength.getLength()) {
        throw new RssShuffleCorruptedException(String.format(
            "Shuffle file %s has less size %s than expected %s for partition %s, %s, %s",
            filePathAndLength.getPath(), fileSize, filePathAndLength.getLength(), partitionId,
            appShuffleId, connectionInfoForLogging));
      }
    }

    long totalFileLength = persistedBytes.stream().mapToLong(t -> t.getLength()).sum();
    if (totalFileLength == 0) {
      logger.info(
          "Total file length is zero: {}, {}",
          StringUtils.join(persistedBytes, ','), connectionInfoForLogging);
      return Collections.emptyList();
    } else if (totalFileLength < 0) {
      throw new RssInvalidStateException(String.format(
          "Invalid total file length: %s, %s",
          totalFileLength, connectionInfoForLogging));
    }

    // TODO verify there is no open files
    return persistedBytes;
  }

  public void finishShuffleStage(AppShuffleId appShuffleId) {
    executor.finishShuffleStage(appShuffleId);
  }

  public ChannelFuture sendFiles(ChannelHandlerContext ctx, List<FilePathAndLength> nonEmptyFiles,
                                 ChannelIdleCheck idleCheck) {
    String connectionInfo = NettyUtils.getServerConnectionInfo(ctx);

    idleCheck.updateLastReadTime();

    ChannelFuture lastSendFileFuture = null;
    for (int i = 0; i < nonEmptyFiles.size(); i++) {
      final int fileIndex = i;
      String splitFile = nonEmptyFiles.get(fileIndex).getPath();
      long fileLength = nonEmptyFiles.get(fileIndex).getLength();
      logger.info(
          "Downloader server sending file: {} ({} of {}, {} bytes), {}",
          splitFile, fileIndex + 1, nonEmptyFiles.size(), fileLength, connectionInfo);
      // TODO support HDFS in future? need to remove code depending
      // on local file: new File(path)
      // TODO is storage.size(splitFile) reliable or consistent when finishing writing a file?
      DefaultFileRegion fileRegion = new DefaultFileRegion(
          new File(splitFile), 0, fileLength);
      ChannelFuture sendFileFuture = ctx.writeAndFlush(fileRegion,
          ctx.newProgressivePromise());
      int numConcurrentReadFilesValue = numConcurrentReadFilesAtomicInteger.incrementAndGet();
      numConcurrentReadFiles.update(numConcurrentReadFilesValue);
      final long sendFileStartTime = System.currentTimeMillis();
      sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
        @Override
        public void operationComplete(ChannelProgressiveFuture future) throws Exception {
          executor.updateLiveness(appShuffleId.getAppId());
          idleCheck.updateLastReadTime();
          int numConcurrentReadFilesValue = numConcurrentReadFilesAtomicInteger.decrementAndGet();
          numConcurrentReadFiles.update(numConcurrentReadFilesValue);
          numReadFileBytes.inc(fileLength);
          String exceptionInfo = "";
          Throwable futureException = future.cause();
          if (futureException != null) {
            M3Stats.addException(futureException, this.getClass().getSimpleName());
            exceptionInfo = String.format(
                ", exception: %s, %s",
                org.apache.spark.remoteshuffle.util.ExceptionUtils.getSimpleMessage(future.cause()),
                ExceptionUtils.getStackTrace(future.cause()));
          }
          double dataSpeed = LogUtils
              .calculateMegaBytesPerSecond(System.currentTimeMillis() - sendFileStartTime,
                  fileLength);
          logger.info(
              "Finished sending file: {} ({} of {}), success: {} ({} mbs, total {} bytes), connection: {} {} {}",
              splitFile, fileIndex + 1, nonEmptyFiles.size(), future.isSuccess(), dataSpeed,
              fileLength, connectionInfo, System.nanoTime(), exceptionInfo);
        }

        @Override
        public void operationProgressed(ChannelProgressiveFuture future, long progress, long total)
            throws Exception {
          double dataSpeed = LogUtils
              .calculateMegaBytesPerSecond(System.currentTimeMillis() - sendFileStartTime,
                  progress);
          logger.debug(
              "Sending file: {}, progress: {} out of {} bytes, {} mbs, {}",
              splitFile, progress, total, dataSpeed, connectionInfo);
          executor.updateLiveness(appShuffleId.getAppId());
          idleCheck.updateLastReadTime();
        }
      });
      lastSendFileFuture = sendFileFuture;
    }

    return lastSendFileFuture;
  }
}
