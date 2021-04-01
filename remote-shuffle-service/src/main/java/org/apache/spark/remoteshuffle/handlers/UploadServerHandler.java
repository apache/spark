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

import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.apache.spark.remoteshuffle.exceptions.RssMaxConnectionsException;
import org.apache.spark.remoteshuffle.execution.ShuffleExecutor;
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/***
 * This class handles messages for shuffle writer to upload data.
 */
public class UploadServerHandler {
  private static final Logger logger = LoggerFactory.getLogger(UploadServerHandler.class);

  private final ShuffleExecutor executor;
  private final UploadChannelManager channelManager;

  private final ConcurrentHashMap<Long, AppShuffleId> taskAttemptMap = new ConcurrentHashMap<>();

  private String connectionInfo;

  private int numPartitions;
  private ShuffleWriteConfig writeConfig;

  private final ConcurrentHashMap<Long, Boolean> taskAttemptUploadStarted =
      new ConcurrentHashMap<>();

  public UploadServerHandler(ShuffleExecutor executor, UploadChannelManager channelManager) {
    this.executor = executor;
    this.channelManager = channelManager;

    channelManager.incNumConnections();
  }

  public void initializeAppTaskAttempt(AppShuffleId appShuffleId,
                                       long taskAttemptId,
                                       int numPartitions,
                                       ShuffleWriteConfig writeConfig,
                                       ChannelHandlerContext ctx) {
    this.connectionInfo = NettyUtils.getServerConnectionInfo(ctx.channel());

    this.numPartitions = numPartitions;
    this.writeConfig = writeConfig;

    AppShuffleId oldAppShuffleIdValue = this.taskAttemptMap.put(taskAttemptId, appShuffleId);
    if (oldAppShuffleIdValue != null) {
      if (!oldAppShuffleIdValue.equals(appShuffleId)) {
        throw new RssInvalidStateException(String.format(
            "There was already value %s with task attempt %s, but trying to set a different value %s",
            oldAppShuffleIdValue, taskAttemptId, appShuffleId));
      }
    }
  }

  public void checkMaxConnections() throws RssMaxConnectionsException {
    channelManager.checkMaxConnections();
  }

  public void updateLiveness(String appId) {
    executor.updateLiveness(appId);
  }

  public void checkAppMaxWriteBytes(String appId) {
    executor.checkAppMaxWriteBytes(appId);
  }

  public void onChannelInactive() {
    channelManager.decNumConnections();
  }

  public void writeRecord(
      org.apache.spark.remoteshuffle.messages.ShuffleDataWrapper shuffleDataWrapper) {
    AppShuffleId appShuffleId = getAppShuffleId(shuffleDataWrapper.getTaskAttemptId());

    lazyStartUpload(appShuffleId, shuffleDataWrapper.getTaskAttemptId());

    if (shuffleDataWrapper.getPartitionId() < 0 ||
        shuffleDataWrapper.getPartitionId() > numPartitions) {
      throw new RssInvalidDataException(String.format("Invalid partition: %s, %s",
          shuffleDataWrapper.getPartitionId(), connectionInfo));
    }

    executor.writeData(new org.apache.spark.remoteshuffle.execution.ShuffleDataWrapper(
        appShuffleId, shuffleDataWrapper.getTaskAttemptId(),
        shuffleDataWrapper.getPartitionId(),
        Unpooled.wrappedBuffer(shuffleDataWrapper.getBytes())));
  }

  public void finishUpload(long taskAttemptId) {
    AppShuffleId appShuffleId = getAppShuffleId(taskAttemptId);
    finishUploadImpl(appShuffleId, taskAttemptId);
  }

  private void finishUploadImpl(AppShuffleId appShuffleId, long taskAttemptIdToFinishUpload) {
    lazyStartUpload(appShuffleId, taskAttemptIdToFinishUpload);
    executor.finishUpload(appShuffleId, taskAttemptIdToFinishUpload);
    taskAttemptMap.remove(taskAttemptIdToFinishUpload);
    taskAttemptUploadStarted.remove(taskAttemptIdToFinishUpload);
  }

  // lazy initialize on executor when only upload the first record, so same map task
  // could retry connecting to the server without really start the upload
  private void lazyStartUpload(AppShuffleId appShuffleId, long taskAttemptIdToStartUpload) {
    if (!taskAttemptUploadStarted.getOrDefault(taskAttemptIdToStartUpload, false)) {
      executor.registerShuffle(appShuffleId, numPartitions, writeConfig);
      executor.startUpload(appShuffleId, taskAttemptIdToStartUpload);

      taskAttemptUploadStarted.put(taskAttemptIdToStartUpload, true);
    }
  }

  private AppShuffleId getAppShuffleId(long taskAttemptId) {
    AppShuffleId result = taskAttemptMap.get(taskAttemptId);
    if (result == null) {
      throw new RssInvalidStateException(String
          .format("Did not get app shuffle id for task attempt %s, %s", taskAttemptId,
              connectionInfo));
    }
    return result;
  }
}
