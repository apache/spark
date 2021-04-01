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

package org.apache.spark.remoteshuffle.testutil;

import org.apache.spark.remoteshuffle.clients.PlainShuffleDataSocketReadClient;
import org.apache.spark.remoteshuffle.clients.SingleServerReadClient;
import org.apache.spark.remoteshuffle.clients.TaskDataBlock;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.exceptions.RssException;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StreamServerTestUtils {

  public static List<String> createTempDirectories(int count) {
    List<String> dirs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      try {
        dirs.add(Files.createTempDirectory("RssShuffleManagerTest").toString());
      } catch (IOException e) {
        throw new RssException("Failed to create temp directory", e);
      }
    }
    return dirs;
  }

  public static List<TaskDataBlock> readAllRecords2(int port, AppShuffleId appShuffleId,
                                                    int partitionId,
                                                    Collection<Long> fetchTaskAttemptIds) {
    return readAllRecords2(port, appShuffleId, partitionId, fetchTaskAttemptIds,
        TestConstants.DATA_AVAILABLE_TIMEOUT);
  }

  public static List<TaskDataBlock> readAllRecords2(int port, AppShuffleId appShuffleId,
                                                    int partitionId,
                                                    Collection<Long> fetchTaskAttemptIds,
                                                    int dataAvailableWaitTime) {
    SingleServerReadClient readClient = null;
    readClient =
        new PlainShuffleDataSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT,
            "user1", new AppShufflePartitionId(appShuffleId, partitionId), fetchTaskAttemptIds,
            TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableWaitTime);

    try {
      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
          appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(),
          partitionId);

      readClient.connect();

      List<TaskDataBlock> result = new ArrayList<>();

      TaskDataBlock record = readClient.readDataBlock();
      while (record != null) {
        result.add(record);
        record = readClient.readDataBlock();
      }
      return result;
    } finally {
      readClient.close();
    }
  }

  public static void waitTillDataAvailable(int port, AppShuffleId appShuffleId,
                                           Collection<Integer> partitionIds,
                                           Collection<Long> fetchTaskAttemptIds) {
    for (Integer p : partitionIds) {
      waitTillDataAvailable(port, appShuffleId, p, fetchTaskAttemptIds);
    }
  }

  public static void waitTillDataAvailable(int port, AppShuffleId appShuffleId, int partitionId,
                                           Collection<Long> fetchTaskAttemptIds) {
    AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
        appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(),
        partitionId);
    SingleServerReadClient readClient = readClient =
        new PlainShuffleDataSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT,
            "user1", new AppShufflePartitionId(appShuffleId, partitionId), fetchTaskAttemptIds,
            TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
    try {
      readClient.connect();
      readClient.readDataBlock();
    } finally {
      readClient.close();
    }
  }
}
