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

package org.apache.spark.remoteshuffle.testutil;

import org.apache.spark.remoteshuffle.clients.*;
import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClientTestUtils {
  public final static List<Pair<String, String>> writeRecords1 = new ArrayList<>();
  public final static List<Pair<String, String>> writeRecords2 = new ArrayList<>();
  public final static List<Pair<String, String>> writeRecords3 = new ArrayList<>();

  static {
    int numRecords1 = 100000;
    for (int i = 0; i < numRecords1; i++) {
      String str1 = "k" + i;
      String str2 = "str2";
      writeRecords1.add(Pair.of(str1, str2));
    }

    int numRecords2 = 8;
    for (int i = 0; i < numRecords2; i++) {
      String str1 = StringUtils.repeat('a', 1000000) + i;
      String str2 = StringUtils.repeat('b', 1000);
      writeRecords2.add(Pair.of(str1, str2));
    }

    int numRecords3 = 8;
    for (int i = 0; i < numRecords3; i++) {
      String str1 = StringUtils.repeat('x', 1000) + i;
      String str2 = StringUtils.repeat('y', 1000000);
      writeRecords3.add(Pair.of(str1, str2));
    }
  }

  public static void connectAndWriteData(Map<Integer, List<Pair<String, String>>> mapTaskData,
                                         int numMaps, int numPartitions,
                                         AppTaskAttemptId appTaskAttemptId,
                                         SingleServerWriteClient writeClient) {
    writeClient.connect();
    writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
    for (Integer partition : mapTaskData.keySet()) {
      List<Pair<String, String>> records = mapTaskData.get(partition);
      for (Pair<String, String> record : records) {
        writeClient.writeDataBlock(partition,
            ByteBuffer.wrap(record.getValue().getBytes(StandardCharsets.UTF_8)));
      }
    }
    writeClient.finishUpload();
  }

  public static List<TaskDataBlock> readData(AppShufflePartitionId appShufflePartitionId,
                                             ShuffleDataSocketReadClient readClient) {
    readClient.connect();
    List<TaskDataBlock> readRecords = new ArrayList<>();
    TaskDataBlock record = readClient.readDataBlock();
    while (record != null) {
      readRecords.add(record);
      record = readClient.readDataBlock();
    }
    return readRecords;
  }

  public static SingleServerWriteClient getOrCreateWriteClient(int port, String appId,
                                                               String appAttempt) {
    return getOrCreateWriteClient(port, appId, appAttempt, true);
  }

  public static SingleServerWriteClient getOrCreateWriteClient(int port, String appId,
                                                               String appAttempt,
                                                               boolean finishUploadAck) {
    ShuffleWriteConfig shuffleWriteConfig = new ShuffleWriteConfig((short) 3);
    return PooledWriteClientFactory.getInstance()
        .getOrCreateClient("localhost", port, TestConstants.NETWORK_TIMEOUT, finishUploadAck,
            "user1", appId, appAttempt, shuffleWriteConfig);
  }
}
