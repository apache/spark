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

package org.apache.spark.remoteshuffle.tools;

import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerReplicationGroup;
import org.apache.spark.remoteshuffle.util.ServerHostAndPort;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.remoteshuffle.clients.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/***
 * This class uses ReadClient to verify stream file (shuffle file) in test tools.
 */
public class StreamReadClientVerify {
  private static final Logger logger = LoggerFactory.getLogger(StreamReadClientVerify.class);

  private List<ServerDetail> rssServers = new ArrayList<>();

  private int numReplicas = 1;

  private String appId;
  private String appAttempt;
  private int shuffleId;

  private int numPartitions;
  private int partitionFanout = 1;

  // Expected total records in the files
  private long expectedTotalRecords = 0;

  private Map<Integer, Long> expectedTotalRecordsForEachPartition;

  private int maxValueLen = 10000;

  private Runnable actionToSimulateBadServer = null;

  public void setRssServers(List<ServerDetail> rssServers, int numReplicas) {
    this.rssServers = new ArrayList<>(rssServers);
    this.numReplicas = numReplicas;
  }

  public void setActionToSimulateBadServer(Runnable actionToSimulateBadServer) {
    this.actionToSimulateBadServer = actionToSimulateBadServer;
  }

  public void setAppShuffleId(AppShuffleId appShuffleId) {
    this.appId = appShuffleId.getAppId();
    this.appAttempt = appShuffleId.getAppAttempt();
    this.shuffleId = appShuffleId.getShuffleId();
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public void setPartitionFanout(int partitionFanout) {
    this.partitionFanout = partitionFanout;
  }

  public void setExpectedTotalRecords(long expectedTotalRecords) {
    this.expectedTotalRecords = expectedTotalRecords;
  }

  public void setExpectedTotalRecordsForEachPartition(
      Map<Integer, Long> expectedTotalRecordsForEachPartition) {
    this.expectedTotalRecordsForEachPartition = expectedTotalRecordsForEachPartition;
  }

  public void verifyRecords(Collection<Integer> partitionIds,
                            Collection<Long> fetchTaskAttemptIds) {
    AtomicLong totalReadRecords = new AtomicLong();

    if (partitionIds == null) {
      partitionIds = IntStream.range(0, numPartitions).boxed().collect(Collectors.toList());
      logger.info(String.format("Verifying record for partitions: [%s, %s)", 0, numPartitions));
    } else {
      logger.info(String
          .format("Verifying record for partitions: %s", StringUtils.join(partitionIds, ",")));
    }

    for (int partition : partitionIds) {
      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
          appId, appAttempt, shuffleId, partition);

      int socketTimeoutMillis = 120 * 1000;
      int dataAvailableWaitTime = socketTimeoutMillis * 3;
      int dataAvailablePollInterval = 10;
      boolean checkDataConsistency = true;
      MultiServerReadClient readClient;
      List<ServerReplicationGroup> serverReplicationGroups;
      serverReplicationGroups = ServerReplicationGroupUtil
          .createReplicationGroupsForPartition(rssServers, numReplicas, partition,
              partitionFanout);
      readClient = new MultiServerSocketReadClient(serverReplicationGroups,
          socketTimeoutMillis,
          new ClientRetryOptions(dataAvailablePollInterval, dataAvailableWaitTime),
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(
              fetchTaskAttemptIds,
              dataAvailablePollInterval,
              dataAvailableWaitTime),
          checkDataConsistency);
      logger.info(String.format("Connecting replicated read client: %s", readClient));
      readClient.connect();
      try {
        long numReadPartitionRecords = 0;
        TaskDataBlock record = readClient.readDataBlock();
        while (record != null) {
          numReadPartitionRecords++;
          long totalReadRecordsValue = totalReadRecords.incrementAndGet();

          if (totalReadRecordsValue == expectedTotalRecords / 2) {
            if (actionToSimulateBadServer != null) {
              logger.info("Simulate bad server during shuffle read");
              actionToSimulateBadServer.run();
            }
          }

          if (record.getPayload() != null && record.getPayload().length > maxValueLen) {
            throw new RuntimeException(String.format(
                "Read wrong value len %s after reading %s records for %s from server %s",
                record.getPayload(), numReadPartitionRecords, appShufflePartitionId,
                serverReplicationGroups));
          }

          record = readClient.readDataBlock();
        }
        logger.info(String.format("Closing read client for %s", appShufflePartitionId));

        long expectedNumRecords = expectedTotalRecordsForEachPartition.getOrDefault(partition, 0L);
        if (numReadPartitionRecords != expectedNumRecords) {
          throw new RuntimeException(String.format(
              "Verify error for partition %s, servers %s, expected records: %s, actual records: %s",
              appShufflePartitionId, serverReplicationGroups, expectedNumRecords,
              numReadPartitionRecords));
        }

        logger.info(String.format("Verified %s records for %s from server %s",
            numReadPartitionRecords, appShufflePartitionId, serverReplicationGroups));
      } finally {
        readClient.close();
      }
    }

    String logMsg = String
        .format("Total expected records: %s, total records read from servers: %s",
            expectedTotalRecords, totalReadRecords);
    logger.info(logMsg);

    if (expectedTotalRecords != 0 && expectedTotalRecords != totalReadRecords.get()) {
      throw new RuntimeException(logMsg);
    }
  }

  public static void main(String[] args) {
    StreamReadClientVerify tool = new StreamReadClientVerify();

    int i = 0;
    while (i < args.length) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-rssServers")) {
        String str = args[i++];
        String[] strArray = str.split(":");
        List<ServerDetail> serverDetails = Arrays.asList(strArray).stream().map(t -> {
          ServerHostAndPort hostAndPort = ServerHostAndPort.fromString(t);
          return TestUtils.getServerDetail(hostAndPort.getHost(), hostAndPort.getPort());
        }).collect(Collectors.toList());
        tool.rssServers.addAll(serverDetails);
      } else if (argName.equalsIgnoreCase("-appId")) {
        tool.appId = args[i++];
      } else if (argName.equalsIgnoreCase("-appAttempt")) {
        tool.appAttempt = args[i++];
      } else if (argName.equalsIgnoreCase("-shuffleId")) {
        tool.shuffleId = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-expectedTotalRecords")) {
        tool.expectedTotalRecords = Long.parseLong(args[i++]);
      } else {
        throw new IllegalArgumentException("Unsupported argument: " + argName);
      }
    }

    // TODO refine following
    tool.verifyRecords(null, null);
  }
}
