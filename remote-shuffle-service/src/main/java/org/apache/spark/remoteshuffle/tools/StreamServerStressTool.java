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

import org.apache.spark.remoteshuffle.StreamServer;
import org.apache.spark.remoteshuffle.StreamServerConfig;
import org.apache.spark.remoteshuffle.clients.*;
import org.apache.spark.remoteshuffle.common.*;
import org.apache.spark.remoteshuffle.metadata.InMemoryServiceRegistry;
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.metrics.ScheduledMetricCollector;
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage;
import org.apache.spark.remoteshuffle.storage.ShuffleStorage;
import org.apache.spark.remoteshuffle.util.ExceptionUtils;
import org.apache.spark.remoteshuffle.util.RateCounter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StreamServerStressTool {
  public static ConcurrentHashMap<Integer, ConcurrentHashMap<String, AtomicLong>> debug =
      new ConcurrentHashMap<>();

  private static final int SHUFFLE_RECORD_EXTRA_BYTES = 8 + 4 + 4;

  private static final Logger logger = LoggerFactory.getLogger(StreamServerStressTool.class);

  private ScheduledExecutorService scheduler;
  private ScheduledMetricCollector scheduledMetricCollector;

  private List<StreamServer> servers = new ArrayList<>();

  // store servers to shut down, this is to simulate failed servers during shuffle write/read, when there are shuffle replicas.
  private final List<String> serverIdsToShutdownDuringShuffleWrite = new ArrayList<>();
  private final List<String> serverIdsToShutdownDuringShuffleRead = new ArrayList<>();

  private List<String> serverHosts = new ArrayList<>();
  private List<Integer> serverPorts = new ArrayList<>();
  private List<String> serverRootDirs = new ArrayList<>();
  private List<ServerDetail> serverDetails = new ArrayList<>();

  private boolean useEpoll = false;

  private String workDir = "/tmp/rss";

  private int numServers = 4;
  private int numServerThreads = 5;

  private String appId = "app_" + System.nanoTime();
  private String appAttempt = "exec1";

  // Number of total map tasks
  private int numMaps = 10;

  // Number of total map tasks
  private int numMapAttempts = 1;

  // This tool generates a range of map tasks to simulate uploading data.
  // This field specifies the lower bound (inclusive) of the map id.
  private int startMapId = 0;

  // This tool generates a range of map tasks to simulate uploading data.
  // This field specifies the upper bound (inclusive) of the map id.
  private int endMapId = numMaps - 1;

  // Number of total partitions
  private int numPartitions = 7;

  // Number of servers (or replication groups) per partition
  private int partitionFanout = 1;

  // Number of splits for shuffle file
  int numSplits = 3;

  // Number of shuffle data replicas
  int numReplicas = 1;

  // Number of milliseconds we delay a map task to start. This is to simulate different map 
  // tasks starting at different time
  private int mapDelay = 1000;

  // Inject sleep in map task to simulate slowness in the map task. The value is in milliseconds.
  // If this value is not zero, map tasks will sleep that amount of milliseconds after uploading
  // each record.
  private int mapSlowness = 0;

  // How long the client should wait before timeout
  private long maxWait = 30 * 1000;

  // Whether to use connection pool
  private boolean useConnectionPool = false;

  // Total number of bytes for all map tasks to generate
  private long numBytes = 100 * 1024 * 1024;

  private int writeClientQueueSize = 100;
  private int writeClientThreads = 4;

  // Whether to delete files after the test
  private boolean deleteFiles = true;

  // Total number of test values to use. This tool wil generate a list of test values and use them
  // to fill shuffle data.
  private int numTestValues = 1000;

  // Max length for test values to use. This tool wil generate a list of test values and use them
  // to fill shuffle data.
  private int maxTestValueLen = 10000;

  private ShuffleStorage storage;

  private ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();

  private Random random = new Random();

  // Total written bytes in shuffle files
  private AtomicLong totalShuffleWrittenBytes = new AtomicLong();
  // Total written records in shuffle files
  private AtomicLong totalShuffleWrittenRecords = new AtomicLong();

  // Successfully written records (by last mapper task attempt) in shuffle files
  private AtomicLong successShuffleWrittenRecords = new AtomicLong();

  // Total bytes sent through socket
  private AtomicLong totalSocketBytes = new AtomicLong();

  // Threads for all map tasks
  private List<Thread> allMapThreads = new ArrayList<>();

  private AppShuffleId appShuffleId;

  private AtomicLong mapThreadErrors = new AtomicLong();

  private ConcurrentHashMap<Integer, Object> usedPartitionIds = new ConcurrentHashMap<>();

  public void setServerHosts(List<String> serverHosts) {
    this.serverHosts = serverHosts;
  }

  public void setServerPorts(List<Integer> serverPorts) {
    this.serverPorts = serverPorts;
  }

  public void setServerRootDirs(List<String> serverRootDirs) {
    this.serverRootDirs = serverRootDirs;
  }

  public void setWorkDir(String workDir) {
    this.workDir = workDir;
  }

  public void setNumServers(int numServers) {
    this.numServers = numServers;
  }

  public void setNumServerThreads(int numServerThreads) {
    this.numServerThreads = numServerThreads;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public void setAppAttempt(String appAttempt) {
    this.appAttempt = appAttempt;
  }

  /***
   * This method will reset endMapId value as well.
   * @param numMaps
   */
  public void setNumMaps(int numMaps) {
    this.numMaps = numMaps;
    // Reset default value for endMapId
    this.endMapId = numMaps - 1;
  }

  public void setNumMapAttempts(int numMapAttempts) {
    this.numMapAttempts = numMapAttempts;
  }

  public void setStartMapId(int startMapId) {
    this.startMapId = startMapId;
  }

  /***
   * setNumMaps() will reset value for endMapId.
   * Thus please make sure to not call setNumMaps() after calling this method. 
   * @param endMapId
   */
  public void setEndMapId(int endMapId) {
    this.endMapId = endMapId;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public void setNumSplits(int numSplits) {
    this.numSplits = numSplits;
  }

  public void setNumReplicas(int numReplicas) {
    this.numReplicas = numReplicas;
  }

  public void setPartitionFanout(int partitionFanout) {
    this.partitionFanout = partitionFanout;
  }

  public void setMapDelay(int mapDelay) {
    this.mapDelay = mapDelay;
  }

  public void setMapSlowness(int mapSlowness) {
    this.mapSlowness = mapSlowness;
  }

  public void setMaxWait(long maxWait) {
    this.maxWait = maxWait;
  }

  public void setUseConnectionPool(boolean useConnectionPool) {
    this.useConnectionPool = useConnectionPool;
  }

  public void setNumBytes(long numBytes) {
    this.numBytes = numBytes;
  }

  public void setWriteClientQueueSize(int writeClientQueueSize) {
    this.writeClientQueueSize = writeClientQueueSize;
  }

  public void setWriteClientThreads(int writeClientThreads) {
    this.writeClientThreads = writeClientThreads;
  }

  public void setDeleteFiles(boolean deleteFiles) {
    this.deleteFiles = deleteFiles;
  }

  public void setNumTestValues(int numTestValues) {
    this.numTestValues = numTestValues;
  }

  public void setMaxTestValueLen(int maxTestValueLen) {
    this.maxTestValueLen = maxTestValueLen;
  }

  public void setStorage(ShuffleStorage storage) {
    this.storage = storage;
  }

  public void setServiceRegistry(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public void run() {
    scheduler = Executors.newScheduledThreadPool(1);
    scheduledMetricCollector = new ScheduledMetricCollector(serviceRegistry);
    scheduledMetricCollector
        .scheduleCollectingMetrics(scheduler, ServiceRegistry.DEFAULT_DATA_CENTER,
            ServiceRegistry.DEFAULT_TEST_CLUSTER);

    appShuffleId = new AppShuffleId(appId, appAttempt, 1);

    storage = new ShuffleFileStorage();

    // Start Remote Shuffle Service servers if no server hosts are provided
    if (serverHosts.isEmpty()) {
      for (int i = 0; i < numServers; i++) {
        startNewServer();
      }

      // only simulate bad servers when using multiple replicas
      if (numReplicas > 1) {
        List<ServerReplicationGroup> serverReplicationGroups =
            ServerReplicationGroupUtil.createReplicationGroups(serverDetails, numReplicas);
        serverReplicationGroups.forEach(t -> {
          logger.info(String.format(String.format("Server replication group: %s", t)));
        });
        int halfSize = (int) Math.ceil((double) serverReplicationGroups.size() / 2.0);
        List<ServerReplicationGroup> firstHalf =
            serverReplicationGroups.stream().limit(halfSize).collect(Collectors.toList());
        List<ServerReplicationGroup> secondHalf =
            serverReplicationGroups.stream().skip(halfSize).collect(Collectors.toList());
        serverIdsToShutdownDuringShuffleWrite.addAll(
            firstHalf.stream().map(t -> t.getServers().get(0).getServerId())
                .collect(Collectors.toList()));
        serverIdsToShutdownDuringShuffleRead.addAll(
            secondHalf.stream().map(t -> t.getServers().get(0).getServerId())
                .collect(Collectors.toList()));
        logger.info(String.format(String.format(
            "Servers to shutdown during shuffle write: %s, servers to shutdown during shuffle read: %s",
            serverIdsToShutdownDuringShuffleWrite, serverIdsToShutdownDuringShuffleRead)));
      }
    }

    logger.info(String.format("Server root dirs: %s", StringUtils.join(serverRootDirs, ':')));

    // Generate test values to use

    List<byte[]> testValues = new ArrayList<>();

    testValues.add(null);
    testValues.add(new byte[0]);
    testValues.add("".getBytes(StandardCharsets.UTF_8));

    while (testValues.size() < numTestValues) {
      char ch = (char) ('a' + random.nextInt(26));
      int repeats = random.nextInt(maxTestValueLen);
      String str = StringUtils.repeat(ch, repeats);
      testValues.add(str.getBytes(StandardCharsets.UTF_8));
    }

    // Simulate some map tasks which do not write any shuffle data

    Set<Integer> mapIdsWritingEmptyData = new HashSet<>();

    if (endMapId + 1 - startMapId > 2) {
      mapIdsWritingEmptyData.add(startMapId + random.nextInt(endMapId + 1 - startMapId));
      mapIdsWritingEmptyData.add(startMapId + random.nextInt(endMapId + 1 - startMapId));
    }

    // Create map task threads to upload shuffle data

    RateCounter[] rateCounters = new RateCounter[endMapId + 1 - startMapId];

    AtomicLong taskAttemptIdSeed = new AtomicLong();

    List<Integer> simulatedNumberOfAttemptsForMappers = new ArrayList<>();

    List<Long> fetchTaskAttemptIds = new ArrayList<>();

    ConcurrentHashMap<Integer, AtomicLong> numPartitionRecords = new ConcurrentHashMap<>();

    for (int i = startMapId; i <= endMapId; i++) {
      int value = random.nextInt(numMapAttempts) + 1;
      simulatedNumberOfAttemptsForMappers.add(value);
    }

    for (int i = startMapId; i <= endMapId; i++) {
      final int mapId = i;
      final int index = i - startMapId;
      rateCounters[index] = new RateCounter(5000);

      AppMapId appMapId = new AppMapId(appShuffleId.getAppId(), appShuffleId.getAppAttempt(),
          appShuffleId.getShuffleId(), mapId);

      int simulatedNumberOfAttempts = simulatedNumberOfAttemptsForMappers.get(index);

      Thread thread = new Thread(() -> {
        for (int attempt = 1; attempt <= simulatedNumberOfAttempts; attempt++) {
          long taskAttemptId = taskAttemptIdSeed.getAndIncrement();
          boolean isLastTaskAttempt = attempt == simulatedNumberOfAttempts;
          boolean simulateEmptyData = mapIdsWritingEmptyData.contains(mapId);

          if (isLastTaskAttempt) {
            synchronized (fetchTaskAttemptIds) {
              fetchTaskAttemptIds.add(taskAttemptId);
            }
          }

          simulateMapperTask(testValues,
              appMapId,
              taskAttemptId,
              isLastTaskAttempt,
              simulateEmptyData,
              rateCounters[index],
              numPartitionRecords);
        }
      });

      thread.setName(String.format("[Map Thread %s]", appMapId));

      thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          logger.error(String.format("Mapper thread %s got exception", t.getName()), e);
          mapThreadErrors.incrementAndGet();
        }
      });

      allMapThreads.add(thread);
    }

    long uploadStartTime = System.currentTimeMillis();

    // Start map task threads
    allMapThreads.forEach(t -> t.start());

    // Wait for map tasks to finish
    allMapThreads.forEach(t -> {
      try {
        t.join();
      } catch (InterruptedException e) {
        M3Stats.addException(e, this.getClass().getSimpleName());
        throw new RuntimeException(e);
      }
    });

    long uploadDuration = System.currentTimeMillis() - uploadStartTime;
    double throughputMb = uploadDuration == 0 ? 0 :
        (((double) totalShuffleWrittenBytes.get()) / uploadDuration) *
            (1000.0 / (1024.0 * 1024.0));
    logger.info(String.format(
        "Total written bytes: %s, records: %s, throughput: %s mb/s, total socket bytes: %s",
        totalShuffleWrittenBytes, totalShuffleWrittenRecords, throughputMb, totalSocketBytes));

    if (mapThreadErrors.get() > 0) {
      throw new RuntimeException("Number of errors in map threads: " + mapThreadErrors);
    }

    // Verify or delete files if necessary

    if (!servers.isEmpty()) {
      try {
        int replicasForReadClient = numReplicas;

        Map<Integer, Long> expectedTotalRecordsForEachPartition = new HashMap<>();
        numPartitionRecords.entrySet().stream().forEach(
            t -> expectedTotalRecordsForEachPartition.put(t.getKey(), t.getValue().get()));

        StreamReadClientVerify streamReadClientVerify = new StreamReadClientVerify();
        streamReadClientVerify.setRssServers(serverDetails, replicasForReadClient);
        streamReadClientVerify.setAppShuffleId(appShuffleId);
        streamReadClientVerify.setNumPartitions(numPartitions);
        streamReadClientVerify.setPartitionFanout(partitionFanout);
        streamReadClientVerify.setExpectedTotalRecords(successShuffleWrittenRecords.get());
        streamReadClientVerify
            .setExpectedTotalRecordsForEachPartition(expectedTotalRecordsForEachPartition);

        streamReadClientVerify.setActionToSimulateBadServer(() -> {
          synchronized (servers) {
            synchronized (serverIdsToShutdownDuringShuffleRead) {
              for (String serverId : serverIdsToShutdownDuringShuffleRead) {
                StreamServer server = servers.stream().filter(t -> t != null)
                    .filter(t -> t.getServerId().equals(serverId)).findFirst().get();
                logger.info(String
                    .format("Simulate bad server during shuffle read by shutting down server: %s",
                        server));
                shutdownServer(server);

                int index = servers.indexOf(server);
                servers.set(index, null);
              }
              serverIdsToShutdownDuringShuffleRead.clear();
            }
          }
        });

        logger.info(String
            .format("Verifying reading from servers: %s", StringUtils.join(serverDetails, ", ")));
        streamReadClientVerify.verifyRecords(usedPartitionIds.keySet(), fetchTaskAttemptIds);
        logger.info(String
            .format("Verifying reading from servers: %s", StringUtils.join(serverDetails, ", ")));
      } catch (Throwable ex) {
        M3Stats.addException(ex, this.getClass().getSimpleName());
        logger.error(String.format("Failed to verify reading from servers: %s",
            StringUtils.join(serverDetails, ", ")), ex);
        throw ex;
      } finally {
        PooledWriteClientFactory.getInstance().shutdown();

        synchronized (servers) {
          servers.forEach(t -> {
            if (t != null) {
              shutdownServer(t);
            }
          });
        }

        if (deleteFiles) {
          try {
            logger
                .info(String.format("Deleting files: %s", StringUtils.join(serverRootDirs, ", ")));
            deleteDirectories(serverRootDirs);
            logger
                .info(String.format("Deleted files: %s", StringUtils.join(serverRootDirs, ", ")));
          } catch (Throwable ex) {
            M3Stats.addException(ex, this.getClass().getSimpleName());
            logger.info("Got some error when deleting files: %s, ignored them");
          }
        }
      }
    }

    if (mapThreadErrors.get() > 0) {
      throw new RuntimeException("Number of errors in map threads: " + mapThreadErrors);
    }
  }

  public void cleanup() {
    scheduledMetricCollector
        .collectMetrics(ServiceRegistry.DEFAULT_DATA_CENTER, ServiceRegistry.DEFAULT_TEST_CLUSTER);

    if (scheduler != null) {
      scheduler.shutdown();
      scheduler = null;
    }
  }

  @Override
  public String toString() {
    return "StreamServerStressTool{" +
        "serverHosts=" + serverHosts +
        ", useEpoll=" + useEpoll +
        ", serverPorts=" + serverPorts +
        ", serverRootDirs=" + serverRootDirs +
        ", workDir='" + workDir + '\'' +
        ", numServers=" + numServers +
        ", numServerThreads=" + numServerThreads +
        ", appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", numBytes=" + numBytes +
        ", numMaps=" + numMaps +
        ", numMapAttempts=" + numMapAttempts +
        ", startMapId=" + startMapId +
        ", endMapId=" + endMapId +
        ", numPartitions=" + numPartitions +
        ", numSplits=" + numSplits +
        ", numReplicas=" + numReplicas +
        ", partitionFanout=" + partitionFanout +
        ", mapDelay=" + mapDelay +
        ", mapSlowness=" + mapSlowness +
        ", maxWait=" + maxWait +
        ", writeClientQueueSize=" + writeClientQueueSize +
        ", writeClientThreads=" + writeClientThreads +
        ", deleteFiles=" + deleteFiles +
        ", numTestValues=" + numTestValues +
        ", maxTestValueLen=" + maxTestValueLen +
        ", storage=" + storage +
        ", serviceRegistry=" + serviceRegistry +
        ", useConnectionPool=" + useConnectionPool +
        '}';
  }

  private void simulateMapperTask(List<byte[]> testValues,
                                  AppMapId appMapId,
                                  long taskAttemptId,
                                  boolean isLastTaskAttempt,
                                  boolean simulateEmptyData,
                                  RateCounter rateCounter,
                                  ConcurrentHashMap<Integer, AtomicLong> numPartitionRecords) {
    if (mapDelay > 0) {
      int delayMillis = random.nextInt(mapDelay);
      logger.info(String.format("Delaying map %s: %s", appMapId, delayMillis));
      try {
        Thread.sleep(delayMillis);
      } catch (InterruptedException e) {
        M3Stats.addException(e, this.getClass().getSimpleName());
        throw new RuntimeException(e);
      }
    }

    ShuffleWriteConfig shuffleWriteConfig = new ShuffleWriteConfig((short) numSplits);

    MultiServerWriteClient writeClient;
    int networkTimeoutMillis = 120 * 1000;
    long maxTryingMillis = networkTimeoutMillis * 3;
    List<ServerReplicationGroup> serverReplicationGroups =
        ServerReplicationGroupUtil.createReplicationGroups(serverDetails, numReplicas);
    boolean finishUploadAck = true; // TODO make this configurable
    if (writeClientQueueSize == 0) {
      // Use sync write client (MultiServerSyncWriteClient)
      writeClient = new MultiServerSyncWriteClient(serverReplicationGroups, partitionFanout,
          networkTimeoutMillis, maxTryingMillis, finishUploadAck, useConnectionPool, "user1",
          appId, appAttempt, shuffleWriteConfig);
      writeClient.connect();
      writeClient
          .startUpload(new AppTaskAttemptId(appMapId, taskAttemptId), numMaps, numPartitions);
    } else {
      // Use async write client (MultiServerAsyncWriteClient)
      writeClient = new MultiServerAsyncWriteClient(serverReplicationGroups, partitionFanout,
          networkTimeoutMillis, maxTryingMillis, finishUploadAck, useConnectionPool,
          writeClientQueueSize, writeClientThreads, "user1", appId, appAttempt,
          shuffleWriteConfig);
      writeClient.connect();
      writeClient
          .startUpload(new AppTaskAttemptId(appMapId, taskAttemptId), numMaps, numPartitions);
    }

    logger.info(String
        .format("Map %s attempt %s started, write client: %s", appMapId, taskAttemptId,
            writeClient));

    if (!simulateEmptyData) {
      int partitionId = random.nextInt(numPartitions);
      writeClient.writeDataBlock(partitionId, null);

      totalShuffleWrittenBytes.addAndGet(SHUFFLE_RECORD_EXTRA_BYTES);
      totalShuffleWrittenRecords.incrementAndGet();

      if (isLastTaskAttempt) {
        successShuffleWrittenRecords.incrementAndGet();
        usedPartitionIds.putIfAbsent(partitionId, partitionId);
        numPartitionRecords.computeIfAbsent(partitionId, k -> new AtomicLong()).incrementAndGet();
      }

      writeClient.writeDataBlock(partitionId, ByteBuffer.wrap(new byte[0]));

      totalShuffleWrittenBytes.addAndGet(SHUFFLE_RECORD_EXTRA_BYTES);
      totalShuffleWrittenRecords.incrementAndGet();

      if (isLastTaskAttempt) {
        successShuffleWrittenRecords.incrementAndGet();
        usedPartitionIds.putIfAbsent(partitionId, partitionId);
        numPartitionRecords.computeIfAbsent(partitionId, k -> new AtomicLong()).incrementAndGet();
      }

      while (totalShuffleWrittenBytes.get() < numBytes) {
        long totalShuffleWrittenBytesOldValue = totalShuffleWrittenBytes.get();

        partitionId = random.nextInt(numPartitions);

        byte[] keyData = testValues.get(random.nextInt(testValues.size()));
        if (keyData != null) {
          totalShuffleWrittenBytes.addAndGet(keyData.length);
        }

        byte[] valueData = testValues.get(random.nextInt(testValues.size()));
        if (valueData != null) {
          totalShuffleWrittenBytes.addAndGet(valueData.length);
        }

        totalShuffleWrittenBytes.addAndGet(SHUFFLE_RECORD_EXTRA_BYTES);
        totalShuffleWrittenRecords.incrementAndGet();

        if (isLastTaskAttempt) {
          successShuffleWrittenRecords.incrementAndGet();
          usedPartitionIds.putIfAbsent(partitionId, partitionId);
          numPartitionRecords.computeIfAbsent(partitionId, k -> new AtomicLong())
              .incrementAndGet();
        }

        writeClient.writeDataBlock(partitionId,
            valueData == null ? null : ByteBuffer.wrap(valueData));

        if (mapSlowness > 0) {
          try {
            Thread.sleep(mapSlowness);
          } catch (InterruptedException e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            throw new RuntimeException(e);
          }
        }

        long hitPointToShutdownServers = numBytes / 2;
        if (totalShuffleWrittenBytesOldValue < hitPointToShutdownServers &&
            totalShuffleWrittenBytes.get() >= hitPointToShutdownServers) {
          synchronized (servers) {
            synchronized (serverIdsToShutdownDuringShuffleWrite) {
              for (String serverId : serverIdsToShutdownDuringShuffleWrite) {
                StreamServer server = servers.stream().filter(t -> t != null)
                    .filter(t -> t.getServerId().equals(serverId)).findFirst().get();
                logger.info(String
                    .format("Simulate bad server during shuffle write by shutting down server: %s",
                        server));
                shutdownServer(server);

                int index = servers.indexOf(server);
                servers.set(index, null);
              }
              serverIdsToShutdownDuringShuffleWrite.clear();
            }
          }
        }
      }
    }

    // TODO simulate broken map tasks without proper closing

    try {
      writeClient.finishUpload();

      long bytes = writeClient.getShuffleWriteBytes();
      Double rate = rateCounter.addValueAndGetRate(bytes);
      if (rate != null) {
        long mapUploadedBytes = rateCounter.getOverallValue();
        logger.info(String
            .format("Map %s uploaded bytes: %s, rate: %s mb/s", appMapId, mapUploadedBytes,
                rate * (1000.0 / (1024.0 * 1024.0))));
      }

      try {
        logger.info(String.format("Closing write client: %s", writeClient));
        writeClient.close();
      } catch (Exception e) {
        M3Stats.addException(e, this.getClass().getSimpleName());
        throw new RuntimeException(e);
      }
    } catch (Throwable ex) {
      writeClient.close();
      M3Stats.addException(ex, this.getClass().getSimpleName());
      if (isLastTaskAttempt) {
        throw ex;
      } else {
        logger.debug(String.format("Got ignorable error from stale map task: %s",
            ExceptionUtils.getSimpleMessage(ex)));
      }
    }

    logger.info(String.format("Map %s attempt %s finished", appMapId, taskAttemptId));

    double overallBytesMb = rateCounter.getOverallValue() / (1024.0 * 1024.0);
    double overallRate = rateCounter.getOverallRate() * (1000.0 / (1024.0 * 1024.0));
    logger.info(String
        .format("Map %s total uploaded bytes: %s mb, rate: %s mb/s", appMapId, overallBytesMb,
            overallRate));
  }

  private void startNewServer() {
    try {
      String serverDirName = String.format("server_%s", System.nanoTime());
      String serverDirFullPath = Paths.get(workDir, serverDirName).toString();
      while (storage.exists(serverDirFullPath)) {
        serverDirName = String.format("server_%s", System.nanoTime());
        serverDirFullPath = Paths.get(workDir, serverDirName).toString();
      }

      StreamServerConfig serverConfig = new StreamServerConfig();
      serverConfig.setUseEpoll(useEpoll);
      serverConfig.setNettyAcceptThreads(numServerThreads);
      serverConfig.setNettyWorkerThreads(numServerThreads);
      serverConfig.setStorage(storage);
      serverConfig.setShufflePort(0);
      serverConfig.setHttpPort(0);
      serverConfig.setRootDirectory(serverDirFullPath);
      serverConfig.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
      serverConfig.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);
      serverConfig.setAppMemoryRetentionMillis(TimeUnit.HOURS.toMillis(1));
      StreamServer server = new StreamServer(serverConfig, serviceRegistry);
      server.run();

      servers.add(server);
      serverHosts.add("localhost");
      serverPorts.add(server.getShufflePort());
      serverRootDirs.add(serverDirFullPath);
      serverDetails.add(new ServerDetail(server.getServerId(),
          String.format("localhost:%s", server.getShufflePort())));
      logger.info(String
          .format("Started server, port: %s, rootDir: %s, %s", server.getShufflePort(),
              serverDirFullPath, serverConfig));
    } catch (Throwable e) {
      M3Stats.addException(e, this.getClass().getSimpleName());
      throw new RuntimeException("Failed to start stream server", e);
    }
  }

  private void shutdownServer(StreamServer server) {
    logger.info(String.format("Shutting down server: %s", server));
    server.shutdown(true);
  }

  private void deleteDirectories(List<String> directories) {
    directories.stream().forEach(t -> {
      logger.info("Deleting directory: " + t);
      if (!storage.exists(t)) {
        logger.info("Directory not exist: " + t);
      } else {
        storage.deleteDirectory(t);
        logger.info("Deleted directory: " + t);
      }
    });
  }

  public static void main(String[] args) {
    StreamServerStressTool tool = new StreamServerStressTool();

    int i = 0;
    while (i < args.length) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-workDir")) {
        tool.workDir = args[i++];
      } else if (argName.equalsIgnoreCase("-servers")) {
        String servers = args[i++];
        String[] strArray = servers.split(",");
        for (String entry : strArray) {
          String[] hostAndPort = entry.split(":");
          tool.serverHosts.add(hostAndPort[0]);
          tool.serverPorts.add(Integer.parseInt(hostAndPort[1]));
        }
      } else if (argName.equalsIgnoreCase("-epoll")) {
        tool.useEpoll = Boolean.parseBoolean(args[i++]);
      } else if (argName.equalsIgnoreCase("-numServers")) {
        tool.numServers = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-numServerThreads")) {
        tool.numServerThreads = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-writeClientQueueSize")) {
        tool.writeClientQueueSize = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-writeClientThreads")) {
        tool.writeClientThreads = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-appId")) {
        tool.appId = args[i++];
      } else if (argName.equalsIgnoreCase("-numBytes")) {
        String numBytesStr = args[i++];
        tool.numBytes = org.apache.spark.remoteshuffle.util.StringUtils.getBytesValue(numBytesStr);
      } else if (argName.equalsIgnoreCase("-numMaps")) {
        tool.numMaps = Integer.parseInt(args[i++]);
        tool.startMapId = 0;
        tool.endMapId = tool.numMaps - 1;
      } else if (argName.equalsIgnoreCase("-numMapAttempts")) {
        tool.numMapAttempts = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-startMapId")) {
        tool.startMapId = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-endMapId")) {
        tool.endMapId = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-mapDelay")) {
        tool.mapDelay = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-mapSlowness")) {
        tool.mapSlowness = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxWait")) {
        tool.maxWait = Long.parseLong(args[i++]);
      } else if (argName.equalsIgnoreCase("-numPartitions")) {
        tool.numPartitions = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-numSplits")) {
        tool.numSplits = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-numReplicas")) {
        tool.numReplicas = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-partitionFanout")) {
        tool.partitionFanout = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-deleteFiles")) {
        tool.deleteFiles = Boolean.parseBoolean(args[i++]);
      } else {
        throw new IllegalArgumentException("Unsupported argument: " + argName);
      }
    }

    try {
      tool.run();
    } finally {
      tool.cleanup();
    }

    M3Stats.closeDefaultScope();

    logger.info(String.format("%s finished", StreamServerStressToolLongRun.class.getSimpleName()));
  }
}
