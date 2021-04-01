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

import org.apache.spark.remoteshuffle.common.Compression;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage;
import org.apache.spark.remoteshuffle.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * This tool repeatedly runs StreamServerStressTool with randomly generated arguments.
 */
public class StreamServerStressToolLongRun {
  private static final Logger logger =
      LoggerFactory.getLogger(StreamServerStressToolLongRun.class);

  private int runMinutes = 1;
  private String workDir = "/tmp/rss";
  private String compressCodec = "lz4";
  private int bufferSize = ShuffleFileStorage.DEFAULT_BUFFER_SIZE;
  private int maxNumServers = 10;
  private long maxNumBytes = ((long) Integer.MAX_VALUE) * 4L;
  private int maxNumMaps = 100;
  private int maxNumMapAttempts = 5;
  private int maxNumPartitions = 100;
  private int maxNumSplits = 10;
  private int maxNumReplicas = 1;

  public void run() {
    long startTime = System.currentTimeMillis();
    long numIterations = 0;
    long elapsedMinutes = 0;

    Random random = new Random();

    String[] compressCodecValues = new String[]{"", Compression.COMPRESSION_CODEC_LZ4};

    while (System.currentTimeMillis() - startTime < runMinutes * 60 * 1000) {
      int writeClientQueueSize = 0;
      int writeClientThreads = 0;
      boolean useAsyncWriteClient = random.nextBoolean();
      if (useAsyncWriteClient) {
        writeClientQueueSize = 1 + random.nextInt(10000);
        writeClientThreads = 1 + random.nextInt(10);
      }

      boolean useConnectionPool = random.nextBoolean();

      StreamServerStressTool tool = new StreamServerStressTool();

      int numServers = 1 + random.nextInt(maxNumServers);
      int numReplicas = Math.min(1 + random.nextInt(maxNumReplicas), numServers);

      int numServerGroups = numServers / numReplicas;
      int partitionFanout = 1 + random.nextInt(numServerGroups);
      logger.info(String
          .format("Using servers: %s, replicas: %s, partition fanout: %s", numServers, numReplicas,
              partitionFanout));

      tool.setMaxWait(3600000L);
      tool.setNumServers(numServers);
      tool.setWorkDir(workDir);
      tool.setNumServerThreads(1 + random.nextInt(100));
      tool.setWriteClientQueueSize(writeClientQueueSize);
      tool.setWriteClientThreads(writeClientThreads);
      tool.setNumBytes((long) (random.nextFloat() * maxNumBytes));
      tool.setNumMaps(1 + random.nextInt(maxNumMaps));
      tool.setNumMapAttempts(1 + random.nextInt(maxNumMapAttempts));
      tool.setMapDelay(1 + random.nextInt(10000));
      tool.setNumPartitions(1 + random.nextInt(maxNumPartitions));
      tool.setNumSplits(1 + random.nextInt(maxNumSplits));
      tool.setNumReplicas(numReplicas);
      tool.setPartitionFanout(partitionFanout);
      tool.setUseConnectionPool(useConnectionPool);

      try {
        logger.info("Running tool: " + tool);
        tool.run();
        numIterations++;
      } catch (Throwable ex) {
        elapsedMinutes = (System.currentTimeMillis() - startTime) / (60 * 1000);
        logger.error(String.format("Failed after running %s minutes with %s iterations, args: %s",
            elapsedMinutes,
            numIterations,
            tool),
            ex);
        System.exit(-1);
      } finally {
        tool.cleanup();
      }
    }

    elapsedMinutes = (System.currentTimeMillis() - startTime) / (60 * 1000);
    logger.info(String
        .format("Succeeded after running %s minutes with %s iterations", elapsedMinutes,
            numIterations));
  }

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        logger.error(String.format("Got exception from thread %s", t.getName()), e);
        System.exit(-1);
      }
    });

    StreamServerStressToolLongRun longRun = new StreamServerStressToolLongRun();

    int i = 0;
    while (i < args.length) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-runMinutes")) {
        longRun.runMinutes = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-workDir")) {
        longRun.workDir = args[i++];
      } else if (argName.equalsIgnoreCase("-compressCodec")) {
        longRun.compressCodec = args[i++];
      } else if (argName.equalsIgnoreCase("-bufferSize")) {
        longRun.bufferSize = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxNumServers")) {
        longRun.maxNumServers = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxNumBytes")) {
        longRun.maxNumBytes = StringUtils.getBytesValue(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxNumMaps")) {
        longRun.maxNumMaps = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxNumMapAttempts")) {
        longRun.maxNumMapAttempts = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxNumPartitions")) {
        longRun.maxNumPartitions = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxNumSplits")) {
        longRun.maxNumSplits = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxNumReplicas")) {
        longRun.maxNumReplicas = Integer.parseInt(args[i++]);
      } else {
        throw new IllegalArgumentException("Unsupported argument: " + argName);
      }
    }

    longRun.run();

    M3Stats.closeDefaultScope();

    logger.info(String.format("%s finished", StreamServerStressToolLongRun.class.getSimpleName()));
  }
}
