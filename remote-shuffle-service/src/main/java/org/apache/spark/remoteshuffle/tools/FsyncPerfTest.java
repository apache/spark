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

package org.apache.spark.remoteshuffle.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This tool tests the performance of fsync operation.
 */
public class FsyncPerfTest {
  private static final Logger logger = LoggerFactory.getLogger(FsyncPerfTest.class);

  private int numFiles = 1;
  private String[] filePaths;
  private FileOutputStream[] fileStreams;

  private int numTestValues = 1000;
  private int maxTestValueSize = 10000;
  private byte[][] testValues;

  private int numWrites = 10000;

  private double[] fsyncMillisValues;
  private AtomicInteger fsyncMillisValuesIndex = new AtomicInteger();

  private Random random = new Random();

  private AtomicLong totalOperationCount = new AtomicLong();

  private AtomicLong totalWriteTime = new AtomicLong();
  private AtomicLong totalFlushTime = new AtomicLong();
  private AtomicLong totalFsyncTime = new AtomicLong();

  private void prepare() {
    filePaths = new String[numFiles];
    fileStreams = new FileOutputStream[numFiles];

    for (int i = 0; i < fileStreams.length; i++) {
      try {
        File tempFile = File.createTempFile("FsyncPerfTest", ".tmp");
        tempFile.deleteOnExit();
        filePaths[i] = tempFile.getAbsolutePath();

        FileOutputStream stream = new FileOutputStream(tempFile);
        fileStreams[i] = stream;

        logger.info("Created file: " + tempFile.getAbsolutePath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create temp file", e);
      }
    }

    fsyncMillisValues = new double[fileStreams.length * numWrites];

    testValues = new byte[numTestValues][];

    for (int i = 0; i < testValues.length; i++) {
      char ch = (char) ('a' + random.nextInt(26));
      int repeats = random.nextInt(maxTestValueSize);
      String str = StringUtils.repeat(ch, repeats);
      testValues[i] = str.getBytes(StandardCharsets.UTF_8);
    }
  }

  private void run() {
    Arrays.stream(fileStreams).parallel().forEach(stream -> {
      long writeBytes = 0;

      for (int i = 0; i < numWrites; i++) {
        int valueIndex = random.nextInt(testValues.length);
        byte[] bytes = testValues[valueIndex];
        try {
          totalOperationCount.incrementAndGet();
          writeBytes += bytes.length;

          long startTime = System.nanoTime();
          stream.write(bytes);
          totalWriteTime.addAndGet(System.nanoTime() - startTime);

          startTime = System.nanoTime();
          stream.flush();
          totalFlushTime.addAndGet(System.nanoTime() - startTime);

          startTime = System.nanoTime();
          stream.getFD().sync();
          long fsyncDuration = System.nanoTime() - startTime;
          totalFsyncTime.addAndGet(fsyncDuration);

          fsyncMillisValues[fsyncMillisValuesIndex.getAndIncrement()] =
              TimeUnit.NANOSECONDS.toMillis(fsyncDuration);
        } catch (IOException e) {
          throw new RuntimeException("Failed to write file", e);
        }
      }

      logger.info(String.format("Finished writing file: %s bytes", writeBytes));
    });

    logger.info(String.format(
        "Total operation: %s, total write seconds: %s, flush: %s, fsync: %s, average write milliseconds: %s, flush: %s, fsync: %s",
        totalOperationCount.get(),
        TimeUnit.NANOSECONDS.toSeconds(totalWriteTime.get()),
        TimeUnit.NANOSECONDS.toSeconds(totalFlushTime.get()),
        TimeUnit.NANOSECONDS.toSeconds(totalFsyncTime.get()),
        TimeUnit.NANOSECONDS.toMillis(totalWriteTime.get() / totalOperationCount.get()),
        TimeUnit.NANOSECONDS.toMillis(totalFlushTime.get() / totalOperationCount.get()),
        TimeUnit.NANOSECONDS.toMillis(totalFsyncTime.get() / totalOperationCount.get())));

    EmpiricalDistribution fsyncMillisDistribution = new EmpiricalDistribution();
    fsyncMillisDistribution.load(fsyncMillisValues);

    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (double v : fsyncMillisValues) {
      stats.addValue(v);
    }

    stats.getPercentile(50);

    logger.info(String.format("fsync duration (milliseconds): P50: %s, P95: %s, P99: %s, max: %s",
        stats.getPercentile(50),
        stats.getPercentile(95),
        stats.getPercentile(99),
        stats.getMax()));
  }

  private void cleanup() {
    for (int i = 0; i < fileStreams.length; i++) {
      try {
        fileStreams[i].close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close file", e);
      }
    }
  }

  public static void main(String[] args) {
    FsyncPerfTest perfTest = new FsyncPerfTest();

    int i = 0;
    while (i < args.length) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-numFiles")) {
        perfTest.numFiles = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-maxTestValueSize")) {
        perfTest.maxTestValueSize = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-numWrites")) {
        perfTest.numWrites = Integer.parseInt(args[i++]);
      } else {
        throw new IllegalArgumentException("Unsupported argument: " + argName);
      }
    }

    perfTest.prepare();
    perfTest.run();
    perfTest.cleanup();
  }
}
