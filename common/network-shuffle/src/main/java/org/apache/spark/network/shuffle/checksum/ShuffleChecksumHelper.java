/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle.checksum;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import com.google.common.io.ByteStreams;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.annotation.Private;
import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * A set of utility functions for the shuffle checksum.
 */
@Private
public class ShuffleChecksumHelper {
  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(ShuffleChecksumHelper.class);

  public static final int CHECKSUM_CALCULATION_BUFFER = 8192;
  public static final Checksum[] EMPTY_CHECKSUM = new Checksum[0];
  public static final long[] EMPTY_CHECKSUM_VALUE = new long[0];

  public static Checksum[] createPartitionChecksums(int numPartitions, String algorithm) {
    return getChecksumsByAlgorithm(numPartitions, algorithm);
  }

  private static Checksum[] getChecksumsByAlgorithm(int num, String algorithm) {
    Checksum[] checksums;
    switch (algorithm) {
      case "ADLER32" -> {
        checksums = new Adler32[num];
        for (int i = 0; i < num; i++) {
          checksums[i] = new Adler32();
        }
      }

      case "CRC32" -> {
        checksums = new CRC32[num];
        for (int i = 0; i < num; i++) {
          checksums[i] = new CRC32();
        }
      }

      default -> throw new UnsupportedOperationException(
        "Unsupported shuffle checksum algorithm: " + algorithm);
    }

    return checksums;
  }

  public static Checksum getChecksumByAlgorithm(String algorithm) {
    return getChecksumsByAlgorithm(1, algorithm)[0];
  }

  public static String getChecksumFileName(String blockName, String algorithm) {
    // append the shuffle checksum algorithm as the file extension
    return String.format("%s.%s", blockName, algorithm);
  }

  private static long readChecksumByReduceId(File checksumFile, int reduceId) throws IOException {
    try (DataInputStream in = new DataInputStream(new FileInputStream(checksumFile))) {
      ByteStreams.skipFully(in, reduceId * 8L);
      return in.readLong();
    }
  }

  private static long calculateChecksumForPartition(
      ManagedBuffer partitionData,
      Checksum checksumAlgo) throws IOException {
    InputStream in = partitionData.createInputStream();
    byte[] buffer = new byte[CHECKSUM_CALCULATION_BUFFER];
    try(CheckedInputStream checksumIn = new CheckedInputStream(in, checksumAlgo)) {
      while (checksumIn.read(buffer, 0, CHECKSUM_CALCULATION_BUFFER) != -1) {}
      return checksumAlgo.getValue();
    }
  }

  /**
   * Diagnose the possible cause of the shuffle data corruption by verifying the shuffle checksums.
   *
   * There're 3 different kinds of checksums for the same shuffle partition:
   *   - checksum (c1) that is calculated by the shuffle data reader
   *   - checksum (c2) that is calculated by the shuffle data writer and stored in the checksum file
   *   - checksum (c3) that is recalculated during diagnosis
   *
   * And the diagnosis mechanism works like this:
   * If c2 != c3, we suspect the corruption is caused by the DISK_ISSUE. Otherwise, if c1 != c3,
   * we suspect the corruption is caused by the NETWORK_ISSUE. Otherwise, the cause remains
   * CHECKSUM_VERIFY_PASS. In case of the any other failures, the cause remains UNKNOWN_ISSUE.
   *
   * @param algorithm The checksum algorithm that is used for calculating checksum value
   *                  of partitionData
   * @param checksumFile The checksum file that written by the shuffle writer
   * @param reduceId The reduceId of the shuffle block
   * @param partitionData The partition data of the shuffle block
   * @param checksumByReader The checksum value that calculated by the shuffle data reader
   * @return The cause of data corruption
   */
  public static Cause diagnoseCorruption(
      String algorithm,
      File checksumFile,
      int reduceId,
      ManagedBuffer partitionData,
      long checksumByReader) {
    Cause cause;
    long duration = -1L;
    long checksumByWriter = -1L;
    long checksumByReCalculation = -1L;
    try {
      long diagnoseStartNs = System.nanoTime();
      // Try to get the checksum instance before reading the checksum file so that
      // `UnsupportedOperationException` can be thrown first before `FileNotFoundException`
      // when the checksum algorithm isn't supported.
      Checksum checksumAlgo = getChecksumByAlgorithm(algorithm);
      checksumByWriter = readChecksumByReduceId(checksumFile, reduceId);
      checksumByReCalculation = calculateChecksumForPartition(partitionData, checksumAlgo);
      duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - diagnoseStartNs);
      if (checksumByWriter != checksumByReCalculation) {
        cause = Cause.DISK_ISSUE;
      } else if (checksumByWriter != checksumByReader) {
        cause = Cause.NETWORK_ISSUE;
      } else {
        cause = Cause.CHECKSUM_VERIFY_PASS;
      }
    } catch (UnsupportedOperationException e) {
      cause = Cause.UNSUPPORTED_CHECKSUM_ALGORITHM;
    } catch (FileNotFoundException e) {
      // Even if checksum is enabled, a checksum file may not exist if error throws during writing.
      logger.warn("Checksum file {} doesn't exit",
        MDC.of(LogKeys.PATH$.MODULE$, checksumFile.getName()));
      cause = Cause.UNKNOWN_ISSUE;
    } catch (Exception e) {
      logger.warn("Unable to diagnose shuffle block corruption", e);
      cause = Cause.UNKNOWN_ISSUE;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Shuffle corruption diagnosis took {} ms, checksum file {}, cause {}, " +
        "checksumByReader {}, checksumByWriter {}, checksumByReCalculation {}",
        duration, checksumFile.getAbsolutePath(), cause,
        checksumByReader, checksumByWriter, checksumByReCalculation);
    } else {
      logger.info("Shuffle corruption diagnosis took {} ms, checksum file {}, cause {}",
        MDC.of(LogKeys.TIME$.MODULE$, duration),
        MDC.of(LogKeys.PATH$.MODULE$, checksumFile.getAbsolutePath()),
        MDC.of(LogKeys.REASON$.MODULE$, cause));
    }
    return cause;
  }
}
